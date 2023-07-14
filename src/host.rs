use std::{collections::HashMap, io};

use crate::agnostic::{connect_to_enclave, EnclaveStream};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tracing::debug;

enum HostTerminusRxMessage {
    NewConn(UnboundedSender<BytesMut>),
    Data(BytesMut),
}

pub struct HostTerminusIdRxMessage {
    message: HostTerminusRxMessage,
    id: u32,
}

pub struct HostTerminusTask {
    dest_conn: TcpStream,
    id: u32,
    buf_size: usize,
    to_proxy_task_tx: UnboundedSender<HostTerminusIdRxMessage>,
    from_proxy_task_rx: UnboundedReceiver<BytesMut>,
}

impl HostTerminusTask {
    pub fn new(
        dest_conn: TcpStream,
        id: u32,
        buf_size: usize,
        to_proxy_task_tx: UnboundedSender<HostTerminusIdRxMessage>,
    ) -> Result<Self> {
        let (to_terminus_tx, from_proxy_task_rx) = unbounded_channel();
        to_proxy_task_tx
            .send(HostTerminusIdRxMessage {
                message: HostTerminusRxMessage::NewConn(to_terminus_tx),
                id,
            })
            .map_err(|_| {
                anyhow!("failed send to_terminus_tx to proxy task, is enclave connection dead?")
            })?;
        Ok(Self {
            dest_conn,
            id,
            buf_size,
            to_proxy_task_tx,
            from_proxy_task_rx,
        })
    }

    fn send_to_proxy_task(&self, message: HostTerminusRxMessage) -> Result<()> {
        self.to_proxy_task_tx
            .send(HostTerminusIdRxMessage {
                id: self.id,
                message,
            })
            .map_err(|_| {
                anyhow!("failed send data chunk to proxy task, is enclave connection dead?")
            })
    }

    async fn shutdown(&mut self) {
        self.dest_conn.shutdown().await.ok();
        self.send_to_proxy_task(HostTerminusRxMessage::Data(Default::default()))
            .ok();
    }

    async fn handle_proxy_task_rx(&mut self, rx_value: Option<BytesMut>) -> Result<bool> {
        let rx_buf = rx_value.ok_or_else(|| {
            anyhow!("failed to recv data chunk from proxy task, is enclave connection dead?")
        })?;
        if let Err(e) = self.dest_conn.write_all(&rx_buf).await {
            self.shutdown().await;
            return Err(e.into());
        }
        if rx_buf.is_empty() {
            self.shutdown().await;
            return Ok(false);
        }
        Ok(true)
    }

    async fn handle_dest_conn_rx(
        &mut self,
        rx_result: io::Result<usize>,
        rx_buf: BytesMut,
    ) -> Result<bool> {
        if let Err(e) = rx_result {
            self.shutdown().await;
            return Err(e.into());
        }
        if rx_buf.is_empty() {
            debug!(
                "recv empty buf from host connection, quitting host comm for id {}",
                self.id
            );
            self.shutdown().await;
            return Ok(false);
        }
        self.send_to_proxy_task(HostTerminusRxMessage::Data(rx_buf))?;
        Ok(true)
    }

    pub async fn run(mut self) -> Result<()> {
        let mut should_continue = true;
        while should_continue {
            let mut rx_buf = BytesMut::with_capacity(self.buf_size);
            should_continue = tokio::select! {
                rx_value = self.from_proxy_task_rx.recv() => self.handle_proxy_task_rx(rx_value).await,
                rx_result = self.dest_conn.read_buf(&mut rx_buf) => self.handle_dest_conn_rx(rx_result, rx_buf).await
            }?;
        }
        Ok(())
    }
}

pub struct HostProxyTask {
    enclave_conn: EnclaveStream,
    from_terminus_rx: UnboundedReceiver<HostTerminusIdRxMessage>,
    conns: HashMap<u32, UnboundedSender<BytesMut>>,
}

impl HostProxyTask {
    pub async fn new(
        enclave_proxy_address: &str,
        from_terminus_rx: UnboundedReceiver<HostTerminusIdRxMessage>,
    ) -> Result<Self> {
        Ok(Self {
            enclave_conn: connect_to_enclave(enclave_proxy_address).await?,
            from_terminus_rx,
            conns: Default::default(),
        })
    }

    async fn handle_enclave_conn_rx(
        &mut self,
        rx_result: io::Result<usize>,
        length_id_enc: &[u8],
    ) -> Result<()> {
        rx_result?;

        let length = u32::from_le_bytes(length_id_enc[0..4].try_into().unwrap());
        let id = u32::from_le_bytes(length_id_enc[4..8].try_into().unwrap());

        let mut rx_buf = BytesMut::with_capacity(length as usize);
        while rx_buf.len() < length as usize {
            self.enclave_conn.read_buf(&mut rx_buf).await?;
        }

        let rx_buf_empty = rx_buf.is_empty();
        if let Some(conn_tx) = self.conns.get(&id) {
            conn_tx.send(rx_buf).ok();
        }

        if rx_buf_empty {
            debug!("recv empty buf for id {id} from enclave, removing connection reference");
            self.conns.remove(&id);
        }

        Ok(())
    }

    async fn handle_terminus_rx(
        &mut self,
        rx_chunk: Option<HostTerminusIdRxMessage>,
    ) -> Result<()> {
        let rx_chunk = rx_chunk.expect("rx_chunk should never be None");
        match rx_chunk.message {
            HostTerminusRxMessage::NewConn(conn_tx) => {
                debug!("new host connection registered with id {}", rx_chunk.id);
                self.conns.insert(rx_chunk.id, conn_tx);
            }
            HostTerminusRxMessage::Data(rx_buf) => {
                if rx_buf.is_empty() {
                    debug!(
                        "recv empty buf for id {} from host, removing connection reference",
                        rx_chunk.id
                    );
                    self.conns.remove(&rx_chunk.id);
                }

                let length = rx_buf.len() as u32;

                let mut length_id_enc = [0u8; 8];
                length_id_enc[0..4].copy_from_slice(&length.to_le_bytes());
                length_id_enc[4..8].copy_from_slice(&rx_chunk.id.to_le_bytes());

                self.enclave_conn.write_all(&length_id_enc).await?;
                self.enclave_conn.write_all(&rx_buf).await?;
            }
        }
        Ok(())
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            let mut length_id_enc = [0u8; 8];
            tokio::select! {
                    rx_result = self.enclave_conn.read_exact(&mut length_id_enc) => self.handle_enclave_conn_rx(rx_result, &length_id_enc).await,
                    rx_chunk = self.from_terminus_rx.recv() => self.handle_terminus_rx(rx_chunk).await
            }?;
        }
    }
}
