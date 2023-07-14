use std::{collections::HashMap, io, net::SocketAddr, str::FromStr, time::Duration};

use crate::agnostic::EnclaveStream;
use anyhow::{Context, Result};
use bytes::BytesMut;
use futures::FutureExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{sleep, Instant},
};
use tracing::{debug, error};

pub struct EnclaveTerminusRxChunk {
    id: u32,
    data: BytesMut,
}

pub struct EnclaveTermimusTask {
    id: u32,
    buf_size: usize,
    to_proxy_task_tx: UnboundedSender<EnclaveTerminusRxChunk>,
    from_proxy_task_rx: UnboundedReceiver<BytesMut>,
    dest_conn: TcpStream,
}

impl EnclaveTermimusTask {
    pub async fn new(
        id: u32,
        buf_size: usize,
        to_proxy_task_tx: UnboundedSender<EnclaveTerminusRxChunk>,
        from_proxy_task_rx: UnboundedReceiver<BytesMut>,
        terminus_address: &str,
    ) -> Result<Self> {
        let socket = TcpSocket::new_v4().expect("should be able to construct TcpSocket");
        let addr = SocketAddr::from_str(terminus_address)?;
        let dest_conn = socket.connect(addr).await?;
        Ok(Self {
            id,
            buf_size,
            to_proxy_task_tx,
            from_proxy_task_rx,
            dest_conn,
        })
    }

    async fn shutdown(&mut self) {
        self.dest_conn.shutdown().await.ok();
        self.to_proxy_task_tx
            .send(EnclaveTerminusRxChunk {
                id: self.id,
                data: Default::default(),
            })
            .ok();
    }

    async fn handle_proxy_task_rx(&mut self, rx_value: Option<BytesMut>) -> Result<bool> {
        match rx_value {
            None => {
                debug!(
                    "proxy task rx has terminated, quitting comms for {}",
                    self.id
                );
                self.dest_conn.shutdown().await.ok();
                return Ok(false);
            }
            Some(rx_buf) => {
                if rx_buf.is_empty() {
                    debug!(
                        "zero buf recv from proxy task, quitting comms for {}",
                        self.id
                    );
                    self.shutdown().await;
                    return Ok(false);
                }
                if let Err(e) = self.dest_conn.write_all(&rx_buf).await {
                    self.shutdown().await;
                    return Err(e.into());
                }
                Ok(true)
            }
        }
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
                "zero buf recv from terminus, quitting comms for {}",
                self.id
            );
            self.shutdown().await;
            return Ok(false);
        }
        self.to_proxy_task_tx
            .send(EnclaveTerminusRxChunk {
                id: self.id,
                data: rx_buf,
            })
            .ok();
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

struct EnclaveProxyConn {
    to_terminus_tx: UnboundedSender<BytesMut>,
    setup_time: Instant,
}

pub struct EnclaveProxyTask {
    host_conn: EnclaveStream,
    conn_cleanup_interval: Duration,
    dead_conn_ttl: Duration,
    buf_size: usize,
    terminus_address: String,
    conns: HashMap<u32, EnclaveProxyConn>,

    to_proxy_tx: UnboundedSender<EnclaveTerminusRxChunk>,
    from_terminus_rx: UnboundedReceiver<EnclaveTerminusRxChunk>,
}

impl EnclaveProxyTask {
    pub fn new(
        host_conn: EnclaveStream,
        conn_cleanup_interval_secs: u64,
        dead_conn_ttl_secs: u64,
        buf_size: usize,
        terminus_address: String,
    ) -> Self {
        let (to_proxy_tx, from_terminus_rx) = unbounded_channel();
        Self {
            host_conn,
            conn_cleanup_interval: Duration::from_secs(conn_cleanup_interval_secs),
            dead_conn_ttl: Duration::from_secs(dead_conn_ttl_secs),
            buf_size,
            terminus_address,
            conns: HashMap::new(),
            to_proxy_tx,
            from_terminus_rx,
        }
    }

    async fn handle_host_conn_rx(
        &mut self,
        rx_result: io::Result<usize>,
        length_id_enc: &[u8],
    ) -> Result<()> {
        rx_result?;

        let length = u32::from_le_bytes(length_id_enc[0..4].try_into().unwrap());
        let id = u32::from_le_bytes(length_id_enc[4..8].try_into().unwrap());

        let mut rx_buf = BytesMut::with_capacity(length as usize);
        while rx_buf.len() < length as usize {
            self.host_conn.read_buf(&mut rx_buf).await?;
        }

        let conn = self.conns.entry(id).or_insert_with(|| {
            let (to_terminus_tx, to_terminus_rx) = unbounded_channel::<BytesMut>();
            let to_proxy_tx = self.to_proxy_tx.clone();
            let buf_size = self.buf_size;
            let terminus_address = self.terminus_address.clone();
            tokio::spawn(async move {
                let result = async {
                    let task = EnclaveTermimusTask::new(
                        id,
                        buf_size,
                        to_proxy_tx,
                        to_terminus_rx,
                        &terminus_address,
                    )
                    .await
                    .context("failed to connect to terminus")?;
                    task.run().await
                };
                if let Err(e) = result.await {
                    error!("enclave terminus task failed: {e}");
                }
            });
            EnclaveProxyConn {
                to_terminus_tx,
                setup_time: Instant::now(),
            }
        });

        conn.to_terminus_tx.send(rx_buf).ok();
        Ok(())
    }

    async fn handle_terminus_rx(&mut self, rx_chunk: Option<EnclaveTerminusRxChunk>) -> Result<()> {
        let rx_chunk = rx_chunk.expect("rx_chunk should never be None");
        let length = rx_chunk.data.len() as u32;

        let mut length_id_enc = [0u8; 8];
        length_id_enc[0..4].copy_from_slice(&length.to_le_bytes());
        length_id_enc[4..8].copy_from_slice(&rx_chunk.id.to_le_bytes());

        self.host_conn.write_all(&length_id_enc).await?;
        self.host_conn.write_all(&rx_chunk.data).await?;
        Ok(())
    }

    async fn cleanup_dead_conns(&mut self) -> Result<()> {
        let count_before = self.conns.len();
        self.conns.retain(|_, conn| {
            !conn.to_terminus_tx.is_closed() || conn.setup_time.elapsed() < self.dead_conn_ttl
        });
        let count_after = self.conns.len();
        debug!(
            "cleaned up {} dead connections; count after = {count_after}",
            count_before - count_after
        );
        Ok(())
    }

    pub async fn run(mut self) -> Result<()> {
        let mut conn_cleanup_sleep = sleep(self.conn_cleanup_interval).boxed();
        loop {
            let mut length_id_enc = [0u8; 8];
            tokio::select! {
                rx_result = self.host_conn.read_exact(&mut length_id_enc) => self.handle_host_conn_rx(rx_result, &length_id_enc).await,
                rx_chunk = self.from_terminus_rx.recv() => self.handle_terminus_rx(rx_chunk).await,
                _ = &mut conn_cleanup_sleep => {
                    let result = self.cleanup_dead_conns().await;
                    conn_cleanup_sleep = sleep(self.conn_cleanup_interval).boxed();
                    result
                }
            }?;
        }
    }
}
