use std::collections::HashMap;

use bytes::BytesMut;
use rand::seq::SliceRandom;
use rlimit::Resource;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};

#[cfg(not(feature = "all-tcp"))]
use tokio_vsock::{VsockListener, VsockStream};

#[cfg(feature = "all-tcp")]
pub type VsockListener = TcpListener;
#[cfg(feature = "all-tcp")]
pub type VsockStream = TcpStream;

const CONN_COUNT: usize = 4000;
const BUF_SIZE: usize = 8192;

const ENCLAVE_DEST_ADDR: &str = "127.0.0.1:8080";

#[cfg(not(feature = "all-tcp"))]
const ENCLAVE_VSOCK_CID: u32 = 4;
#[cfg(not(feature = "all-tcp"))]
const ENCLAVE_VSOCK_PORT: u32 = 8181;
#[cfg(feature = "all-tcp")]
const ENCLAVE_LISTEN_ADDR: &str = "0.0.0.0:8181";

const HOST_LISTEN_ADDR: &str = "0.0.0.0:8282";

async fn enclave_tcp_comm_task(
    mut dest_conn: TcpStream,
    id: u32,
    tx: UnboundedSender<(u32, BytesMut)>,
    mut rx: UnboundedReceiver<BytesMut>,
) {
    loop {
        let mut rx_buf = BytesMut::with_capacity(BUF_SIZE);
        tokio::select! {
            result = rx.recv() => {
                let rx_buf = result.unwrap();
                if let Err(e) = dest_conn.write_all(&rx_buf).await {
                    eprintln!("could not write, quitting dest comm: {e}");
                    tx.send((id, BytesMut::new())).ok();
                    return;
                }
                if rx_buf.is_empty() {
                    eprintln!("zero buf recv, quitting dest comm");
                    tx.send((id, BytesMut::new())).ok();
                    return;
                }
            },
            result = dest_conn.read_buf(&mut rx_buf) => {
                if let Err(e) = result {
                    eprintln!("could not read, quitting dest comm: {e}");
                    tx.send((id, BytesMut::new())).ok();
                    return;
                }
                if rx_buf.is_empty() {
                    eprintln!("tcp zero buf recv, quitting tcp comm");
                    tx.send((id, BytesMut::new())).ok();
                    return;
                }
                tx.send((id, rx_buf)).ok();
            }
        }
    }
}

async fn enclave_vsock_comm_task(mut vsock_conn: VsockStream) {
    let (dest_tx, mut dest_rx) = unbounded_channel::<(u32, BytesMut)>();
    let mut active_conns: HashMap<u32, UnboundedSender<BytesMut>> = HashMap::new();
    loop {
        let mut length_id_enc = [0u8; 8];
        tokio::select! {
            result = vsock_conn.read_exact(&mut length_id_enc) => {
                if let Err(e) = result {
                    eprintln!("vsock length-id read error, quitting enclave comm: {e}");
                    return;
                }
                let length = u32::from_le_bytes(length_id_enc[0..4].try_into().unwrap());
                let id = u32::from_le_bytes(length_id_enc[4..8].try_into().unwrap());

                eprintln!("packet recv {length} {id}");

                let dest_conn = active_conns.entry(id).or_insert_with(|| {
                    let (conn_tx, conn_rx) = unbounded_channel::<BytesMut>();
                    let dest_tx = dest_tx.clone();
                    tokio::spawn(async move {
                        let dest_conn = TcpStream::connect(ENCLAVE_DEST_ADDR).await.unwrap();
                        enclave_tcp_comm_task(dest_conn, id, dest_tx, conn_rx).await;
                    });
                    conn_tx
                });

                let mut rx_buf = BytesMut::with_capacity(length as usize);
                while rx_buf.len() < length as usize {
                    if let Err(e) = vsock_conn.read_buf(&mut rx_buf).await {
                        eprintln!("vsock buf read error, quitting enclave comm: {e}");
                        return;
                    }
                }

                dest_conn.send(rx_buf).ok();
            },
            result = dest_rx.recv() => {
                let (id, buf) = result.unwrap();
                if buf.is_empty() {
                    active_conns.remove(&id);
                }
                let length = buf.len() as u32;
                length_id_enc[0..4].copy_from_slice(&length.to_le_bytes());
                length_id_enc[4..8].copy_from_slice(&id.to_le_bytes());
                if let Err(e) = vsock_conn.write_all(&length_id_enc).await {
                    eprintln!("vsock length-id write error, quitting enclave comm: {e}");
                    return;
                }
                if let Err(e) = vsock_conn.write_all(&buf).await {
                    eprintln!("vsock buf write error, quitting enclave comm: {e}");
                    return;
                }
            }
        }
    }
}

enum HostDataNotif {
    NewConn(UnboundedSender<BytesMut>),
    Data(BytesMut),
}

async fn host_tcp_comm_task(
    mut dest_conn: TcpStream,
    id: u32,
    tx: UnboundedSender<(u32, HostDataNotif)>,
) {
    let (data_tx, mut data_rx) = unbounded_channel();
    tx.send((id, HostDataNotif::NewConn(data_tx))).ok();
    loop {
        let mut rx_buf = BytesMut::with_capacity(BUF_SIZE);
        tokio::select! {
            result = data_rx.recv() => {
                let rx_buf = result.unwrap();
                if let Err(e) = dest_conn.write_all(&rx_buf).await {
                    eprintln!("could not write, quitting tcp comm: {e}");
                    tx.send((id, HostDataNotif::Data(BytesMut::new()))).ok();
                    return;
                }
                if rx_buf.is_empty() {
                    eprintln!("zero buf recv, quitting tcp comm");
                    tx.send((id, HostDataNotif::Data(BytesMut::new()))).ok();
                    return;
                }
            },
            result = dest_conn.read_buf(&mut rx_buf) => {
                if let Err(e) = result {
                    eprintln!("could not read, quitting tcp comm: {e}");
                    tx.send((id, HostDataNotif::Data(BytesMut::new()))).ok();
                    return;
                }
                eprintln!("tcp read {}", rx_buf.len());
                if rx_buf.is_empty() {
                    eprintln!("tcp zero buf recv, quitting tcp comm");
                    tx.send((id, HostDataNotif::Data(BytesMut::new()))).ok();
                    return;
                }
                tx.send((id, HostDataNotif::Data(rx_buf))).ok();
            }
        }
    }
}

async fn host_vsock_comm_task(
    mut vsock_conn: VsockStream,
    mut data_rx: UnboundedReceiver<(u32, HostDataNotif)>,
) {
    let mut active_conns: HashMap<u32, UnboundedSender<BytesMut>> = HashMap::new();
    loop {
        let mut length_id_enc = [0u8; 8];
        tokio::select! {
            result = vsock_conn.read_exact(&mut length_id_enc) => {
                if let Err(e) = result {
                    eprintln!("vsock length-id read error, quitting host comm: {e}");
                    return;
                }
                let length = u32::from_le_bytes(length_id_enc[0..4].try_into().unwrap());
                let id = u32::from_le_bytes(length_id_enc[4..8].try_into().unwrap());

                let mut rx_buf = BytesMut::with_capacity(length as usize);
                while rx_buf.len() < length as usize {
                    if let Err(e) = vsock_conn.read_buf(&mut rx_buf).await {
                        eprintln!("vsock buf read error, quitting host comm: {e}");
                        return;
                    }
                }

                let rx_buf_empty = rx_buf.is_empty();
                if let Some(conn_tx) = active_conns.get(&id) {
                    conn_tx.send(rx_buf).ok();
                }

                if rx_buf_empty {
                    eprintln!("remove new conn sender {id} from enclave");
                    active_conns.remove(&id);
                }
            },
            result = data_rx.recv() => {
                let (id, notif) = result.unwrap();
                match notif {
                    HostDataNotif::NewConn(conn) => {
                        eprintln!("recv new conn sender {id}");
                        active_conns.insert(id, conn);
                    },
                    HostDataNotif::Data(buf) => {
                        if buf.is_empty() {
                            eprintln!("remove new conn sender {id} from tcp host");
                            active_conns.remove(&id);
                        }
                        let length = buf.len() as u32;

                        eprintln!("sending {length} for {id}");

                        length_id_enc[0..4].copy_from_slice(&length.to_le_bytes());
                        length_id_enc[4..8].copy_from_slice(&id.to_le_bytes());
                        if let Err(e) = vsock_conn.write_all(&length_id_enc).await {
                            eprintln!("vsock length-id write error, quitting enclave comm: {e}");
                            return;
                        }
                        if let Err(e) = vsock_conn.write_all(&buf).await {
                            eprintln!("vsock buf write error, quitting enclave comm: {e}");
                            return;
                        }
                    },
                }
            }
        }
    }
}

#[cfg(not(feature = "all-tcp"))]
async fn run_in_enclave() {
    let mut vsock_listen = VsockListener::bind(ENCLAVE_VSOCK_CID, ENCLAVE_VSOCK_PORT).unwrap();
    eprintln!("Listening on vsock...");
    loop {
        let (stream, _) = vsock_listen.accept().await.unwrap();
        eprintln!("Accepted vsock connection");
        tokio::spawn(async move {
            enclave_vsock_comm_task(stream).await;
        });
    }
}

#[cfg(feature = "all-tcp")]
async fn run_in_enclave() {
    let vsock_listen = VsockListener::bind(ENCLAVE_LISTEN_ADDR).await.unwrap();
    eprintln!("Listening on mock vsock...");
    loop {
        let (stream, _) = vsock_listen.accept().await.unwrap();
        eprintln!("Accepted vsock connection");
        tokio::spawn(async move {
            enclave_vsock_comm_task(stream).await;
        });
    }
}

#[cfg(not(feature = "all-tcp"))]
async fn run_in_host() {
    let mut vsock_txs = Vec::new();
    eprintln!("Creating vsock connections");
    for _ in 0..CONN_COUNT {
        let vsock_conn = VsockStream::connect(ENCLAVE_VSOCK_CID, ENCLAVE_VSOCK_PORT)
            .await
            .unwrap();
        let (vsock_tx, vsock_rx) = unbounded_channel();
        vsock_txs.push(vsock_tx);
        tokio::spawn(async move {
            host_vsock_comm_task(vsock_conn, vsock_rx).await;
        });
    }
    let mut next_id = 1u32;
    let tcp_listen = TcpListener::bind(HOST_LISTEN_ADDR).await.unwrap();
    eprintln!("Listening on tcp {HOST_LISTEN_ADDR}...");
    loop {
        let id = next_id;
        next_id = next_id.overflowing_add(1).0;
        let (tcp_stream, _) = tcp_listen.accept().await.unwrap();
        let vsock_tx = vsock_txs.choose(&mut rand::thread_rng()).unwrap().clone();
        tokio::spawn(async move {
            host_tcp_comm_task(tcp_stream, id, vsock_tx).await;
        });
    }
}

#[cfg(feature = "all-tcp")]
async fn run_in_host() {
    let mut vsock_txs = Vec::new();
    eprintln!("Creating vsock connections");
    for _ in 0..CONN_COUNT {
        let vsock_conn = VsockStream::connect(ENCLAVE_LISTEN_ADDR).await.unwrap();
        let (vsock_tx, vsock_rx) = unbounded_channel();
        vsock_txs.push(vsock_tx);
        tokio::spawn(async move {
            host_vsock_comm_task(vsock_conn, vsock_rx).await;
        });
    }
    let mut next_id = 1u32;
    let tcp_listen = TcpListener::bind(HOST_LISTEN_ADDR).await.unwrap();
    eprintln!("Listening on tcp {HOST_LISTEN_ADDR}...");
    loop {
        let id = next_id;
        next_id = next_id.overflowing_add(1).0;
        let (tcp_stream, _) = tcp_listen.accept().await.unwrap();
        let vsock_tx = vsock_txs.choose(&mut rand::thread_rng()).unwrap().clone();
        tokio::spawn(async move {
            host_tcp_comm_task(tcp_stream, id, vsock_tx).await;
        });
    }
}

#[tokio::main]
async fn main() {
    // let curr_limits = rlimit::getrlimit(Resource::NOFILE).unwrap();
    // eprintln!("setting fd limit in randsrv curr = {:?}", curr_limits);
    // rlimit::setrlimit(Resource::NOFILE, 65535, 65535).unwrap();
    // let curr_limits = rlimit::getrlimit(Resource::NOFILE).unwrap();
    // eprintln!("set fd limit in randsrv! changed = {:?}", curr_limits);

    let on_host = !std::env::var("ON_HOST").unwrap_or_default().is_empty();
    if on_host {
        run_in_host().await;
    } else {
        run_in_enclave().await;
    }
}
