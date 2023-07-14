mod enclave;
mod host;

use agnostic::{DEFAULT_ENCLAVE_PROXY_ADDR, DEFAULT_HOST_PROXY_ADDR};
use anyhow::{Context, Result};
use clap::{command, Parser};
use rand::seq::SliceRandom;
use rlimit::Resource;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::unbounded_channel,
};
use tracing::{error, info, metadata::LevelFilter};
use tracing_subscriber::EnvFilter;

use crate::{
    agnostic::{connect_to_enclave, listen_in_enclave},
    enclave::EnclaveProxyTask,
    host::{HostProxyTask, HostTerminusTask},
};

#[cfg(not(feature = "enclave-tcp"))]
mod agnostic {
    use anyhow::{anyhow, Result};
    use tokio_vsock::{VsockAddr, VsockListener, VsockStream};

    pub type EnclaveListener = VsockListener;
    pub type EnclaveStream = VsockStream;

    pub const DEFAULT_ENCLAVE_PROXY_ADDR: &str = "4:9443";
    pub const DEFAULT_HOST_PROXY_ADDR: &str = "0.0.0.0:8443";

    pub fn parse_vsock_addr(address: &str) -> Result<VsockAddr> {
        let mut address_split = address.split(":");
        let cid = address_split
            .next()
            .ok_or(anyhow!("missing cid from vsock addr: {address}"))?
            .parse()?;
        let port = address_split
            .next()
            .ok_or(anyhow!("missing port from vsock addr: {address}"))?
            .parse()?;
        Ok(VsockAddr::new(cid, port))
    }

    pub async fn connect_to_enclave(address: &str) -> Result<EnclaveStream> {
        let addr = parse_vsock_addr(address)?;
        Ok(VsockStream::connect(addr.cid(), addr.port()).await?)
    }

    pub async fn listen_in_enclave(address: &str) -> Result<EnclaveListener> {
        let addr = parse_vsock_addr(address)?;
        Ok(VsockListener::bind(addr.cid(), addr.port())?)
    }
}

#[cfg(feature = "enclave-tcp")]
mod agnostic {
    use anyhow::Result;
    use tokio::net::{TcpListener, TcpStream};

    pub type EnclaveListener = TcpListener;
    pub type EnclaveStream = TcpStream;

    pub const DEFAULT_ENCLAVE_PROXY_ADDR: &str = "0.0.0.0:9443";
    pub const DEFAULT_HOST_PROXY_ADDR: &str = "0.0.0.0:9543";

    pub async fn connect_to_enclave(address: &str) -> Result<EnclaveStream> {
        Ok(TcpStream::connect(address).await?)
    }

    pub async fn listen_in_enclave(address: &str) -> Result<EnclaveListener> {
        Ok(TcpListener::bind(address).await?)
    }
}

#[derive(Parser, Debug)]
#[command(version)]
struct Cli {
    #[arg(short = 'H', long, default_value_t = false)]
    run_on_host: bool,

    #[arg(short, long, default_value_t = 1)]
    pool_size: usize,

    #[arg(long, default_value_t = 8192)]
    max_chunk_size: usize,

    #[arg(long, default_value_t = 10)]
    conn_cleanup_interval_secs: u64,

    #[arg(long, default_value_t = 10)]
    dead_conn_ttl_secs: u64,

    #[arg(short = 'c', long, default_value = "127.0.0.1:8443")]
    enclave_connect_address: String,

    #[arg(short = 'l', long, default_value = DEFAULT_ENCLAVE_PROXY_ADDR)]
    enclave_listen_address: String,

    #[arg(short = 'L', long, default_value = DEFAULT_HOST_PROXY_ADDR)]
    host_listen_address: String,

    #[arg(short = 'r', long, default_value_t = false)]
    increase_nofile_rlimit: bool,
}

async fn run_in_enclave(args: &Cli) -> Result<()> {
    let mut enclave_listener = listen_in_enclave(&args.enclave_listen_address)
        .await
        .context("failed to start enclave listener")?;
    info!("Listening on {}...", args.enclave_listen_address);
    loop {
        match enclave_listener.accept().await {
            Ok((stream, _)) => {
                let conn_cleanup_interval_secs = args.conn_cleanup_interval_secs;
                let dead_conn_ttl_secs = args.dead_conn_ttl_secs;
                let buf_size = args.max_chunk_size;
                let terminus_address = args.enclave_connect_address.clone();
                tokio::spawn(async move {
                    let task = EnclaveProxyTask::new(
                        stream,
                        conn_cleanup_interval_secs,
                        dead_conn_ttl_secs,
                        buf_size,
                        terminus_address,
                    );
                    if let Err(e) = task.run().await {
                        error!("enclave proxy task failed: {e}");
                    }
                });
            }
            Err(e) => error!("failed to accept connection: {e}"),
        }
    }
}

async fn run_on_host(args: &Cli) -> Result<()> {
    // let mut to_enclave_txs = Vec::new();
    // info!("Setting up {} enclave connections...", args.pool_size);
    // for _ in 0..args.pool_size {
    //     let (to_enclave_tx, from_terminus_rx) = unbounded_channel();
    //     to_enclave_txs.push(to_enclave_tx);
    //     let task = HostProxyTask::new(&args.enclave_listen_address, from_terminus_rx).await?;
    //     tokio::spawn(async move {
    //         if let Err(e) = task.run().await {
    //             error!("enclave proxy task failed: {e}");
    //         }
    //     });
    // }

    let mut next_id = 1u32;
    let host_listener = TcpListener::bind(&args.host_listen_address)
        .await
        .context("failed to start host listener")?;
    info!("Listening on tcp {}...", args.host_listen_address);
    loop {
        // let id = next_id;
        // next_id = next_id.overflowing_add(1).0;
        match host_listener.accept().await {
            Ok((tcp_stream, _)) => {
                // let to_enclave_tx = to_enclave_txs
                //     .choose(&mut rand::thread_rng())
                //     .expect("should be able to select random to_enclave_tx")
                //     .clone();
                let buf_size = args.max_chunk_size;
                let enclave_listen_address = args.enclave_listen_address.clone();
                tokio::spawn(async move {
                    let dest_stream = connect_to_enclave(&enclave_listen_address).await.unwrap();
                    let result = async {
                        let task = HostTerminusTask::new(tcp_stream, dest_stream, buf_size)?;
                        task.run().await
                    };
                    if let Err(e) = result.await {
                        error!("tcp terminus task failed: {e}");
                    }
                });
            }
            Err(e) => error!("failed to accept connection: {e}"),
        }
    }
}

fn increase_nofile_rlimit() -> Result<()> {
    let curr_limits = rlimit::getrlimit(Resource::NOFILE)?;
    info!("Current fd limits = {:?}", curr_limits);

    rlimit::setrlimit(Resource::NOFILE, 65535, 65535)?;
    let curr_limits = rlimit::getrlimit(Resource::NOFILE)?;
    info!(
        "Attempted fd limit change! Current fd limits = {:?}",
        curr_limits
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    if args.increase_nofile_rlimit {
        increase_nofile_rlimit().context("failed to increase fd limit")?;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .expect("should create tracing subscriber env filter"),
        )
        .init();

    if args.run_on_host {
        run_on_host(&args).await
    } else {
        run_in_enclave(&args).await
    }
}
