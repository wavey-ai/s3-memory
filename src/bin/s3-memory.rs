use s3_memory::MemoryS3;
use std::net::{Ipv4Addr, SocketAddr};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let address = match (args.next(), args.next(), args.next()) {
        (None, None, None) => SocketAddr::from((Ipv4Addr::LOCALHOST, 9000)),
        (Some(flag), Some(address), None) if flag == "--listen" => address.parse()?,
        _ => return Err("usage: s3-memory [--listen 127.0.0.1:9000]".into()),
    };
    let server = MemoryS3::new().listen(address).await?;
    println!("s3-memory ready at {}", server.endpoint());
    tokio::signal::ctrl_c().await?;
    server.stop().await;
    Ok(())
}
