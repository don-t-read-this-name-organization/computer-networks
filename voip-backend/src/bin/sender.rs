use cpal::default_host;
use tokio::signal::ctrl_c;
use voip_backend::{io::input_stream_fn, networking::send_task, web::web_task};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(128);
    let _stream = input_stream_fn(default_host(), tx)?;
    tokio::spawn(async move {
        if let Err(e) = send_task(rx).await {
            eprintln!("send task error: {}", e);
        }
    });
    tokio::spawn(web_task());
    ctrl_c().await?;
    Ok(())
}
