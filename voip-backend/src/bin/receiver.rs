use tokio::signal::ctrl_c;
use voip_backend::{io::output_stream_fn, jitter::JitterBuffer, networking::receive_task};

use std::sync::{Arc, Mutex};

use cpal::default_host;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jitter = Arc::new(Mutex::new(JitterBuffer::new()));
    let jitter_networking = jitter.clone();
    let jitter_audio = jitter.clone();
    let host = default_host();
    tokio::spawn(async move {
        if let Err(e) = receive_task(jitter_networking).await {
            eprintln!("receive task error: {}", e);
        }
    });
    let _output_stream = output_stream_fn(host, jitter_audio)?;
    ctrl_c().await?;
    Ok(())
}
