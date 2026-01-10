use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use cpal::default_host;
use tokio::{
    signal::ctrl_c,
    sync::{broadcast, mpsc},
};

use crate::{
    io::AudioState, jitter::JitterBuffer, networking::udp_task, signal::ControlMessage,
    web::web_task,
};

pub mod io;
pub mod jitter;
pub mod networking;
pub mod packet;
pub mod signal;
pub mod web;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = default_host();

    let audio_state = Arc::new(Mutex::new(AudioState::new(host)));
    let (tx_control, rx_control) = mpsc::channel::<ControlMessage>(32);
    let (tx_audio, _rx_audio) = broadcast::channel::<Vec<u8>>(128);
    let jitter_buffer = Arc::new(Mutex::new(JitterBuffer::new()));
    let jitter_udp = jitter_buffer.clone();
    let audio_clone = audio_state.clone();

    tokio::spawn(web_task(tx_control));
    tokio::spawn(async move {
        if let Err(e) = udp_task(tx_audio, rx_control, jitter_udp, audio_clone).await {
            eprintln!("[MAIN] udp task error: {}", e);
        }
    });
    let _ = ctrl_c().await;
    Ok(())
}
