use std::{
    error::Error,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use cpal::default_host;
use tokio::{
    signal::ctrl_c,
    sync::{broadcast, mpsc},
};

use crate::{io::AudioState, jitter::JitterBuffer, networking::udp_task, web::web_task};

pub mod io;
pub mod jitter;
pub mod networking;
pub mod packet;
pub mod web;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = default_host();

    let audio_state = Arc::new(Mutex::new(AudioState::new(host)));
    let (tx_control, rx_control) = mpsc::channel::<(SocketAddr, String)>(32);
    let (tx_audio, _rx_audio) = broadcast::channel::<Vec<u8>>(128);
    let jitter_buffer = Arc::new(Mutex::new(JitterBuffer::new()));
    let jitter_udp = jitter_buffer.clone();
    let audio_clone = audio_state.clone();

    //  MUTE + LOGGING: shared peer mute state
    let peer_mute_state = Arc::new(Mutex::new(std::collections::HashMap::<SocketAddr, bool>::new()));

    //  Start web server (which also handles logs and peers)
    tokio::spawn(web_task(tx_control));

    //  Start UDP audio task with mute state
    tokio::spawn(async move {
        if let Err(e) = udp_task(tx_audio, rx_control, jitter_udp, audio_clone, peer_mute_state).await {
            eprintln!("udp task error: {}", e);
        }
    });

    let _ = ctrl_c().await;
    Ok(())
}