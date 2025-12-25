use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use tokio::{net::UdpSocket, sync::mpsc::Receiver};

use crate::{jitter::JitterBuffer, packet::AudioPacket};

pub async fn send_task(mut channel: Receiver<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    let socket = UdpSocket::bind("0.0.0.0:40000").await?;
    let _ = socket.connect("0.0.0.0:40001").await;
    loop {
        while let Some(msg) = channel.recv().await {
            let _ = socket.send(&msg).await;
        }
    }
}

pub async fn receive_task(jitter: Arc<Mutex<JitterBuffer>>) -> Result<(), Box<dyn Error>> {
    let socket = UdpSocket::bind("0.0.0.0:40001").await?;

    let mut buf = [0u8; 4096];
    let mut last_seq: Option<u16> = None;
    loop {
        while let Ok((size, _)) = socket.recv_from(&mut buf).await {
            if let Some(packet) = AudioPacket::deserialize(&buf[..size]) {
                if let Some(prev) = last_seq {
                    let exprected = prev.wrapping_add(1);
                    if packet.seq != exprected {
                        println!("Packet loss: exprected {}, got {}", exprected, packet.seq);
                    }
                }

                last_seq = Some(packet.seq);

                let mut jb = jitter.lock().unwrap();
                jb.push_packet(&packet.samples);
            }
        }
    }
}
