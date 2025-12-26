use std::{
    error::Error,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::{
    net::UdpSocket,
    sync::{
        broadcast::Receiver as BroadcastReceiver, broadcast::Sender as BroadcastSender,
        mpsc::Receiver as SingleReceiver,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{jitter::JitterBuffer, packet::AudioPacket};

struct CallHandler {
    pub cancel_token: CancellationToken,
    pub send_handle: JoinHandle<()>,
    pub recv_handle: JoinHandle<()>,
}

pub async fn udp_task(
    mut audio_channel: BroadcastSender<Vec<u8>>,
    mut control_channel: SingleReceiver<(SocketAddr, String)>,
    jitter: Arc<Mutex<JitterBuffer>>,
) -> Result<(), Box<dyn Error>> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:40000").await?);
    loop {
        let mut call_handler: Option<CallHandler> = None;
        if let Some((address, msg)) = control_channel.recv().await {
            if msg.contains("start_call") {
                let target_ip = address.ip();
                let target_addr = SocketAddr::new(target_ip, 40000);
                let _ = socket.connect(target_addr).await;
                let cancel_token = CancellationToken::new();
                let audio_rx = audio_channel.subscribe();

                let send_handle = {
                    let token = cancel_token.clone();
                    let socket_send = socket.clone();
                    tokio::spawn(async move {
                        let _ = send_task(socket_send, audio_rx, token).await;
                    })
                };

                let recv_handle = {
                    let token = cancel_token.clone();
                    let socket_recv = socket.clone();
                    let jitter_clone = jitter.clone();
                    tokio::spawn(async move {
                        let _ = receive_task(socket_recv, jitter_clone, token).await;
                    })
                };

                call_handler = Some(CallHandler {
                    cancel_token,
                    send_handle,
                    recv_handle,
                });
            }
            if msg.contains("end_call") {
                unimplemented!("Will be implemented in a while");
            }
        }
    }
}

pub async fn send_task(
    socket: Arc<UdpSocket>,
    mut audio_channel: BroadcastReceiver<Vec<u8>>,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    loop {
        tokio::select! {
            Ok(msg) = audio_channel.recv() => {
                let _ = socket.send(&msg).await;
            }
            _ = cancel_token.cancelled() => {
                println!("Send task cancelled");
                break;
            }
        }
    }

    Ok(())
}

pub async fn receive_task(
    socket: Arc<UdpSocket>,
    jitter: Arc<Mutex<JitterBuffer>>,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    let mut buf = [0u8; 4096];
    let mut last_seq: Option<u16> = None;
    loop {
        tokio::select! {
            recv = socket.recv_from(&mut buf) => {
                if let Ok((size, _)) = recv {
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
            _ = cancel_token.cancelled() => {
                println!("Receive task cancelled");
                break;
            }
        };
    }
    Ok(())
}
