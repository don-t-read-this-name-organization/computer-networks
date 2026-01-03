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

use crate::{io::AudioState, jitter::JitterBuffer, packet::AudioPacket};

struct CallHandler {
    pub cancel_token: CancellationToken,
    pub send_handle: JoinHandle<()>,
}

pub async fn udp_task(
    audio_channel: BroadcastSender<Vec<u8>>,
    mut control_channel: SingleReceiver<(SocketAddr, String)>,
    jitter: Arc<Mutex<JitterBuffer>>,
    audio_state: Arc<Mutex<AudioState>>,
    tx_ws: BroadcastSender<String>,
) -> Result<(), Box<dyn Error>> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:40000").await?);
    let mut call_handler: Option<CallHandler> = None;

    // Spawn receive_task always
    let socket_recv = socket.clone();
    let jitter_recv = jitter.clone();
    let tx_ws_recv = tx_ws.clone();
    tokio::spawn(async move {
        let _ = receive_task(socket_recv, jitter_recv, CancellationToken::new(), tx_ws_recv).await;
    });

    loop {
        if let Some((address, msg)) = control_channel.recv().await {
            if msg.starts_with("pinging ") {
                let target_ip_str = msg.strip_prefix("pinging ").unwrap().trim();
                if let Ok(target_ip) = target_ip_str.parse::<std::net::IpAddr>() {
                    let target_addr = SocketAddr::new(target_ip, 40000);
                    // Send ping packet: seq=0, empty samples
                    let ping_packet = AudioPacket { seq: 0, samples: vec![] };
                    let data = ping_packet.serialize();
                    let _ = socket.send_to(&data, target_addr).await;
                    println!("Sent ping to {}", target_addr);
                }
            }
            if msg.contains("start_call") {
                let target_ip = address.ip();
                let target_addr = SocketAddr::new(target_ip, 40000);
                let _ = socket.connect(target_addr).await;
                println!("Starting call to {}", target_addr);
                let cancel_token = CancellationToken::new();
                let audio_rx = audio_channel.subscribe();

                let send_handle = {
                    let token = cancel_token.clone();
                    let socket_send = socket.clone();
                    tokio::spawn(async move {
                        let _ = send_task(socket_send, audio_rx, token).await;
                    })
                };

                call_handler = Some(CallHandler {
                    cancel_token,
                    send_handle,
                });
                let mut state = audio_state.lock().unwrap();
                let jitter_audio = jitter.clone();
                let input_channel = audio_channel.clone();
                state.start(input_channel, jitter_audio);
            }
            if msg.contains("end_call") {
                if let Some(call) = call_handler {
                    call.cancel_token.cancel();
                    let _ = call.send_handle.await;
                    call_handler = None;
                    let mut state = audio_state.lock().unwrap();
                    state.clear();
                    println!("call ended");
                }
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
                println!("Sent audio packet");
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
    tx_ws: BroadcastSender<String>,
) -> Result<(), Box<dyn Error>> {
    let mut buf = [0u8; 4096];
    let mut last_seq: Option<u16> = None;
    loop {
        tokio::select! {
            recv = socket.recv_from(&mut buf) => {
                if let Ok((size, _)) = recv {
                     if let Some(packet) = AudioPacket::deserialize(&buf[..size]) {
                      if packet.seq == 0 && packet.samples.is_empty() {
                          // It's a ping
                          let _ = tx_ws.send("pinging".to_string());
                          println!("Received ping");
                      } else {
                          println!("Received audio packet seq {}", packet.seq);
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
            _ = cancel_token.cancelled() => {
                println!("Receive task cancelled");
                break;
            }
        };
    }
    Ok(())
}
