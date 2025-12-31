use std::{
    error::Error,
    net::{SocketAddr, Ipv4Addr},
    str::FromStr,
    sync::{Arc, Mutex},
};

use tokio::{
    net::UdpSocket,
    sync::{
        broadcast::Receiver as BroadcastReceiver,
        broadcast::Sender as BroadcastSender,
        mpsc::Receiver as SingleReceiver,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{io::AudioState, jitter::JitterBuffer, packet::AudioPacket};

type PeerMuteState = Arc<Mutex<std::collections::HashMap<SocketAddr, bool>>>;
type PeerTarget = Arc<Mutex<std::collections::HashMap<SocketAddr, String>>>;

struct CallHandler {
    pub cancel_token: CancellationToken,
    pub send_handle: JoinHandle<()>,
    pub recv_handle: JoinHandle<()>,
}

pub async fn udp_task(
    audio_channel: BroadcastSender<Vec<u8>>,
    mut control_channel: SingleReceiver<(SocketAddr, String)>,
    jitter: Arc<Mutex<JitterBuffer>>,
    audio_state: Arc<Mutex<AudioState>>,
    peer_mute_state: PeerMuteState,
    peer_target: PeerTarget,
) -> Result<(), Box<dyn Error>> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:40000").await?);
    let mut call_handler: Option<CallHandler> = None;

    loop {
        if let Some((address, msg)) = control_channel.recv().await {
            if msg.contains("start_call") {
                // Get target IP from peer_target map, fallback to sender's IP
                let target_ip_str = {
                    let targets = peer_target.lock().unwrap();
                    targets.get(&address).cloned().unwrap_or_else(|| address.ip().to_string())
                };

                // Parse IP
                let target_ip = if let Ok(ip) = Ipv4Addr::from_str(&target_ip_str) {
                    ip
                } else {
                    eprintln!("Invalid target IP: {}", target_ip_str);
                    continue;
                };

                let target_addr = SocketAddr::new(target_ip.into(), 40000);
                let _ = socket.connect(target_addr).await;
                let cancel_token = CancellationToken::new();
                let audio_rx = audio_channel.subscribe();

                let send_handle = {
                    let token = cancel_token.clone();
                    let socket_send = socket.clone();
                    let mute_state = peer_mute_state.clone();
                    let peer_addr = address;
                    tokio::spawn(async move {
                        let _ = send_task(socket_send, audio_rx, token, mute_state, peer_addr).await;
                    })
                };

                let recv_handle = {
                    let token = cancel_token.clone();
                    let socket_recv = socket.clone();
                    let jitter_recv = jitter.clone();
                    tokio::spawn(async move {
                        let _ = receive_task(socket_recv, jitter_recv, token).await;
                    })
                };

                call_handler = Some(CallHandler {
                    cancel_token,
                    send_handle,
                    recv_handle,
                });

                let mut state = audio_state.lock().unwrap();
                let jitter_audio = jitter.clone();
                let input_channel = audio_channel.clone();
                state.start(input_channel, jitter_audio);
            }

            if msg.contains("end_call") {
                if let Some(call) = call_handler {
                    call.cancel_token.cancel();
                    let (_, _) = tokio::join!(call.send_handle, call.recv_handle);
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
    mute_state: PeerMuteState,
    peer_addr: SocketAddr,
) -> Result<(), Box<dyn Error>> {
    loop {
        tokio::select! {
            Ok(msg) = audio_channel.recv() => {
                let is_muted = {
                    let state = mute_state.lock().unwrap();
                    *state.get(&peer_addr).unwrap_or(&false)
                };

                if !is_muted {
                    let _ = socket.send(&msg).await;
                }
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
                            let expected = prev.wrapping_add(1);
                            if packet.seq != expected {
                                println!("Packet loss: expected {}, got {}", expected, packet.seq);
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