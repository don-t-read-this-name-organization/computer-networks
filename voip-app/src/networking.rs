use std::{
    error::Error,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
};

use tokio::{
    net::UdpSocket,
    sync::{
        broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender},
        mpsc::Receiver as SingleReceiver,
    },
    task::JoinHandle,
};

use tokio_util::sync::CancellationToken;

use crate::{
    io::AudioState,
    jitter::JitterBuffer,
    packet::AudioPacket,
    signal::{CallState, ControlMessage},
};

struct CallHandler {
    cancel_token: CancellationToken,
    send_handle: JoinHandle<()>,
    recv_handle: JoinHandle<()>,
}

const UDP_PORT: u16 = 40000;

pub async fn udp_task(
    audio_channel: BroadcastSender<Vec<u8>>,
    mut control_channel: SingleReceiver<ControlMessage>,
    jitter: Arc<Mutex<JitterBuffer>>,
    audio_state: Arc<Mutex<AudioState>>,
) -> Result<(), Box<dyn Error>> {
    let socket =
        Arc::new(UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), UDP_PORT)).await?);
    // Remove the initial connect to 0.0.0.0 - it's not needed
    // socket.connect(...) - DELETE THIS
    
    let mut call_state = CallState::Idle;
    let mut call_handler: Option<CallHandler> = None;
    loop {
        if let Some(msg) = control_channel.recv().await {
            match msg {
                ControlMessage::CallOffer(ip) => match call_state {
                    CallState::Idle => {
                        let addr = SocketAddr::V4(SocketAddrV4::new(ip, UDP_PORT));
                        call_state = CallState::Connecting(addr);
                        socket.connect(addr).await;
                        println!("[UDP] Sockets are connected");
                    }
                    _ => {
                        println!("[UDP] impossible to start another call being already in a one");
                    }
                },
                ControlMessage::CallAccept => match call_state {
                    CallState::Connecting(addr) => {
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
                        call_state = CallState::InCall(addr);
                        println!("[UDP] Call has been accepted and started the audio flow.");
                    }
                    _ => {
                        println!(
                            "[UDP] impossible to accept a call without any info, transition is possible only from Connecting state."
                        );
                    }
                },
                ControlMessage::CallReject => match call_state {
                    CallState::Connecting(_addr) => {
                        // Remove connection
                        socket
                            .connect(SocketAddr::V4(SocketAddrV4::new(
                                Ipv4Addr::new(0, 0, 0, 0),
                                UDP_PORT,
                            )))
                            .await;
                        call_state = CallState::Idle;
                        println!("[UDP] Call has been rejected, moving to Idle.");
                    }
                    _ => {
                        println!(
                            "[UDP] impossible to reject a call without any info, transition is possible only from Connecting state."
                        );
                    }
                },
                ControlMessage::CallEnd => match call_state {
                    CallState::InCall(_addr) => {
                        if let Some(call) = call_handler {
                            call.cancel_token.cancel();
                            let (_, _) = tokio::join!(call.send_handle, call.recv_handle);
                            call_handler = None;
                            {
                                let mut state = audio_state.lock().unwrap();
                                state.clear();
                            }
                            {
                                // Clear jitter buffer
                                let mut jb = jitter.lock().unwrap();
                                jb.clear();
                            }
                            socket
                                .connect(SocketAddr::V4(SocketAddrV4::new(
                                    Ipv4Addr::new(0, 0, 0, 0),
                                    UDP_PORT,
                                )))
                                .await;
                            call_state = CallState::Idle;
                            println!("[UDP] Call has been ended, moving to Idle.");
                        }
                    }
                    _ => {
                        println!("[UDP] impossible to reject a non-existing call.");
                    }
                },
            }
        }
    }
}

pub async fn send_task(
    socket: Arc<UdpSocket>,
    mut audio_channel: BroadcastReceiver<Vec<u8>>,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    let mut count = 0u64;
    
    loop {
        tokio::select! {
            result = audio_channel.recv() => {
                match result {
                    Ok(data) => {
                        count += 1;
                        if count % 100 == 0 {
                            println!("[SEND-UDP] Sent {} packets, last size: {}", count, data.len());
                        }
                        let _ = socket.send(&data).await;
                    }
                    Err(e) => {
                        eprintln!("[SEND-UDP] Channel error: {:?}", e);
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                println!("[SEND-UDP] Send task cancelled, total sent: {}", count);
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
    let mut count = 0u64;
    let mut lost_packets = 0u64;
    
    println!("[RECV-UDP] Starting receive task...");
    
    loop {
        tokio::select! {
            recv = socket.recv(&mut buf) => {
                match recv {
                    Ok(size) => {
                        count += 1;
                        if count % 100 == 0 {
                            println!("[RECV-UDP] Received {} packets, jitter len: {}, lost: {}", 
                                count, jitter.lock().unwrap().len(), lost_packets);
                        }
                        if let Some(packet) = AudioPacket::deserialize(&buf[..size]) {
                            if let Some(prev) = last_seq {
                                let expected = prev.wrapping_add(1);
                                if packet.seq != expected {
                                    let gap = packet.seq.wrapping_sub(prev).wrapping_sub(1);
                                    lost_packets += gap as u64;
                                }
                            }
                            last_seq = Some(packet.seq);
                            let mut jb = jitter.lock().unwrap();
                            jb.push_packet(&packet.samples);
                        } else {
                            println!("[RECV-UDP] Failed to deserialize packet of size {}", size);
                        }
                    }
                    Err(e) => {
                        println!("[RECV-UDP] Recv error: {:?}", e);
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                println!("[RECV-UDP] Receive task cancelled, total received: {}, lost: {}", count, lost_packets);
                break;
            }
        };
    }
    Ok(())
}
