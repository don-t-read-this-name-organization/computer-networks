use std::{
    collections::HashMap,
    error::Error,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};

use tokio::{
    net::UdpSocket,
    sync::{
        broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender},
        mpsc,
        mpsc::Receiver as SingleReceiver,
    },
    task::JoinHandle,
    time::{interval, Duration},
};

use tokio_util::sync::CancellationToken;

use crate::{io::AudioState, jitter::JitterBuffer, packet::AudioPacket};

struct CallHandler {
    cancel_token: CancellationToken,
    send_handle: JoinHandle<()>,
}

pub async fn udp_task(
    audio_channel: BroadcastSender<Vec<u8>>,
    mut control_channel: SingleReceiver<(SocketAddr, String)>,
    jitter: Arc<Mutex<JitterBuffer>>,
    audio_state: Arc<Mutex<AudioState>>,
    clients: Arc<Mutex<HashMap<IpAddr, mpsc::Sender<String>>>>,
) -> Result<(), Box<dyn Error>> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:40000").await?);

    let mut call_handler: Option<CallHandler> = None;
    let mut caller_ip: Option<IpAddr> = None;

    let (tx_caller, mut rx_caller) = mpsc::channel::<IpAddr>(1);

    {
        let socket_recv = socket.clone();
        let jitter_recv = jitter.clone();
        let tx_caller_recv = tx_caller.clone();

        tokio::spawn(async move {
            let _ = receive_task(
                socket_recv,
                jitter_recv,
                CancellationToken::new(),
                clients.clone(),
                tx_caller_recv,
            )
            .await;
        });
    }

    loop {
        tokio::select! {
            Some(ip) = rx_caller.recv() => {
                caller_ip = Some(ip);
            }

            msg = control_channel.recv() => {
                if let Some((_addr, msg)) = msg {

                    if msg.starts_with("pinging ") {
                        let ip_str = msg.strip_prefix("pinging ").unwrap().trim();
                        if let Ok(ip) = ip_str.parse::<IpAddr>() {
                            let addr = SocketAddr::new(ip, 40000);
                            let packet = AudioPacket { seq: 0, samples: vec![] };
                            let _ = socket.send_to(&packet.serialize(), addr).await;
                        }
                    } else if msg.contains("start_call") {
                        if let Some(target_ip) = caller_ip {
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

                            call_handler = Some(CallHandler {
                                cancel_token,
                                send_handle,
                            });

                            audio_state
                                .lock()
                                .unwrap()
                                .start(audio_channel.clone(), jitter.clone());
                        }
                    } else if msg.contains("end_call") {
                        caller_ip = None;

                        if let Some(call) = call_handler.take() {
                            call.cancel_token.cancel();
                            let _ = call.send_handle.await;
                            audio_state.lock().unwrap().clear();
                        }
                    }
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
    let mut interval = interval(Duration::from_millis(50));
    let mut last_data = None;

    loop {
        tokio::select! {
            Ok(data) = audio_channel.recv() => {
                last_data = Some(data.clone());
                let _ = socket.send(&data).await;
                interval.reset();
            }
            _ = interval.tick() => {
                if let Some(data) = &last_data {
                    let _ = socket.send(data).await;
                }
            }
            _ = cancel_token.cancelled() => break,
        }
    }
    Ok(())
}

pub async fn receive_task(
    socket: Arc<UdpSocket>,
    jitter: Arc<Mutex<JitterBuffer>>,
    cancel_token: CancellationToken,
    clients: Arc<Mutex<HashMap<IpAddr, mpsc::Sender<String>>>>,
    tx_caller: mpsc::Sender<IpAddr>,
) -> Result<(), Box<dyn Error>> {
    let mut buf = [0u8; 4096];

    loop {
        tokio::select! {
            recv = socket.recv_from(&mut buf) => {
                if let Ok((size, addr)) = recv {
                    if let Some(packet) = AudioPacket::deserialize(&buf[..size]) {
                        if packet.seq == 0 {
                            let tx = {
                                let clients = clients.lock().unwrap();
                                clients.get(&addr.ip()).cloned()
                            };
                            if let Some(tx) = tx {
                                let _ = tx.send("pinging".to_string()).await;
                            }
                            let _ = tx_caller.send(addr.ip()).await;
                        } else {
                            jitter.lock().unwrap().push_packet(&packet.samples);
                        }
                    }
                }
            }
            _ = cancel_token.cancelled() => break,
        }
    }
    Ok(())
}
