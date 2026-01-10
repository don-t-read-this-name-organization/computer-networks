use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        ConnectInfo, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::{Html, IntoResponse},
    routing::{any, get},
    serve,
};
use axum_extra::TypedHeader;
use axum_extra::headers::UserAgent;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc::Sender},
};
use tower_http::services::ServeDir;

use crate::signal::ControlMessage;

#[derive(Clone)]
struct AppState {
    udp_tx: Sender<ControlMessage>,
    ws_broadcast: broadcast::Sender<String>,
}

pub async fn web_task(channel: Sender<ControlMessage>) {
    let (ws_broadcast, _) = broadcast::channel::<String>(100);
    
    let state = AppState {
        udp_tx: channel,
        ws_broadcast,
    };

    let app = Router::new()
        .route("/", get(main_page))
        .route("/signal", any(ws_handler))
        .nest_service("/assets", ServeDir::new("assets"))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn main_page() -> Html<&'static str> {
    Html(include_str!("../assets/index.html"))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("[WS-TASK] `{user_agent}` at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, state))
}

async fn handle_socket(socket: WebSocket, who: SocketAddr, state: AppState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let mut broadcast_rx = state.ws_broadcast.subscribe();

    // Task to forward broadcast messages to this WebSocket client
    let send_task = tokio::spawn(async move {
        while let Ok(msg) = broadcast_rx.recv().await {
            if ws_sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    // Receive messages from this WebSocket client
    while let Some(msg) = ws_receiver.next().await {
        if let Ok(msg) = msg {
            if let Ok(text) = msg.to_text() {
                match serde_json::from_str::<ControlMessage>(&text) {
                    Ok(ctrl_msg) => {
                        let (broadcast_msg, udp_msg) = match &ctrl_msg {
                            ControlMessage::CallOffer(_) => {
                                let ipv4 = match who.ip() {
                                    std::net::IpAddr::V4(ip) => ip,
                                    std::net::IpAddr::V6(_) => std::net::Ipv4Addr::new(0, 0, 0, 0),
                                };
                                let with_ip = ControlMessage::CallOffer(ipv4);
                                let json = serde_json::to_string(&with_ip).unwrap();
                                (json, with_ip)
                            }
                            _ => (text.to_string(), ctrl_msg),
                        };

                        // Broadcast to ALL connected clients
                        let _ = state.ws_broadcast.send(broadcast_msg);

                        // Send to UDP task
                        if let Err(e) = state.udp_tx.send(udp_msg).await {
                            eprintln!("[WS-TASK] Failed to send: {:?}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("[WS-TASK] Invalid message from {}: {:?}", who, e);
                    }
                }
            }
        }
    }

    send_task.abort();
    println!("[WS-TASK] WebSocket disconnected: {}", who);
}
