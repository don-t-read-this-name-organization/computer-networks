use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::{
    Router,
    extract::{ConnectInfo, WebSocketUpgrade, ws::WebSocket},
    response::{Html, IntoResponse},
    routing::{any, get},
    serve,
};
use axum_extra::{
    TypedHeader,
    headers::{self, UserAgent},
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, broadcast},
};
use tower_http::services::ServeDir;

type PeerState = Arc<Mutex<HashMap<SocketAddr, bool>>>;
type LogSender = broadcast::Sender<String>;

pub async fn web_task(control_tx: mpsc::Sender<(SocketAddr, String)>) {
    // Broadcast channel for logs (sent to all connected clients)
    let (log_tx, _) = broadcast::channel::<String>(64);

    let peer_mute_state: PeerState = Arc::new(Mutex::new(HashMap::new()));

    let app = Router::new()
        .route("/", get(main_page))
        .route(
            "/signal",
            any({
                move |ws: WebSocketUpgrade,
                      ua: Option<TypedHeader<UserAgent>>,
                      info: ConnectInfo<SocketAddr>| {
                    let peer_state = peer_mute_state.clone();
                    let log_tx_ws = log_tx.clone();
                    let control_tx_clone = control_tx.clone();
                    ws_handler(ws, ua, info, control_tx_clone, peer_state, log_tx_ws)
                }
            }),
        )
        .nest_service("/assets", ServeDir::new("assets"));

    // 👇 Bind to all interfaces for multi-PC access
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("DEBUG: Server starting on 0.0.0.0:3000");

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
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    tx_channel: mpsc::Sender<(SocketAddr, String)>,
    peer_mute_state: PeerState,
    log_tx: LogSender,
) -> impl IntoResponse {
    let user_agent_str = if let Some(TypedHeader(ua)) = user_agent {
        ua.to_string()
    } else {
        "Unknown".to_string()
    };

    // Log peer connection
    let log_msg = format!("Peer {} connected", addr);
    println!("{}", log_msg);
    let _ = log_tx.send(log_msg.clone());

    // Initialize peer as unmuted
    {
        let mut state = peer_mute_state.lock().unwrap();
        state.insert(addr, false);
    }

    // Notify all clients of peer list update
    let _ = log_tx.send(format!("PEER_LIST_UPDATE:{}", addr));

    ws.on_upgrade(move |socket| {
        handle_socket(socket, addr, tx_channel, peer_mute_state, log_tx, user_agent_str)
    })
}

async fn handle_socket(
    mut socket: WebSocket,
    who: SocketAddr,
    tx_channel: mpsc::Sender<(SocketAddr, String)>,
    peer_mute_state: PeerState,
    log_tx: LogSender,
    _user_agent: String,
) {
    while let Some(Ok(msg)) = socket.recv().await {
        if let Ok(text) = msg.to_text() {
            if text == "mute" || text == "unmute" {
                let is_muted = text == "mute";
                let mut state = peer_mute_state.lock().unwrap();
                state.insert(who, is_muted);

                let log_msg = format!("Peer {} is now {}", who, if is_muted { "muted" } else { "unmuted" });
                println!("{}", log_msg);
                let _ = log_tx.send(log_msg);
                let _ = log_tx.send(format!("PEER_LIST_UPDATE:{}", who));
            } else if text == "start_call" || text == "end_call" {
                let _ = tx_channel.send((who, text.to_string())).await;
                let action = if text == "start_call" { "started call" } else { "ended call" };
                let log_msg = format!("Peer {} {}", who, action);
                println!("{}", log_msg);
                let _ = log_tx.send(log_msg);
            } else {
                // Forward other messages (currently none)
                let _ = tx_channel.send((who, text.to_string())).await;
            }
        }
    }

    // On disconnect
    {
        let mut state = peer_mute_state.lock().unwrap();
        state.remove(&who);
    }
    let _ = log_tx.send(format!("Peer {} disconnected", who));
    let _ = log_tx.send(format!("PEER_LIST_UPDATE:{}", who));
}