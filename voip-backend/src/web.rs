use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use axum::{
    Router,
    extract::{ConnectInfo, WebSocketUpgrade, ws::WebSocket},
    response::{Html, IntoResponse},
    routing::{any, get},
    serve,
};
use axum::extract::ws::Message;
use axum_extra::TypedHeader;
use axum_extra::headers::UserAgent;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use tokio::sync::mpsc::{self, Sender};
use tokio::{net::TcpListener};
use tower_http::services::ServeDir;

pub async fn web_task(channel: Sender<(SocketAddr, String)>, clients: std::sync::Arc<std::sync::Mutex<HashMap<IpAddr, Sender<String>>>>) {
    let app = Router::new()
        .route("/", get(main_page))
        .route(
            "/signal",
            any({
                let channel = channel.clone();
                let clients = clients.clone();
                move |ws: WebSocketUpgrade,
                      ua: Option<TypedHeader<UserAgent>>,
                      info: ConnectInfo<SocketAddr>| {
                    ws_handler(ws, ua, info, channel, clients)
                }
            }),
        )
        .nest_service("/assets", ServeDir::new("assets"));
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("DEBUG: Before serve");
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
    tx_channel: Sender<(SocketAddr, String)>,
    clients: std::sync::Arc<std::sync::Mutex<HashMap<IpAddr, Sender<String>>>>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, tx_channel, clients))
}

async fn handle_socket(
    socket: WebSocket,
    who: SocketAddr,
    tx_channel: Sender<(SocketAddr, String)>,
    clients: std::sync::Arc<std::sync::Mutex<HashMap<IpAddr, Sender<String>>>>,
) {
    let (tx_client, rx_client) = mpsc::channel(32);
    clients.lock().unwrap().insert(who.ip(), tx_client);

    let (sender, receiver): (SplitSink<WebSocket, Message>, SplitStream<WebSocket>) = socket.split();

    let send_task = tokio::spawn(async move {
        let mut sender: SplitSink<WebSocket, Message> = sender;
        let mut rx_client = rx_client;
        while let Some(msg) = rx_client.recv().await {
            if sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    let mut receiver: SplitStream<WebSocket> = receiver;
    while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
            if let Ok(text) = msg.to_text() {
                tx_channel
                    .send((who.clone(), text.to_string()))
                    .await
                    .unwrap_or_default();
            }
        }
    }

    clients.lock().unwrap().remove(&who.ip());
    send_task.abort();
}
