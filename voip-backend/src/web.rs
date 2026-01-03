use std::net::SocketAddr;

use axum::{
    Router,
    extract::{ConnectInfo, WebSocketUpgrade, ws::WebSocket},
    response::{Html, IntoResponse},
    routing::{any, get},
    serve,
};
use axum::extract::ws::Message;
use axum_extra::TypedHeader;
use axum_extra::headers;
use axum_extra::headers::UserAgent;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::{net::TcpListener, sync::mpsc::Sender};
use tower_http::services::ServeDir;

pub async fn web_task(channel: Sender<(SocketAddr, String)>, tx_ws: BroadcastSender<String>) {
    let app = Router::new()
        .route("/", get(main_page))
        .route(
            "/signal",
            any({
                let channel = channel.clone();
                let tx_ws = tx_ws.clone();
                move |ws: WebSocketUpgrade,
                      ua: Option<TypedHeader<UserAgent>>,
                      info: ConnectInfo<SocketAddr>| {
                    ws_handler(ws, ua, info, channel, tx_ws)
                }
            }),
        )
        .nest_service("/assets", ServeDir::new("assets"));
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
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
    tx_ws: BroadcastSender<String>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, tx_channel, tx_ws))
}

async fn handle_socket(
    socket: WebSocket,
    who: SocketAddr,
    tx_channel: Sender<(SocketAddr, String)>,
    tx_ws: BroadcastSender<String>,
) {
    let rx_ws = tx_ws.subscribe();
    let (sender, receiver): (SplitSink<WebSocket, Message>, SplitStream<WebSocket>) = socket.split();

    let send_task = tokio::spawn(async move {
        let mut sender: SplitSink<WebSocket, Message> = sender;
        let mut rx_ws = rx_ws;
        while let Ok(msg) = rx_ws.recv().await {
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

    send_task.abort();
}
