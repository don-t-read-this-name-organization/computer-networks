use std::net::SocketAddr;

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
use tokio::{net::TcpListener, sync::mpsc::Sender};
use tower_http::services::ServeDir;

pub async fn web_task(channel: Sender<(SocketAddr, String)>) {
    let app = Router::new()
        .route("/", get(main_page))
        .route(
            "/signal",
            any({
                move |ws: WebSocketUpgrade,
                      ua: Option<TypedHeader<UserAgent>>,
                      info: ConnectInfo<SocketAddr>| {
                    ws_handler(ws, ua, info, channel)
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
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    tx_channel: Sender<(SocketAddr, String)>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, tx_channel))
}

async fn handle_socket(
    mut socket: WebSocket,
    who: SocketAddr,
    tx_channel: Sender<(SocketAddr, String)>,
) {
    while let Some(Ok(msg)) = socket.recv().await {
        if let Ok(text) = msg.to_text() {
            tx_channel
                .send((who.clone(), text.to_string()))
                .await
                .unwrap_or_default();
        }
    }
}
