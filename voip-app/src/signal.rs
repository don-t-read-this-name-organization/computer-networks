use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr};

pub(crate) enum CallState {
    Idle,
    Connecting(SocketAddr),
    InCall(SocketAddr),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "ip")]
pub(crate) enum ControlMessage {
    CallOffer(Ipv4Addr),
    CallAccept,
    CallReject,
    CallEnd,
}
