let socket = null;

function connect() {
    if (socket && socket.readyState === WebSocket.OPEN) {
        console.log("Already connected");
        return;
    }

    socket = new WebSocket(`ws://${location.host}/signal`);

    socket.onopen = () => {
        console.log("WebSocket connected");
    };

    socket.onmessage = (event) => {
        console.log("Message from server:", event.data);
    };

    socket.onclose = () => {
        console.log("WebSocket closed");
    };

    socket.onerror = (err) => {
        console.error("WebSocket error:", err);
    };
}

function startCall() {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
        console.warn("Socket not connected");
        return;
    }

    socket.send("start_call");
    console.log("start_call sent");
}

function endCall() {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
        console.warn("Socket not connected");
        return;
    }

    socket.send("end_call");
    console.log("end_call sent");
}

