let peerWs = null;
let localWs = null;
let currentCall = null;
let isCaller = false;

const statusEl = document.getElementById("status");
const targetInput = document.getElementById("targetIp");
const callBtn = document.getElementById("callBtn");
const endCallBtn = document.getElementById("endCallBtn");
const incomingModal = document.getElementById("incomingModal");
const incomingCaller = document.getElementById("incomingCaller");
const acceptBtn = document.getElementById("acceptBtn");
const rejectBtn = document.getElementById("rejectBtn");
const micStatus = document.getElementById("micStatus");

window.onload = () => {
    logStatus("Idle");
    connectToLocal();
};

function logStatus(text) {
    statusEl.textContent = text;
}

function connectToLocal() {
    localWs = new WebSocket(`ws://${location.hostname}:3000/signal`);
    
    localWs.onopen = () => {
        console.log("Connected to local backend");
    };
    
    localWs.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        console.log("Local received:", msg);
        
        if (msg.type === "CallOffer" && !isCaller) {
            currentCall = msg.ip;
            incomingCaller.textContent = currentCall;
            incomingModal.style.display = "block";
            logStatus(`Incoming call from ${currentCall}`);
        } else if (msg.type === "CallEnd" && currentCall) {
            logStatus("Call ended by peer");
            resetUI();
        }
    };
    
    localWs.onclose = () => {
        console.log("Local WS closed, reconnecting...");
        setTimeout(connectToLocal, 2000);
    };
    
    localWs.onerror = () => {
        console.error("Local WS error");
    };
}

function resetUI() {
    currentCall = null;
    isCaller = false;
    callBtn.disabled = false;
    endCallBtn.disabled = true;
    micStatus.textContent = "🎤";
    incomingModal.style.display = "none";
    if (peerWs) {
        peerWs.close();
        peerWs = null;
    }
}

function endCurrentCall() {
    logStatus("Idle");
    resetUI();
}

function connectToPeer(peerIp, onOpen) {
    if (peerWs) peerWs.close();

    peerWs = new WebSocket(`ws://${peerIp}:3000/signal`);
    let connected = false;

    const timeout = setTimeout(() => {
        if (!connected) {
            peerWs.close();
            logStatus(`Connection timeout - ${peerIp} not reachable`);
            resetUI();
        }
    }, 5000);

    peerWs.onopen = () => {
        clearTimeout(timeout);
        connected = true;
        logStatus(`Connected to ${peerIp}`);
        if (onOpen) onOpen();
    };

    peerWs.onerror = () => {
        clearTimeout(timeout);
        if (!connected) {
            logStatus(`Unable to reach ${peerIp}`);
            resetUI();
        }
    };

    peerWs.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        console.log("Peer received:", msg);

        switch (msg.type) {
            case "CallAccept":
                logStatus(`In call with ${currentCall}`);
                micStatus.textContent = "🎤 On";
                break;
            case "CallReject":
                logStatus("Call was rejected");
                endCurrentCall();
                break;
            case "CallEnd":
                logStatus("Call ended by peer");
                endCurrentCall();
                break;
        }
    };

    peerWs.onclose = () => {
        clearTimeout(timeout);
    };
}

function sendToLocal(msg) {
    if (localWs && localWs.readyState === WebSocket.OPEN) {
        localWs.send(JSON.stringify(msg));
    } else {
        // Fallback: create new connection
        const ws = new WebSocket(`ws://${location.hostname}:3000/signal`);
        ws.onopen = () => {
            ws.send(JSON.stringify(msg));
            setTimeout(() => ws.close(), 100);
        };
    }
}

// Caller initiates
callBtn.addEventListener("click", () => {
    const targetIp = targetInput.value.trim();
    if (!targetIp || targetIp === "127.0.0.1" || targetIp === "0.0.0.0") {
        alert("Enter a valid target IP");
        return;
    }

    currentCall = targetIp;
    callBtn.disabled = true;
    endCallBtn.disabled = false;
    isCaller = true;

    connectToPeer(targetIp, () => {
        peerWs.send(JSON.stringify({ type: "CallOffer", ip: "0.0.0.0" }));
        logStatus(`Calling ${targetIp}...`);
    });
});

endCallBtn.addEventListener("click", () => {
    console.log("End call clicked, peerWs:", peerWs, "currentCall:", currentCall);
    
    // Send to peer
    if (peerWs && peerWs.readyState === WebSocket.OPEN) {
        peerWs.send(JSON.stringify({ type: "CallEnd" }));
    }
    
    // Send to local backend
    sendToLocal({ type: "CallEnd" });
    
    endCurrentCall();
});

// Callee accepts
acceptBtn.addEventListener("click", () => {
    const callerIp = currentCall;
    incomingModal.style.display = "none";

    // Enable end call button for callee
    callBtn.disabled = true;
    endCallBtn.disabled = false;

    // Connect to caller and send CallOffer + CallAccept
    connectToPeer(callerIp, () => {
        peerWs.send(JSON.stringify({ type: "CallOffer", ip: "0.0.0.0" }));
        peerWs.send(JSON.stringify({ type: "CallAccept" }));
    });

    // Tell our own backend to accept
    sendToLocal({ type: "CallAccept" });

    micStatus.textContent = "🎤 On";
    logStatus(`In call with ${callerIp}`);
});

// Callee rejects
rejectBtn.addEventListener("click", () => {
    incomingModal.style.display = "none";

    if (peerWs && peerWs.readyState === WebSocket.OPEN) {
        peerWs.send(JSON.stringify({ type: "CallReject" }));
    }
    sendToLocal({ type: "CallReject" });

    logStatus("Call rejected");
    currentCall = null;
});
