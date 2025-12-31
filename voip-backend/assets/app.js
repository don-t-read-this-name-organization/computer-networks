const themeToggle = document.getElementById('themeToggle');
const body = document.body;
const savedTheme = localStorage.getItem('theme') || 'dark';
if (savedTheme === 'light') body.classList.add('light');

themeToggle.addEventListener('click', () => {
    body.classList.toggle('light');
    const newTheme = body.classList.contains('light') ? 'light' : 'dark';
    localStorage.setItem('theme', newTheme);
});

const visualizer = document.getElementById('visualizer');
const barCount = 24;
for (let i = 0; i < barCount; i++) {
    const bar = document.createElement('div');
    bar.className = 'bar';
    visualizer.appendChild(bar);
}
const bars = visualizer.querySelectorAll('.bar');

let audioContext = null;
let analyser = null;
let microphone = null;
let source = null;
let isMuted = false;
let isLocalAudioActive = false;

async function initAudio() {
    if (audioContext) return;
    try {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
        analyser = audioContext.createAnalyser();
        analyser.fftSize = 64;
        const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
        microphone = stream;
        source = audioContext.createMediaStreamSource(stream);
        source.connect(analyser);
        isLocalAudioActive = true;
        animateVisualizer();
    } catch (err) {
        console.warn("Microphone access denied:", err);
        isLocalAudioActive = false;
    }
}

function animateVisualizer() {
    if (!analyser || !isLocalAudioActive) return;
    const bufferLength = analyser.frequencyBinCount;
    const dataArray = new Uint8Array(bufferLength);

    function draw() {
        if (!isLocalAudioActive) return;
        requestAnimationFrame(draw);
        analyser.getByteFrequencyData(dataArray);
        for (let i = 0; i < bars.length; i++) {
            const value = dataArray[i] || 0;
            const height = Math.max(2, value / 255 * 30);
            bars[i].style.height = height + 'px';
            bars[i].style.backgroundColor = isMuted ? '#72767D' : '#5865F2';
        }
    }
    draw();
}

const peers = new Map();
let socket = null;
let isConnected = false;
let isCalling = false;
let callStartTime = null;
let timerInterval = null;
let localIp = null;

const statusEl = document.getElementById('status');
const connectBtn = document.getElementById('connectBtn');
const startCallBtn = document.getElementById('startCallBtn');
const endCallBtn = document.getElementById('endCallBtn');
const muteBtn = document.getElementById('muteBtn');
const timerEl = document.getElementById('timer');
const errorMessage = document.getElementById('errorMessage');
const peersList = document.getElementById('peersList');
const logsOutput = document.getElementById('logsOutput');
const container = document.querySelector('.container');
const peerIpInput = document.getElementById('peerIpInput');
const setPeerBtn = document.getElementById('setPeerBtn');

function addLog(message, className = '') {
    const entry = document.createElement('div');
    entry.className = `log-entry ${className}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    logsOutput.appendChild(entry);
    logsOutput.scrollTop = logsOutput.scrollHeight;
}

function showError(msg) {
    errorMessage.textContent = msg;
    addLog(msg, 'error');
    setTimeout(() => errorMessage.textContent = '', 3000);
}

function setLoading(element, isLoading) {
    if (isLoading) {
        element.classList.add('loading');
        element.dataset.original = element.textContent;
        element.textContent = '⋯';
    } else {
        element.classList.remove('loading');
        if (element.dataset.original) {
            element.textContent = element.dataset.original;
        }
    }
}

function renderPeersList() {
    const allPeers = new Map(peers);
    if (localIp && isConnected) {
        allPeers.set(localIp, { muted: isMuted, inCall: isCalling });
    }

    if (allPeers.size === 0) {
        peersList.innerHTML = '<div class="peer"><div class="peer-ip">No peers connected</div></div>';
        return;
    }

    peersList.innerHTML = '';
    for (const [ip, info] of allPeers.entries()) {
        const peerEl = document.createElement('div');
        peerEl.className = 'peer';
        let statusText = info.muted ? ' (muted)' : '';
        if (info.inCall) statusText += ' 📞';
        if (ip === peerIpInput.value.trim()) {
            statusText += ' 🔍 Target';
        }
        peerEl.innerHTML = `<div class="peer-ip">${ip}${statusText}</div>`;
        peersList.appendChild(peerEl);
    }
}

function updateUI() {
    if (isConnected && !isCalling) {
        statusEl.textContent = "Connected";
        statusEl.className = "status connected";
        startCallBtn.disabled = false;
        endCallBtn.disabled = true;
        muteBtn.disabled = false;
        container.classList.remove('in-call');
    } else if (isConnected && isCalling) {
        statusEl.textContent = "In Call";
        statusEl.className = "status calling";
        startCallBtn.disabled = true;
        endCallBtn.disabled = false;
        muteBtn.disabled = false;
        container.classList.add('in-call');
    } else {
        statusEl.textContent = "Disconnected";
        statusEl.className = "status disconnected";
        startCallBtn.disabled = true;
        endCallBtn.disabled = true;
        muteBtn.disabled = true;
        container.classList.remove('in-call');
        localIp = null;
    }

    muteBtn.textContent = isMuted ? "🔈 Unmute" : "🔇 Mute";
    muteBtn.classList.toggle('muted', isMuted);
    if (isLocalAudioActive && bars) {
        bars.forEach(bar => {
            bar.style.backgroundColor = isMuted ? '#72767D' : '#5865F2';
        });
    }

    renderPeersList();
}

function connect() {
    if (socket && socket.readyState === WebSocket.OPEN) return;
    setLoading(connectBtn, true);

    const protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
    socket = new WebSocket(`${protocol}${window.location.host}/signal`);

    socket.onopen = () => {
        isConnected = true;
        localIp = window.location.hostname;
        if (localIp === 'localhost' || localIp === '127.0.0.1') {
            localIp = '127.0.0.1 (you)';
        }
        addLog("Connected to server", "peer-event");
        setLoading(connectBtn, false);
        updateUI();
        initAudio();
    };

    socket.onmessage = (event) => {
        const msg = event.data;
        addLog(`Server: ${msg}`);

        if (msg.startsWith("PEER_LIST_UPDATE:")) {
            const ip = msg.split(":")[1];
            if (ip && ip !== "undefined" && ip !== localIp?.split(' ')[0]) {
                if (!peers.has(ip)) {
                    peers.set(ip, { muted: false, inCall: false });
                    addLog(`Peer ${ip} appeared`, "peer-event");
                }
                renderPeersList();
            }
        }

        if (msg.includes("started call") || msg.includes("ended call")) {
            const match = msg.match(/Peer ([^ ]+) (started|ended) call/);
            if (match) {
                const ip = match[1];
                const inCall = match[2] === "started";
                if (peers.has(ip)) {
                    peers.get(ip).inCall = inCall;
                } else if (ip !== localIp?.split(' ')[0]) {
                    peers.set(ip, { muted: false, inCall });
                }
                renderPeersList();
            }
        }

        if (msg.includes("is now muted") || msg.includes("is now unmuted")) {
            const match = msg.match(/Peer ([^ ]+) is now (muted|unmuted)/);
            if (match) {
                const ip = match[1];
                const muted = match[2] === "muted";
                if (peers.has(ip)) {
                    peers.get(ip).muted = muted;
                } else if (ip !== localIp?.split(' ')[0]) {
                    peers.set(ip, { muted, inCall: false });
                }
                renderPeersList();
            }
        }
    };

    socket.onclose = () => {
        isConnected = false;
        isCalling = false;
        setLoading(connectBtn, false);
        updateUI();
        stopCallTimer();
        addLog("Disconnected", "error");
        peers.clear();
    };

    socket.onerror = (err) => {
        showError("Connection failed");
        isConnected = false;
        setLoading(connectBtn, false);
        updateUI();
    };
}

function startCall() {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
        showError("Not connected. Click 'Connect' first.");
        return;
    }
    setLoading(startCallBtn, true);
    socket.send("start_call");
    isCalling = true;
    callStartTime = Date.now();
    startCallTimer();
    setTimeout(() => setLoading(startCallBtn, false), 800);
    updateUI();
}

function endCall() {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    setLoading(endCallBtn, true);
    socket.send("end_call");
    isCalling = false;
    setTimeout(() => setLoading(endCallBtn, false), 500);
    stopCallTimer();
    updateUI();
}

function toggleMute() {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
        showError("Not connected.");
        return;
    }

    isMuted = !isMuted;
    const command = isMuted ? "mute" : "unmute";
    socket.send(command);

    if (microphone) {
        const tracks = microphone.getAudioTracks();
        tracks.forEach(track => track.enabled = !isMuted);
    }

    updateUI();
}

function startCallTimer() {
    timerInterval = setInterval(() => {
        if (!callStartTime) return;
        const elapsed = Math.floor((Date.now() - callStartTime) / 1000);
        timerEl.textContent = `Call duration: ${elapsed}s`;
    }, 1000);
}

function stopCallTimer() {
    if (timerInterval) clearInterval(timerInterval);
    timerInterval = null;
    timerEl.textContent = "Call duration: 0s";
}

connectBtn.addEventListener('click', connect);
startCallBtn.addEventListener('click', startCall);
endCallBtn.addEventListener('click', endCall);
muteBtn.addEventListener('click', toggleMute);

setPeerBtn.addEventListener('click', () => {
    const ip = peerIpInput.value.trim();
    if (!ip) {
        showError("Please enter a valid IP address");
        return;
    }

    if (!socket || socket.readyState !== WebSocket.OPEN) {
        showError("Connect first before setting peer IP");
        return;
    }

    addLog(` Manual peer target set: ${ip}`, "peer-event");
    peers.set(ip, { muted: false, inCall: false });
    renderPeersList();

    const originalText = setPeerBtn.textContent;
    setPeerBtn.textContent = "✓ Set!";
    setTimeout(() => {
        setPeerBtn.textContent = originalText;
    }, 1500);
});

document.addEventListener('keydown', (e) => {
    if (e.key === 'c' && !e.ctrlKey) connect();
    if (e.key === 's' && !e.ctrlKey) startCall();
    if (e.key === 'e' && !e.ctrlKey) endCall();
    if (e.key === 'm' && !e.ctrlKey) toggleMute();
});

updateUI();
addLog("VoIP client initialized");