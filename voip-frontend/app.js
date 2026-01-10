// VoIP Frontend Application
// Handles WebSocket signaling for VoIP calls

class VoIPApp {
    constructor() {
        this.ws = null;
        this.status = 'Idle'; // Idle, Calling, In Call
        this.targetIp = '';

        this.audioContext = null;
        this.analyser = null;
        this.localVolume = 0;
        this.receivedVolume = 0;

        // WebRTC
        this.peerConnection = null;
        this.localStream = null;

        // DOM elements
        this.statusEl = document.getElementById('status');
        this.targetIpEl = document.getElementById('targetIp');
        this.callBtn = document.getElementById('callBtn');
        this.endCallBtn = document.getElementById('endCallBtn');
        this.incomingModal = document.getElementById('incomingModal');
        this.acceptBtn = document.getElementById('acceptBtn');
        this.rejectBtn = document.getElementById('rejectBtn');
        this.localCanvas = document.getElementById('localCanvas');
        this.receivedCanvas = document.getElementById('receivedCanvas');

        this.init();
    }

    init() {
        this.checkMic();
        this.connectWebSocket();
        this.bindEvents();
        this.initWebRTC();
    }

    async checkMic() {
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
            stream.getTracks().forEach(track => track.stop());
            document.getElementById('micStatus').textContent = '🎤';
        } catch (error) {
            console.error('Microphone access denied:', error);
            document.getElementById('micStatus').textContent = '🚫';
        }
    }

    connectWebSocket() {
        this.ws = new WebSocket('ws://localhost:3000/signal');

        this.ws.onopen = () => {
            console.log('WebSocket connected');
        };

        this.ws.onmessage = (event) => {
            this.handleMessage(event.data);
        };

        this.ws.onclose = () => {
            console.log('WebSocket closed');
            this.setStatus('Idle');
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    bindEvents() {
        this.callBtn.addEventListener('click', () => this.startCall());
        this.endCallBtn.addEventListener('click', () => this.endCall());
        this.acceptBtn.addEventListener('click', () => this.acceptCall());
        this.rejectBtn.addEventListener('click', () => this.rejectCall());
    }

    initWebRTC() {
        this.peerConnection = new RTCPeerConnection({
            iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
        });

        this.peerConnection.onicecandidate = (event) => {
            if (event.candidate) {
                this.sendMessage('ice ' + JSON.stringify(event.candidate));
            }
        };

        this.peerConnection.ontrack = (event) => {
            const remoteAudio = new Audio();
            remoteAudio.srcObject = event.streams[0];
            remoteAudio.play();
        };

        this.peerConnection.onconnectionstatechange = () => {
            if (this.peerConnection.connectionState === 'connected') {
                this.setStatus('In Call');
            }
        };
    }

    handleMessage(message) {
        console.log('Received:', message);
        if (message.startsWith('offer ')) {
            const offer = JSON.parse(message.substring(6));
            this.handleOffer(offer);
        } else if (message.startsWith('answer ')) {
            const answer = JSON.parse(message.substring(7));
            this.handleAnswer(answer);
        } else if (message.startsWith('ice ')) {
            const candidate = JSON.parse(message.substring(4));
            this.handleIceCandidate(candidate);
        } else if (message === 'call_ended') {
            this.setStatus('Idle');
            if (this.peerConnection) {
                this.peerConnection.close();
                this.peerConnection = null;
            }
            if (this.localStream) {
                this.localStream.getTracks().forEach(track => track.stop());
                this.localStream = null;
            }
        }
        // Other messages can be handled here if needed
    }

    startCall() {
        this.targetIp = this.targetIpEl.value.trim();
        if (!this.targetIp) {
            alert('Please enter a target IP address');
            return;
        }
        if (this.status !== 'Idle') return;

        this.setStatus('Calling');
        this.createOffer();
    }

    endCall() {
        if (this.peerConnection) {
            this.peerConnection.close();
            this.peerConnection = null;
        }
        if (this.localStream) {
            this.localStream.getTracks().forEach(track => track.stop());
            this.localStream = null;
        }
        this.sendMessage('end_call');
        this.setStatus('Idle');
    }

    async createOffer() {
        try {
            this.localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
            this.localStream.getTracks().forEach(track => this.peerConnection.addTrack(track, this.localStream));
            this.setupAudioVisualizer(this.localStream);

            const offer = await this.peerConnection.createOffer();
            await this.peerConnection.setLocalDescription(offer);
            this.sendMessage('offer ' + JSON.stringify(offer));
        } catch (error) {
            console.error('Error creating offer:', error);
            this.setStatus('Idle');
        }
    }

    async handleOffer(offer) {
        try {
            await this.peerConnection.setRemoteDescription(new RTCSessionDescription(offer));
            this.showIncomingCall();
        } catch (error) {
            console.error('Error handling offer:', error);
        }
    }

    async handleAnswer(answer) {
        try {
            await this.peerConnection.setRemoteDescription(new RTCSessionDescription(answer));
        } catch (error) {
            console.error('Error handling answer:', error);
        }
    }

    async handleIceCandidate(candidate) {
        try {
            await this.peerConnection.addIceCandidate(new RTCIceCandidate(candidate));
        } catch (error) {
            console.error('Error handling ICE candidate:', error);
        }
    }

    async acceptCall() {
        try {
            this.localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
            this.localStream.getTracks().forEach(track => this.peerConnection.addTrack(track, this.localStream));
            this.setupAudioVisualizer(this.localStream);

            const answer = await this.peerConnection.createAnswer();
            await this.peerConnection.setLocalDescription(answer);
            this.sendMessage('answer ' + JSON.stringify(answer));
            this.hideIncomingCall();
            this.setStatus('In Call');
        } catch (error) {
            console.error('Error accepting call:', error);
            this.setStatus('Idle');
        }
    }

    rejectCall() {
        this.hideIncomingCall();
        this.setStatus('Idle');
    }

    hideIncomingCall() {
        this.incomingModal.style.display = 'none';
    }

    sendMessage(message) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(message);
            console.log('Sent:', message);
        } else {
            console.error('WebSocket not connected');
        }
    }

    setupAudioVisualizer(stream) {
        this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
        this.analyser = this.audioContext.createAnalyser();
        this.analyser.fftSize = 256;
        const source = this.audioContext.createMediaStreamSource(stream);
        source.connect(this.analyser);
        this.drawLocal();
    }

    drawLocal() {
        if (!this.analyser) return;
        const bufferLength = this.analyser.frequencyBinCount;
        const dataArray = new Uint8Array(bufferLength);
        this.analyser.getByteFrequencyData(dataArray);
        const avg = dataArray.reduce((a, b) => a + b) / bufferLength;
        this.localVolume = avg / 255;
        this.drawBar(this.localCanvas, this.localVolume);
        requestAnimationFrame(() => this.drawLocal());
    }

    drawBar(canvas, volume) {
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.fillStyle = '#4CAF50';
        ctx.fillRect(0, 0, canvas.width * volume, canvas.height);
    }

    setStatus(status) {
        this.status = status;
        this.statusEl.textContent = status;

        // Update button states
        if (status === 'Idle') {
            this.callBtn.disabled = false;
            this.endCallBtn.disabled = true;
        } else if (status === 'Calling') {
            this.callBtn.disabled = true;
            this.endCallBtn.disabled = false;
        } else if (status === 'In Call') {
            this.callBtn.disabled = true;
            this.endCallBtn.disabled = false;
        }
    }
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new VoIPApp();
});