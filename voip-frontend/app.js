// VoIP Frontend Application
// Handles WebSocket signaling for VoIP calls

class VoIPApp {
    constructor() {
        this.ws = null;
        this.status = 'Idle'; // Idle, Calling, In Call
        this.targetIp = '';

        // DOM elements
        this.statusEl = document.getElementById('status');
        this.targetIpEl = document.getElementById('targetIp');
        this.callBtn = document.getElementById('callBtn');
        this.endCallBtn = document.getElementById('endCallBtn');
        this.incomingModal = document.getElementById('incomingModal');
        this.acceptBtn = document.getElementById('acceptBtn');
        this.rejectBtn = document.getElementById('rejectBtn');

        this.init();
    }

    init() {
        this.connectWebSocket();
        this.bindEvents();
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

    handleMessage(message) {
        console.log('Received:', message);
        if (message === 'pinging') {
            this.showIncomingCall();
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

        this.sendMessage(`pinging ${this.targetIp}`);
        this.setStatus('Calling');
    }

    endCall() {
        this.sendMessage('end_call');
        this.setStatus('Idle');
    }

    acceptCall() {
        this.sendMessage('start_call');
        this.hideIncomingCall();
        this.setStatus('In Call');
    }

    rejectCall() {
        this.hideIncomingCall();
        this.setStatus('Idle');
    }

    showIncomingCall() {
        this.incomingModal.style.display = 'flex';
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