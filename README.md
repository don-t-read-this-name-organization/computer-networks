# Simple VoIP Network Setup

This project implements a basic Voice over IP (VoIP) system using Rust for the backend and HTML/JavaScript for the frontend.

## Features

- Real-time audio capture and playback
- UDP-based audio transmission
- WebSocket signaling for call setup
- Simple jitter buffer for audio synchronization
- Web-based user interface

## Architecture

- **Backend (Rust)**: Handles audio I/O, networking, and web serving
- **Frontend (HTML/JS)**: Provides UI for initiating and managing calls

## Requirements

- Rust (latest stable)
- Microphone and speakers/headphones
- Two computers on the same network (or with direct IP connectivity)

## Running the Application

1. **Build and run the backend**:
   ```bash
   cd voip-backend
   cargo run
   ```

2. **Open the frontend**:
   - Open a web browser and navigate to `http://localhost:3000`
   - Allow microphone access when prompted

3. **Make a call**:
   - Enter the target IP address of the other computer
   - Click "Call"
   - On the receiving end, accept the incoming call

## How It Works

- Audio is captured from the microphone in real-time
- Audio data is packetized and sent via UDP to the target IP on port 40000
- Received audio is buffered to handle network jitter
- WebSocket connections handle call signaling (ping, start, end)

## Limitations

- No audio compression (uses raw PCM, high bandwidth)
- No NAT traversal (requires direct IP connectivity)
- Basic jitter buffer (may have latency issues on poor networks)
- No security (no encryption)

## Ports Used

- 3000: HTTP/WebSocket server
- 40000: UDP audio transmission