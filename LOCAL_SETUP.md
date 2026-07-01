# Local Setup Guide for Fancall WebRTC Gateway

This guide covers setting up the Fancall WebRTC Gateway (`fancall-webrtc-gateway`) locally. This gateway is written in Go and acts as the bridge translating WebRTC audio from the iOS fan app to SIP audio for the Vobiz celebrity connection.

## Prerequisites

Ensure you have the following installed on your local machine:
- **Go**: Version 1.22 or higher.
- **Docker**: (Optional) if you prefer to run it containerized.
- **Git**: For cloning the repository.

## 1. Clone the Repository

Clone the gateway repository (if not already done) and navigate into the folder:

```bash
git clone <repository_url> fancall-webrtc-gateway
cd fancall-webrtc-gateway
```

## 2. Install Go Dependencies

Run the following command to download all necessary Go modules (Pion WebRTC, Gorilla WebSocket, GoSIP, etc.):

```bash
go mod tidy
```

## 3. Environment Variables (.env)

Create a `.env` file at the root of the `fancall-webrtc-gateway` directory to store your configuration.

```bash
touch .env
```

Add the following essential variables:

```env
# Server Configuration
PORT=8080
GATEWAY_API_KEY=fancall-webrtc-secret-2026
PUBLIC_URL=http://localhost:8080

# ICE Servers (For local testing, standard Google STUN is fine)
ICE_SERVERS=[{"urls": "stun:stun.l.google.com:19302"}]

# Vobiz Configuration (Required for bridging)
VOBIZ_AUTH_ID=your_vobiz_auth_id
VOBIZ_AUTH_TOKEN=your_vobiz_auth_token
VOBIZ_BASE_URL=https://api.vobiz.ai
VOBIZ_SIP_DOMAIN=registrar.vobiz.ai

# Vobiz Endpoint Settings (These must match the backend .env)
VOBIZ_SIP_ENDPOINT_APPLICATION_ID=your_app_id
```

## 4. Running the Gateway Locally

You can run the Go application directly:

```bash
go run .
```

You should see logs indicating the server has started successfully:
```
2026/06/24 10:00:00 [Gateway] Starting on :8080
2026/06/24 10:00:00 [Gateway] SIP User Agent started on :5060
```

## 5. Network Configuration & Ports

WebRTC requires UDP ports to transmit media (audio). When running locally, Pion WebRTC will dynamically allocate UDP ports. 

If you are using Docker, you **must** run the container in host network mode or map a wide range of UDP ports, otherwise the audio will not bridge.

### Running via Docker (Local)
```bash
docker build -t fancall-webrtc-gateway .

# Option A: Host networking (Easiest for local audio testing)
docker run --network host --env-file .env fancall-webrtc-gateway

# Option B: Port mapping (If host networking isn't available)
docker run -p 8080:8080 -p 5060:5060/udp -p 10000-10050:10000-10050/udp --env-file .env fancall-webrtc-gateway
```

## 6. Testing with the Backend (Ngrok)

The Fancall Node.js backend needs to talk to this Go Gateway to create rooms.

If both are running on your local machine:
1. Ensure the Node.js backend `.env` has:
   ```env
   VOBIZ_GATEWAY_URL=http://localhost:8080
   VOBIZ_GATEWAY_API_KEY=fancall-webrtc-secret-2026
   ```

2. If testing from an actual mobile device, `localhost` won't work for the WebSocket URL. You must expose the Gateway via Ngrok:
   ```bash
   ngrok http 8080
   ```
   Then update the `PUBLIC_URL` in the Gateway's `.env`:
   ```env
   PUBLIC_URL=https://<your-ngrok-url>.ngrok.io
   ```
   And update the Node.js backend `.env`:
   ```env
   VOBIZ_GATEWAY_URL=https://<your-ngrok-url>.ngrok.io
   ```

Restart both servers if you change `.env` variables.

## Important Note on Local Audio Testing

If you are running the gateway locally and testing with an iOS device on the same local WiFi network, audio should connect using mDNS/local IP candidates. 

However, if testing across cellular networks or different WiFis, you will need a TURN server (like Coturn) configured in the `ICE_SERVERS` environment variable, otherwise the WebRTC connection will fail to establish an audio stream.