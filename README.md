# Fancall WebRTC Gateway

Separate Go service that bridges native WebRTC fan media to Vobiz SIP for celebrity PSTN calls.

## Architecture

```
Fan iOS App (Native WebRTC) → Gateway (WebRTC ↔ SIP) → Vobiz → Celebrity PSTN
```

## Stack

- **Go 1.22+**
- **Pion WebRTC v4** — native WebRTC implementation
- **Gorilla WebSocket** — signaling transport
- **Optional: Pion SIP or gosip** — SIP client for Vobiz bridge

## API

### Create Room (Backend → Gateway)

```bash
POST /api/v1/rooms
Content-Type: application/json

{
  "session_id": "ses_123",
  "fan_id": "fan_456",
  "celebrity_id": "cel_789",
  "booking_id": "book_000"
}
```

Response:
```json
{
  "room_id": "room_1234567890_abcdef12",
  "session_id": "ses_123",
  "token": "tk_room_123...",
  "ice_servers": [{"urls": "stun:stun.l.google.com:19302"}],
  "signaling_url": "wss://gateway.fancall.com/ws",
  "status": "created"
}
```

### WebSocket Signaling (Fan App → Gateway)

```
wss://gateway.fancall.com/ws?room_id=ROOM_ID&token=TOKEN
```

Messages:
- `join` — fan joins room
- `offer` — fan sends SDP offer
- `answer` — gateway sends SDP answer
- `ice-candidate` — ICE exchange
- `leave` — fan disconnects

### Delete Room (Cleanup)

```bash
DELETE /api/v1/rooms/:room_id
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `HTTP_PORT` | Gateway HTTP port | `8080` |
| `PUBLIC_URL` | Public URL for WebSocket | Required |
| `VOBIZ_AUTH_ID` | Vobiz auth ID | Required for SIP |
| `VOBIZ_AUTH_TOKEN` | Vobiz auth token | Required for SIP |
| `VOBIZ_BASE_URL` | Vobiz API base | Required for SIP |
| `VOBIZ_FROM_NUMBER` | Approved caller ID | Required for PSTN |
| `ICE_SERVERS` | Custom ICE server | Optional |

## Run

```bash
cd fancall-webrtc-gateway
go mod tidy
go run .
```

## Build Docker Image

```bash
docker build -t fancall-webrtc-gateway .
docker run -p 8080:8080 -e PUBLIC_URL=https://gateway.fancall.com fancall-webrtc-gateway
```

## Deployment

The gateway should be deployed as a separate service alongside the Fancall backend. It needs:

1. Public HTTPS/WSS endpoint
2. UDP port range for WebRTC ICE (10000-20000 typical)
3. TURN server for NAT traversal (coturn or Twilio TURN)

## iOS Integration

Add to `Podfile`:
```ruby
pod 'WebRTC-SDK', '~> 125.0'
```

The iOS app uses `NativeWebRTCManager` (singleton) which:
- Connects to gateway via WebSocket
- Creates native `RTCPeerConnection`
- Owns `AVAudioSession` (like Agora)
- Supports CallKit mute/route sync
- Bluetooth headset mic works natively

## Backend Integration

Set in `VobizConfig`:
```typescript
{
  gatewayUrl: 'https://gateway.fancall.com',
  gatewayApiKey: 'your-api-key'
}
```

Set feature flag:
```typescript
{
  voice_provider: 'native_webrtc',
  voice_media_mode: 'native_webrtc'
}
```

## TODO

- [ ] Implement SIP client in Go (using Pion SIP or gosip)
- [ ] Implement media bridge: WebRTC RTP ↔ SIP RTP
- [ ] Add TURN server configuration
- [ ] Add room auto-cleanup (TTL)
- [ ] Add metrics and health checks
- [ ] Load test with multiple concurrent rooms
