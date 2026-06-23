package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

// SignalingMessage represents a WebSocket signaling message
type SignalingMessage struct {
	Type      string                     `json:"type"` // offer, answer, ice-candidate, join, leave, error
	RoomID    string                     `json:"room_id"`
	Token     string                     `json:"token"`
	SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
	Error     string                     `json:"error,omitempty"`
}

// UnmarshalJSON handles both flat SDP string formats and nested SDP object formats
func (s *SignalingMessage) UnmarshalJSON(data []byte) error {
	type Alias SignalingMessage
	var standard struct {
		Alias
		SDP any `json:"sdp,omitempty"`
	}

	if err := json.Unmarshal(data, &standard); err != nil {
		return err
	}

	s.Type = standard.Type
	s.RoomID = standard.RoomID
	s.Token = standard.Token
	s.Candidate = standard.Candidate
	s.Error = standard.Error

	if standard.SDP != nil {
		switch v := standard.SDP.(type) {
		case string:
			// Flat string format: "sdp": "v=0\n..."
			var sdpType webrtc.SDPType
			switch s.Type {
			case "offer":
				sdpType = webrtc.SDPTypeOffer
			case "answer":
				sdpType = webrtc.SDPTypeAnswer
			case "pranswer":
				sdpType = webrtc.SDPTypePranswer
			case "rollback":
				sdpType = webrtc.SDPTypeRollback
			}
			s.SDP = &webrtc.SessionDescription{
				Type: sdpType,
				SDP:  v,
			}
		case map[string]any:
			// Nested object format: "sdp": { "type": "offer", "sdp": "v=0\n..." }
			sdpBytes, err := json.Marshal(v)
			if err != nil {
				return err
			}
			var sdp webrtc.SessionDescription
			if err := json.Unmarshal(sdpBytes, &sdp); err != nil {
				return err
			}
			s.SDP = &sdp
		}
	}

	return nil
}

// MarshalJSON marshals the signaling message, flat-mapping the SDP to be compatible with JS WebRTC client
func (s SignalingMessage) MarshalJSON() ([]byte, error) {
	// If it is an offer or answer, we output a flat structure where "sdp" is the raw string
	// to make it directly compatible with new RTCSessionDescription(data) in the JS client
	if s.SDP != nil && (s.Type == "offer" || s.Type == "answer") {
		type FlatMessage struct {
			Type      string                   `json:"type"`
			RoomID    string                   `json:"room_id,omitempty"`
			Token     string                   `json:"token,omitempty"`
			SDP       string                   `json:"sdp,omitempty"`
			Candidate *webrtc.ICECandidateInit `json:"candidate,omitempty"`
			Error     string                   `json:"error,omitempty"`
		}
		return json.Marshal(FlatMessage{
			Type:      s.Type,
			RoomID:    s.RoomID,
			Token:     s.Token,
			SDP:       s.SDP.SDP, // output the raw string directly at the top level
			Candidate: s.Candidate,
			Error:     s.Error,
		})
	}

	// Default fallback
	type Alias SignalingMessage
	return json.Marshal(Alias(s))
}

// HandleWebSocket upgrades HTTP to WebSocket and handles signaling
func handleWebSocket(w http.ResponseWriter, r *http.Request, upgrader websocket.Upgrader, rm *RoomManager) {
	// Validate token from query param
	token := r.URL.Query().Get("token")
	roomID := r.URL.Query().Get("room_id")
	if roomID == "" {
		roomID = r.URL.Query().Get("roomId")
	}

	if token == "" || roomID == "" {
		http.Error(w, `{"error":"token and room_id/roomId required"}`, http.StatusBadRequest)
		return
	}

	room, ok := rm.GetRoom(roomID)
	if !ok {
		http.Error(w, `{"error":"room not found"}`, http.StatusNotFound)
		return
	}

	// Upgrade to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WebSocket] Upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("[WebSocket] Fan connected to room %s", roomID)

	// Start goroutine to send messages to client
	go func() {
		for msg := range room.SignalingChan {
			if err := conn.WriteJSON(msg); err != nil {
				log.Printf("[WebSocket] Write error: %v", err)
				return
			}
		}
	}()

	// Handle incoming messages
	for {
		var msg SignalingMessage
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("[WebSocket] Read error: %v", err)
			break
		}

		msg.RoomID = roomID
		msg.Token = token

		if err := handleSignalingMessage(room, &msg); err != nil {
			log.Printf("[WebSocket] Handle error: %v", err)
			room.SendSignalingMessage(SignalingMessage{
				Type:  "error",
				Error: err.Error(),
			})
		}
	}

	log.Printf("[WebSocket] Fan disconnected from room %s", roomID)
}

// HandleSignalingMessage processes a signaling message
func handleSignalingMessage(room *Room, msg *SignalingMessage) error {
	switch msg.Type {
	case "join":
		return handleJoin(room, msg)
	case "offer":
		return handleOffer(room, msg)
	case "answer":
		return handleAnswer(room, msg)
	case "ice-candidate":
		return handleICECandidate(room, msg)
	case "leave":
		return handleLeave(room, msg)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// HandleJoin processes a join request
func handleJoin(room *Room, msg *SignalingMessage) error {
	if room.GetStatus() != RoomStatusCreated {
		return fmt.Errorf("room is not in created state")
	}

	room.SetStatus(RoomStatusSignaling)

	// Notify client that join was successful
	room.SendSignalingMessage(SignalingMessage{
		Type:   "joined",
		RoomID: room.ID,
	})

	return nil
}

// HandleOffer processes an SDP offer from the fan
func handleOffer(room *Room, msg *SignalingMessage) error {
	if msg.SDP == nil {
		return fmt.Errorf("SDP offer is nil")
	}

	room.SetStatus(RoomStatusConnecting)

	// Create peer connection config
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlan,
	}

	// Create peer connection
	api := room.GetWebRTCAPI()
	var pc *webrtc.PeerConnection
	var err error
	if api != nil {
		pc, err = api.NewPeerConnection(config)
	} else {
		pc, err = webrtc.NewPeerConnection(config)
	}
	if err != nil {
		return fmt.Errorf("failed to create peer connection: %w", err)
	}

	// Create local audio track (PCMU for G.711 / Vobiz compatibility)
	localTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU},
		"audio",
		"pion",
	)
	if err != nil {
		pc.Close()
		return fmt.Errorf("failed to create local audio track: %w", err)
	}

	// Add track to PeerConnection to send audio to fan
	_, err = pc.AddTrack(localTrack)
	if err != nil {
		pc.Close()
		return fmt.Errorf("failed to add local track to peer connection: %w", err)
	}

	room.mu.Lock()
	room.PeerConnection = pc
	room.LocalTrack = localTrack
	room.mu.Unlock()

	// Set up track handlers
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("[WebRTC] Track received: %s (%s). Codec: %s", track.ID(), track.Kind().String(), track.Codec().MimeType)

		room.mu.Lock()
		room.RemoteTrack = track
		room.mu.Unlock()

		// If this is audio, bridge it to SIP
		if track.Kind() == webrtc.RTPCodecTypeAudio {
			go bridgeAudioToSIP(room, track)
		}
	})

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("[WebRTC] ICE state changed: %s", state.String())

		switch state {
		case webrtc.ICEConnectionStateConnected:
			room.SetStatus(RoomStatusConnected)
			room.SendSignalingMessage(SignalingMessage{
				Type:   "connected",
				RoomID: room.ID,
			})
		case webrtc.ICEConnectionStateFailed, webrtc.ICEConnectionStateClosed:
			room.SetStatus(RoomStatusFailed)
			room.SendSignalingMessage(SignalingMessage{
				Type:   "disconnected",
				RoomID: room.ID,
			})
		}
	})

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}

		init := candidate.ToJSON()
		room.SendSignalingMessage(SignalingMessage{
			Type:      "ice-candidate",
			RoomID:    room.ID,
			Candidate: &init,
		})
	})

	// Set remote description (the offer)
	if err := pc.SetRemoteDescription(*msg.SDP); err != nil {
		return fmt.Errorf("failed to set remote description: %w", err)
	}

	// Create answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("failed to create answer: %w", err)
	}

	// Set local description
	if err := pc.SetLocalDescription(answer); err != nil {
		return fmt.Errorf("failed to set local description: %w", err)
	}

	// Send answer to client
	room.SendSignalingMessage(SignalingMessage{
		Type:   "answer",
		RoomID: room.ID,
		SDP:    &answer,
	})

	return nil
}

// HandleAnswer processes an SDP answer (not typically used in fan->gateway flow)
func handleAnswer(room *Room, msg *SignalingMessage) error {
	if msg.SDP == nil {
		return fmt.Errorf("SDP answer is nil")
	}

	room.mu.Lock()
	pc := room.PeerConnection
	room.mu.Unlock()

	if pc == nil {
		return fmt.Errorf("no peer connection exists")
	}

	if err := pc.SetRemoteDescription(*msg.SDP); err != nil {
		return fmt.Errorf("failed to set remote description: %w", err)
	}

	return nil
}

// HandleICECandidate processes an ICE candidate from the fan
func handleICECandidate(room *Room, msg *SignalingMessage) error {
	if msg.Candidate == nil {
		return fmt.Errorf("ICE candidate is nil")
	}

	room.mu.Lock()
	pc := room.PeerConnection
	room.mu.Unlock()

	if pc == nil {
		return fmt.Errorf("no peer connection exists")
	}

	if err := pc.AddICECandidate(*msg.Candidate); err != nil {
		return fmt.Errorf("failed to add ICE candidate: %w", err)
	}

	return nil
}

// HandleLeave processes a leave request
func handleLeave(room *Room, msg *SignalingMessage) error {
	room.SetStatus(RoomStatusEnding)
	room.Close()
	room.SetStatus(RoomStatusEnded)

	room.SendSignalingMessage(SignalingMessage{
		Type:   "left",
		RoomID: room.ID,
	})

	return nil
}

// BridgeAudioToSIP bridges WebRTC audio track to SIP and vice-versa
func bridgeAudioToSIP(room *Room, track *webrtc.TrackRemote) {
	log.Printf("[Bridge] Starting audio bridge for room %s", room.ID)

	// Get SIP bridge
	sipBridge := room.GetSIPBridge()
	if sipBridge == nil {
		log.Printf("[Bridge] No SIP bridge available for room %s", room.ID)
		return
	}

	// Read RTP packets from WebRTC and forward to SIP
	go func() {
		var packetCount int
		for {
			if room.GetStatus() == RoomStatusEnded || room.GetStatus() == RoomStatusFailed {
				break
			}

			rtpPacket, _, err := track.ReadRTP()
			if err != nil {
				log.Printf("[Bridge] Read RTP error: %v", err)
				break
			}

			packetCount++
			if packetCount%100 == 0 {
				sipBridge.mu.RLock()
				remoteIP := sipBridge.remoteRtpIP
				remotePort := sipBridge.remoteRtpPort
				sipBridge.mu.RUnlock()
				log.Printf("[Bridge] WebRTC -> SIP: Forwarded %d RTP packets to %s:%d. Original PT=%d, SSRC=%d, TS=%d", 
					packetCount, remoteIP, remotePort, rtpPacket.PayloadType, rtpPacket.SSRC, rtpPacket.Timestamp)
			}

			// Explicitly normalize payload type to 0 (PCMU) for Vobiz compatibility
			rtpPacket.PayloadType = 0

			// Serialize RTP packet
			buf, err := rtpPacket.Marshal()
			if err != nil {
				continue
			}

			sipBridge.WriteRTP(buf)
		}
		log.Printf("[Bridge] WebRTC->SIP bridge ended for room %s", room.ID)
	}()

	// Read RTP packets from SIP and forward to WebRTC
	go func() {
		localTrack := room.GetLocalTrack()
		if localTrack == nil {
			log.Printf("[Bridge] No local WebRTC track available for room %s", room.ID)
			return
		}

		var packetCount int
		for {
			if room.GetStatus() == RoomStatusEnded || room.GetStatus() == RoomStatusFailed {
				break
			}

			packet, ok := sipBridge.ReadRTP()
			if !ok {
				continue
			}

			// Ensure the packet size is at least the size of standard RTP header (12 bytes)
			if len(packet) < 12 {
				continue
			}

			packetCount++
			if packetCount%100 == 0 {
				log.Printf("[Bridge] SIP -> WebRTC: Forwarded %d RTP packets to WebRTC localTrack. Source length=%d", packetCount, len(packet))
			}

			// Normalize the Payload Type of the outgoing WebRTC packet to PCMU (0)
			packet[1] = (packet[1] & 0x80) | 0

			// Forward packet directly to WebRTC
			_, err := localTrack.Write(packet)
			if err != nil {
				log.Printf("[Bridge] Write RTP to WebRTC error: %v", err)
			}
		}
		log.Printf("[Bridge] SIP->WebRTC bridge ended for room %s", room.ID)
	}()

	log.Printf("[Bridge] Audio bridge started for room %s", room.ID)
}

// ExtractRoomIDFromPath extracts room ID from URL path
func extractRoomIDFromPath(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}
