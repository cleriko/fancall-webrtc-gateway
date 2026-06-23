package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

// Room represents a fan-celebrity call session
type Room struct {
	ID             string                      `json:"id"`
	SessionID      string                      `json:"session_id"`
	FanID          string                      `json:"fan_id"`
	CelebrityID    string                      `json:"celebrity_id"`
	Status         RoomStatus                  `json:"status"`
	CreatedAt      time.Time                   `json:"created_at"`
	UpdatedAt      time.Time                   `json:"updated_at"`
	PeerConnection *webrtc.PeerConnection      `json:"-"`
	SignalingChan  chan SignalingMessage       `json:"-"`
	SIPBridge      *SIPBridge                  `json:"-"`
	LocalTrack     *webrtc.TrackLocalStaticRTP `json:"-"`
	RemoteTrack    *webrtc.TrackRemote         `json:"-"`
	webrtcAPI      *webrtc.API                 `json:"-"`
	mu             sync.RWMutex
}

// RoomStatus represents the room state
type RoomStatus string

const (
	RoomStatusCreated    RoomStatus = "created"
	RoomStatusSignaling  RoomStatus = "signaling"
	RoomStatusConnecting RoomStatus = "connecting"
	RoomStatusConnected  RoomStatus = "connected"
	RoomStatusEnding     RoomStatus = "ending"
	RoomStatusEnded      RoomStatus = "ended"
	RoomStatusFailed     RoomStatus = "failed"
)

// CreateRoomRequest is sent by the backend to create a room
type CreateRoomRequest struct {
	SessionID   string `json:"session_id"`
	FanID       string `json:"fan_id"`
	CelebrityID string `json:"celebrity_id"`
	BookingID   string `json:"booking_id,omitempty"`
}

// CreateRoomResponse is returned after room creation
type CreateRoomResponse struct {
	RoomID       string             `json:"room_id"`
	SessionID    string             `json:"session_id"`
	Token        string             `json:"token"`
	ICEServers   []webrtc.ICEServer `json:"ice_servers"`
	SignalingURL string             `json:"signaling_url"`
	Status       RoomStatus         `json:"status"`
}

// RoomManager manages all active rooms
type RoomManager struct {
	cfg       *Config
	rooms     map[string]*Room // room_id -> Room
	mu        sync.RWMutex
	webrtcAPI *webrtc.API // Share WebRTC SettingEngine with UDPMux
}

// NewRoomManager creates a new room manager
func NewRoomManager(cfg *Config) *RoomManager {
	settingEngine := webrtc.SettingEngine{}
	
	// Create global ICE UDP Mux on port 50000
	mux, err := ice.NewMultiUDPMuxFromPort(50000)
	if err != nil {
		log.Printf("[RoomManager] Warning: Failed to create ICE UDP Mux on port 50000: %v. Ephemeral ports will be used.", err)
	} else {
		log.Printf("[RoomManager] Shared WebRTC ICE UDP Mux started on port 50000")
		settingEngine.SetICEUDPMux(mux)
	}

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	return &RoomManager{
		cfg:       cfg,
		rooms:     make(map[string]*Room),
		webrtcAPI: api,
	}
}

// CreateRoom creates a new room for a fan-celebrity call
func (rm *RoomManager) CreateRoom(req CreateRoomRequest) (*CreateRoomResponse, error) {
	roomID := generateRoomID()
	token := generateToken(roomID, req.SessionID)

	room := &Room{
		ID:            roomID,
		SessionID:     req.SessionID,
		FanID:         req.FanID,
		CelebrityID:   req.CelebrityID,
		Status:        RoomStatusCreated,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
		SignalingChan: make(chan SignalingMessage, 100),
		webrtcAPI:     rm.webrtcAPI,
	}

	// Resolve PublicURL to get the public IP address for SIP routing
	publicIP := "0.0.0.0"
	if rm.cfg.PublicURL != "" {
		ips, err := net.LookupIP(rm.cfg.PublicURL)
		if err == nil && len(ips) > 0 {
			publicIP = ips[0].String()
			log.Printf("[RoomManager] Resolved PublicURL %s to IP: %s", rm.cfg.PublicURL, publicIP)
		} else {
			log.Printf("[RoomManager] Warning: Failed to resolve PublicURL %s to IP, falling back to 0.0.0.0", rm.cfg.PublicURL)
		}
	}

	// Create SIP bridge for Vobiz integration
	// The SIP bridge will listen for incoming calls from Vobiz
	sipConfig := SIPConfig{
		LocalIP:     "0.0.0.0",
		LocalPort:   0, // Let system assign port
		PublicIP:    publicIP,
		Username:    fmt.Sprintf("fan_%s", req.FanID),
		Password:    generateRandomString(16),
		Domain:      rm.cfg.SIPDomain,
		DisplayName: "Fancall Fan",
	}

	sipBridge, err := NewSIPBridge(sipConfig, room)
	if err != nil {
		log.Printf("[RoomManager] Failed to create SIP bridge: %v", err)
		// Non-fatal — room can still work for testing
	} else {
		room.SIPBridge = sipBridge
		if err := sipBridge.Start(); err != nil {
			log.Printf("[RoomManager] Failed to start SIP bridge: %v", err)
		}
	}

	rm.mu.Lock()
	rm.rooms[roomID] = room
	rm.mu.Unlock()

	// Auto-cleanup after max call duration
	go rm.scheduleRoomCleanup(roomID, rm.cfg.MaxCallDuration)

	log.Printf("[RoomManager] Created room %s for session %s", roomID, req.SessionID)

	return &CreateRoomResponse{
		RoomID:       roomID,
		SessionID:    req.SessionID,
		Token:        token,
		ICEServers:   rm.cfg.ICEServers,
		SignalingURL: fmt.Sprintf("wss://%s%s", rm.cfg.PublicURL, rm.cfg.WebSocketPath),
		Status:       RoomStatusCreated,
	}, nil
}

// GetRoom retrieves a room by ID
func (rm *RoomManager) GetRoom(roomID string) (*Room, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	room, ok := rm.rooms[roomID]
	return room, ok
}

// DeleteRoom removes a room and cleans up resources
func (rm *RoomManager) DeleteRoom(roomID string) error {
	rm.mu.Lock()
	room, ok := rm.rooms[roomID]
	if !ok {
		rm.mu.Unlock()
		return fmt.Errorf("room not found: %s", roomID)
	}
	delete(rm.rooms, roomID)
	rm.mu.Unlock()

	room.Close()
	log.Printf("[RoomManager] Deleted room %s", roomID)
	return nil
}

// scheduleRoomCleanup auto-deletes room after duration
func (rm *RoomManager) scheduleRoomCleanup(roomID string, duration time.Duration) {
	if duration <= 0 {
		duration = 1 * time.Hour
	}

	time.Sleep(duration)

	rm.mu.RLock()
	_, exists := rm.rooms[roomID]
	rm.mu.RUnlock()

	if exists {
		log.Printf("[RoomManager] Auto-cleaning room %s after %v", roomID, duration)
		rm.DeleteRoom(roomID)
	}
}

// Close cleans up room resources
func (r *Room) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.PeerConnection != nil {
		r.PeerConnection.Close()
		r.PeerConnection = nil
	}

	if r.SIPBridge != nil {
		r.SIPBridge.Stop()
		r.SIPBridge = nil
	}

	close(r.SignalingChan)
}

// SetStatus updates room status thread-safely
func (r *Room) SetStatus(status RoomStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Status = status
	r.UpdatedAt = time.Now()
}

// GetStatus returns current room status
func (r *Room) GetStatus() RoomStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Status
}

// GetSIPBridge returns the SIP bridge safely
func (r *Room) GetSIPBridge() *SIPBridge {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.SIPBridge
}

// GetLocalTrack returns the local audio track safely
func (r *Room) GetLocalTrack() *webrtc.TrackLocalStaticRTP {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.LocalTrack
}

// GetWebRTCAPI returns the webrtc API safely
func (r *Room) GetWebRTCAPI() *webrtc.API {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.webrtcAPI
}

// HandleCreateRoom handles HTTP POST /api/v1/rooms
func handleCreateRoom(w http.ResponseWriter, r *http.Request, rm *RoomManager) {
	var req CreateRoomRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid request: %v"}`, err), http.StatusBadRequest)
		return
	}

	if req.SessionID == "" || req.FanID == "" || req.CelebrityID == "" {
		http.Error(w, `{"error":"missing required fields"}`, http.StatusBadRequest)
		return
	}

	resp, err := rm.CreateRoom(req)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

// HandleGetRoom handles HTTP GET /api/v1/rooms/:id
func handleGetRoom(w http.ResponseWriter, r *http.Request, rm *RoomManager) {
	roomID := strings.TrimPrefix(r.URL.Path, "/api/v1/rooms/")
	if roomID == "" {
		http.Error(w, `{"error":"room ID required"}`, http.StatusBadRequest)
		return
	}

	room, ok := rm.GetRoom(roomID)
	if !ok {
		http.Error(w, `{"error":"room not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"room_id":    room.ID,
		"session_id": room.SessionID,
		"status":     room.GetStatus(),
		"created_at": room.CreatedAt,
		"updated_at": room.UpdatedAt,
	})
}

// HandleDeleteRoom handles HTTP DELETE /api/v1/rooms/:id
func handleDeleteRoom(w http.ResponseWriter, r *http.Request, rm *RoomManager) {
	roomID := strings.TrimPrefix(r.URL.Path, "/api/v1/rooms/")
	if roomID == "" {
		http.Error(w, `{"error":"room ID required"}`, http.StatusBadRequest)
		return
	}

	if err := rm.DeleteRoom(roomID); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// generateRoomID creates a unique room ID
func generateRoomID() string {
	return fmt.Sprintf("room_%d_%s", time.Now().Unix(), generateRandomString(8))
}

// generateToken creates a secure token for room access
func generateToken(roomID, sessionID string) string {
	return fmt.Sprintf("tk_%s_%s_%s", roomID, sessionID, generateRandomString(16))
}

// generateRandomString generates a random alphanumeric string
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}
