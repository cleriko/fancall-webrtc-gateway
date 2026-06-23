package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Minimal UDP SIP implementation for Vobiz integration
// ---------------------------------------------------------------------------

// SIPBridge handles SIP signaling for bridging WebRTC to Vobiz PSTN
type SIPBridge struct {
	config SIPConfig
	room   *Room

	mu         sync.RWMutex
	callID     string
	fromTag    string
	toTag      string
	cseq       int
	connected  bool
	registered bool

	// UDP socket for SIP
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr

	// Media channels
	rtpIn  chan []byte // WebRTC -> SIP
	rtpOut chan []byte // SIP -> WebRTC

	// RTP socket
	rtpConn *net.UDPConn
	rtpPort int

	// Remote RTP target parsed from SDP
	remoteRtpIP   string
	remoteRtpPort int
}

// SIPConfig holds SIP endpoint configuration
type SIPConfig struct {
	LocalIP     string
	LocalPort   int    // SIP signaling port
	PublicIP    string // Publicly routable IP address
	Username    string
	Password    string
	Domain      string
	DisplayName string
}

func (s *SIPBridge) sipIP() string {
	if s.config.PublicIP != "" && s.config.PublicIP != "0.0.0.0" {
		return s.config.PublicIP
	}
	return s.config.LocalIP
}

// NewSIPBridge creates a new SIP bridge
func NewSIPBridge(config SIPConfig, room *Room) (*SIPBridge, error) {
	// First, try binding to the preferred fixed SIP port 5062 and RTP port 5064
	sipPort := 5062
	rtpPort := 5064

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", config.LocalIP, sipPort))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve SIP addr: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		// Port 5062 is in use, fallback to dynamic port allocation
		log.Printf("[SIP] Fixed port 5062 in use or unavailable: %v. Falling back to dynamic port.", err)
		addrDynamic, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:0", config.LocalIP))
		conn, err = net.ListenUDP("udp", addrDynamic)
		if err != nil {
			return nil, fmt.Errorf("failed to listen dynamic SIP UDP: %w", err)
		}
	}

	assignedSipPort := conn.LocalAddr().(*net.UDPAddr).Port
	config.LocalPort = assignedSipPort

	// Try binding RTP to 5064
	var rtpConn *net.UDPConn
	var assignedRtpPort int

	if assignedSipPort == 5062 {
		rtpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", config.LocalIP, rtpPort))
		if err == nil {
			rtpConn, err = net.ListenUDP("udp", rtpAddr)
		}
	}

	if rtpConn == nil || err != nil {
		// Fallback to dynamic port allocation for RTP too
		if assignedSipPort == 5062 {
			log.Printf("[SIP] Fixed RTP port 5064 in use, falling back to dynamic port allocation")
		}
		rtpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:0", config.LocalIP))
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to resolve RTP addr: %w", err)
		}
		rtpConn, err = net.ListenUDP("udp", rtpAddr)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to listen RTP UDP: %w", err)
		}
	}

	assignedRtpPort = rtpConn.LocalAddr().(*net.UDPAddr).Port

	bridge := &SIPBridge{
		config:  config,
		room:    room,
		conn:    conn,
		rtpConn: rtpConn,
		rtpPort: assignedRtpPort,
		rtpIn:   make(chan []byte, 200),
		rtpOut:  make(chan []byte, 200),
	}

	return bridge, nil
}

// Start starts the SIP server and sends REGISTER
func (s *SIPBridge) Start() error {
	// Start SIP message handler
	go s.sipMessageLoop()

	// Start RTP receiver
	go s.rtpReceiveLoop()

	// Start media bridge
	go s.mediaBridgeLoop()

	// Send REGISTER
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := s.register(); err != nil {
			log.Printf("[SIP] Initial registration failed: %v", err)
		}
	}()

	log.Printf("[SIP] Bridge started on %s:%d (RTP: %d)", s.config.LocalIP, s.config.LocalPort, s.rtpPort)
	return nil
}

// Stop stops the SIP bridge
func (s *SIPBridge) Stop() {
	s.Hangup()
	if s.conn != nil {
		s.conn.Close()
	}
	if s.rtpConn != nil {
		s.rtpConn.Close()
	}
	close(s.rtpIn)
	close(s.rtpOut)
	log.Printf("[SIP] Bridge stopped")
}

// sipMessageLoop handles incoming SIP messages
func (s *SIPBridge) sipMessageLoop() {
	buf := make([]byte, 65535)
	for {
		n, remoteAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if s.room.GetStatus() == RoomStatusEnded || s.room.GetStatus() == RoomStatusFailed || strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			log.Printf("[SIP] Read error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		msg := string(buf[:n])
		go s.handleSIPMessage(msg, remoteAddr)
	}
}

// handleSIPMessage parses and routes SIP messages
func (s *SIPBridge) handleSIPMessage(msg string, remoteAddr *net.UDPAddr) {
	lines := strings.Split(msg, "\r\n")
	if len(lines) == 0 {
		return
	}

	firstLine := lines[0]

	if strings.HasPrefix(firstLine, "INVITE") {
		s.handleInvite(msg, lines, remoteAddr)
	} else if strings.HasPrefix(firstLine, "ACK") {
		s.handleAck(msg, lines, remoteAddr)
	} else if strings.HasPrefix(firstLine, "BYE") {
		s.handleBye(msg, lines, remoteAddr)
	} else if strings.Contains(firstLine, "200 OK") && s.isPendingRegister() {
		s.handleRegisterResponse(msg, lines)
	} else if strings.Contains(firstLine, "200 OK") {
		s.handleOK(msg, lines)
	} else if strings.Contains(firstLine, "100 Trying") {
		// Ignore
	} else if strings.Contains(firstLine, "180 Ringing") {
		log.Printf("[SIP] 180 Ringing received")
	}
}

// handleInvite processes incoming INVITE from Vobiz
func (s *SIPBridge) handleInvite(msg string, lines []string, remoteAddr *net.UDPAddr) {
	log.Printf("[SIP] INVITE received from %s", remoteAddr)
	s.remoteAddr = remoteAddr

	// Extract Call-ID and From tag
	callID := extractHeader(lines, "Call-ID")
	from := extractHeader(lines, "From")

	// Parse remote media IP and port from incoming SDP offer
	remoteRtpIP, remoteRtpPort := parseSDP(msg)

	s.mu.Lock()
	s.callID = callID
	s.fromTag = extractTag(from)
	s.toTag = generateTag()
	s.cseq = extractCSeq(lines)
	s.remoteRtpIP = remoteRtpIP
	s.remoteRtpPort = remoteRtpPort
	s.mu.Unlock()

	log.Printf("[SIP] Parsed remote RTP destination from SDP: %s:%d", remoteRtpIP, remoteRtpPort)

	// Send 100 Trying
	s.sendResponse("100 Trying", lines, remoteAddr)

	// Send 180 Ringing
	s.sendResponse("180 Ringing", lines, remoteAddr)

	// Send 200 OK with SDP
	sdpAnswer := s.createSDPAnswer()
	ok := s.buildResponse("200 OK", lines)
	ok += fmt.Sprintf("Content-Type: application/sdp\r\n")
	ok += fmt.Sprintf("Content-Length: %d\r\n", len(sdpAnswer))
	ok += "\r\n"
	ok += sdpAnswer

	s.sendRaw(ok, remoteAddr)

	s.mu.Lock()
	s.connected = true
	s.mu.Unlock()

	log.Printf("[SIP] Sent 200 OK, call connected")

	// Notify room
	select {
	case s.room.SignalingChan <- SignalingMessage{Type: "connected", RoomID: s.room.ID}:
	default:
	}
}

// handleAck processes ACK
func (s *SIPBridge) handleAck(msg string, lines []string, remoteAddr *net.UDPAddr) {
	log.Printf("[SIP] ACK received")
}

// handleBye processes BYE
func (s *SIPBridge) handleBye(msg string, lines []string, remoteAddr *net.UDPAddr) {
	log.Printf("[SIP] BYE received")

	// Send 200 OK
	s.sendResponse("200 OK", lines, remoteAddr)

	s.mu.Lock()
	s.connected = false
	s.mu.Unlock()

	// Notify room
	s.room.SetStatus(RoomStatusEnded)
	select {
	case s.room.SignalingChan <- SignalingMessage{Type: "left", RoomID: s.room.ID}:
	default:
	}
}

// handleRegisterResponse processes 200 OK for REGISTER
func (s *SIPBridge) handleRegisterResponse(msg string, lines []string) {
	log.Printf("[SIP] REGISTER 200 OK received")
	s.mu.Lock()
	s.registered = true
	s.mu.Unlock()
}

// handleOK processes generic 200 OK
func (s *SIPBridge) handleOK(msg string, lines []string) {
	log.Printf("[SIP] 200 OK received")
}

// register sends REGISTER to Vobiz
func (s *SIPBridge) register() error {
	registerURI := fmt.Sprintf("sip:%s", s.config.Domain)
	fromURI := fmt.Sprintf("sip:%s@%s", s.config.Username, s.config.Domain)
	sipIP := s.sipIP()
	contactURI := fmt.Sprintf("sip:%s@%s:%d", s.config.Username, sipIP, s.config.LocalPort)

	msg := fmt.Sprintf("REGISTER %s SIP/2.0\r\n", registerURI)
	msg += fmt.Sprintf("Via: SIP/2.0/UDP %s:%d;branch=z9hG4bK%s\r\n", sipIP, s.config.LocalPort, generateBranch())
	msg += fmt.Sprintf("From: \"%s\" <%s>;tag=%s\r\n", s.config.DisplayName, fromURI, generateTag())
	msg += fmt.Sprintf("To: \"%s\" <%s>\r\n", s.config.DisplayName, fromURI)
	msg += fmt.Sprintf("Call-ID: %s\r\n", generateCallID())
	msg += fmt.Sprintf("CSeq: 1 REGISTER\r\n")
	msg += fmt.Sprintf("Contact: <%s>\r\n", contactURI)
	msg += fmt.Sprintf("Max-Forwards: 70\r\n")
	msg += fmt.Sprintf("Expires: 3600\r\n")
	msg += fmt.Sprintf("Content-Length: 0\r\n")
	msg += "\r\n"

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:5060", s.config.Domain))
	if err != nil {
		return err
	}

	_, err = s.conn.WriteToUDP([]byte(msg), addr)
	if err != nil {
		return err
	}

	log.Printf("[SIP] REGISTER sent to %s", addr)
	return nil
}

// Call initiates outbound call (used for testing, normally Vobiz calls us)
func (s *SIPBridge) Call(toNumber string) error {
	if !s.isRegistered() {
		return fmt.Errorf("not registered")
	}

	// Build INVITE
	uri := fmt.Sprintf("sip:%s@%s", toNumber, s.config.Domain)
	fromURI := fmt.Sprintf("sip:%s@%s", s.config.Username, s.config.Domain)
	sipIP := s.sipIP()
	contactURI := fmt.Sprintf("sip:%s@%s:%d", s.config.Username, sipIP, s.config.LocalPort)

	callID := generateCallID()
	fromTag := generateTag()

	s.mu.Lock()
	s.callID = callID
	s.fromTag = fromTag
	s.cseq = 1
	s.mu.Unlock()

	sdp := s.createSDPOffer()

	msg := fmt.Sprintf("INVITE %s SIP/2.0\r\n", uri)
	msg += fmt.Sprintf("Via: SIP/2.0/UDP %s:%d;branch=z9hG4bK%s\r\n", sipIP, s.config.LocalPort, generateBranch())
	msg += fmt.Sprintf("From: \"%s\" <%s>;tag=%s\r\n", s.config.DisplayName, fromURI, fromTag)
	msg += fmt.Sprintf("To: <sip:%s@%s>\r\n", toNumber, s.config.Domain)
	msg += fmt.Sprintf("Call-ID: %s\r\n", callID)
	msg += fmt.Sprintf("CSeq: 1 INVITE\r\n")
	msg += fmt.Sprintf("Contact: <%s>\r\n", contactURI)
	msg += fmt.Sprintf("Content-Type: application/sdp\r\n")
	msg += fmt.Sprintf("Content-Length: %d\r\n", len(sdp))
	msg += "\r\n"
	msg += sdp

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:5060", s.config.Domain))
	if err != nil {
		return err
	}

	_, err = s.conn.WriteToUDP([]byte(msg), addr)
	if err != nil {
		return err
	}

	log.Printf("[SIP] INVITE sent to %s", toNumber)
	return nil
}

// Hangup sends BYE
func (s *SIPBridge) Hangup() {
	s.mu.Lock()
	if !s.connected {
		s.mu.Unlock()
		return
	}
	callID := s.callID
	fromTag := s.fromTag
	toTag := s.toTag
	cseq := s.cseq + 1
	s.cseq = cseq
	s.connected = false
	s.mu.Unlock()

	fromURI := fmt.Sprintf("sip:%s@%s", s.config.Username, s.config.Domain)
	sipIP := s.sipIP()

	msg := fmt.Sprintf("BYE sip:%s@%s SIP/2.0\r\n", s.config.Domain, s.config.Domain)
	msg += fmt.Sprintf("Via: SIP/2.0/UDP %s:%d;branch=z9hG4bK%s\r\n", sipIP, s.config.LocalPort, generateBranch())
	msg += fmt.Sprintf("From: <%s>;tag=%s\r\n", fromURI, fromTag)
	msg += fmt.Sprintf("To: <sip:%s@%s>;tag=%s\r\n", s.config.Domain, s.config.Domain, toTag)
	msg += fmt.Sprintf("Call-ID: %s\r\n", callID)
	msg += fmt.Sprintf("CSeq: %d BYE\r\n", cseq)
	msg += fmt.Sprintf("Content-Length: 0\r\n")
	msg += "\r\n"

	if s.remoteAddr != nil {
		s.sendRaw(msg, s.remoteAddr)
	}

	log.Printf("[SIP] BYE sent")
}

// rtpReceiveLoop receives RTP packets from SIP side
func (s *SIPBridge) rtpReceiveLoop() {
	buf := make([]byte, 1500)
	for {
		n, _, err := s.rtpConn.ReadFromUDP(buf)
		if err != nil {
			if s.room.GetStatus() == RoomStatusEnded || s.room.GetStatus() == RoomStatusFailed || strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		packet := make([]byte, n)
		copy(packet, buf[:n])

		select {
		case s.rtpOut <- packet:
		default:
		}
	}
}

// mediaBridgeLoop bridges media between WebRTC and SIP
func (s *SIPBridge) mediaBridgeLoop() {
	log.Printf("[SIP] Media bridge started for room %s", s.room.ID)

	// WebRTC -> SIP
	go func() {
		for packet := range s.rtpIn {
			if !s.isConnected() {
				continue
			}
			// Forward to SIP remote
			s.mu.RLock()
			remoteIP := s.remoteRtpIP
			remotePort := s.remoteRtpPort
			s.mu.RUnlock()

			if remoteIP != "" && remotePort != 0 {
				rtpAddr := &net.UDPAddr{
					IP:   net.ParseIP(remoteIP),
					Port: remotePort,
				}
				s.rtpConn.WriteToUDP(packet, rtpAddr)
			} else if s.remoteAddr != nil {
				// Fallback if SDP parsing failed or we don't have remote RTP details
				rtpAddr := &net.UDPAddr{
					IP:   s.remoteAddr.IP,
					Port: s.remoteAddr.Port + 2, // RTP port fallback
				}
				s.rtpConn.WriteToUDP(packet, rtpAddr)
			}
		}
	}()

	// SIP -> WebRTC (handled by reading from rtpOut in room)
}

// WriteRTP writes RTP to SIP side
func (s *SIPBridge) WriteRTP(packet []byte) {
	select {
	case s.rtpIn <- packet:
	default:
	}
}

// ReadRTP reads RTP from SIP side
func (s *SIPBridge) ReadRTP() ([]byte, bool) {
	select {
	case packet := <-s.rtpOut:
		return packet, true
	case <-time.After(50 * time.Millisecond):
		return nil, false
	}
}

// Helper methods

func (s *SIPBridge) sendResponse(status string, requestLines []string, addr *net.UDPAddr) {
	resp := s.buildResponse(status, requestLines)
	resp += "Content-Length: 0\r\n"
	resp += "\r\n"
	s.sendRaw(resp, addr)
}

func (s *SIPBridge) buildResponse(status string, requestLines []string) string {
	via := extractHeader(requestLines, "Via")
	from := extractHeader(requestLines, "From")
	to := extractHeader(requestLines, "To")
	callID := extractHeader(requestLines, "Call-ID")
	cseq := extractHeader(requestLines, "CSeq")

	s.mu.RLock()
	toTag := s.toTag
	s.mu.RUnlock()

	resp := fmt.Sprintf("SIP/2.0 %s\r\n", status)
	resp += fmt.Sprintf("Via: %s\r\n", via)
	resp += fmt.Sprintf("From: %s\r\n", from)

	if toTag != "" && !strings.Contains(to, "tag=") {
		resp += fmt.Sprintf("To: %s;tag=%s\r\n", to, toTag)
	} else {
		resp += fmt.Sprintf("To: %s\r\n", to)
	}

	resp += fmt.Sprintf("Call-ID: %s\r\n", callID)
	resp += fmt.Sprintf("CSeq: %s\r\n", cseq)
	resp += fmt.Sprintf("Contact: <sip:%s@%s:%d>\r\n", s.config.Username, s.sipIP(), s.config.LocalPort)
	resp += fmt.Sprintf("Server: FancallGateway/1.0\r\n")

	return resp
}

func (s *SIPBridge) sendRaw(msg string, addr *net.UDPAddr) {
	if addr == nil {
		return
	}
	_, err := s.conn.WriteToUDP([]byte(msg), addr)
	if err != nil {
		log.Printf("[SIP] Failed to send: %v", err)
	}
}

func (s *SIPBridge) isConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

func (s *SIPBridge) isRegistered() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.registered
}

func (s *SIPBridge) isPendingRegister() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.registered
}

func (s *SIPBridge) createSDPOffer() string {
	sipIP := s.sipIP()
	return fmt.Sprintf("v=0\r\n"+
		"o=- %d 0 IN IP4 %s\r\n"+
		"s=FancallGateway\r\n"+
		"c=IN IP4 %s\r\n"+
		"t=0 0\r\n"+
		"m=audio %d RTP/AVP 0 8 101\r\n"+
		"a=rtpmap:0 PCMU/8000\r\n"+
		"a=rtpmap:8 PCMA/8000\r\n"+
		"a=rtpmap:101 telephone-event/8000\r\n"+
		"a=fmtp:101 0-15\r\n"+
		"a=sendrecv\r\n",
		time.Now().Unix(), sipIP, sipIP, s.rtpPort)
}

func (s *SIPBridge) createSDPAnswer() string {
	sipIP := s.sipIP()
	return fmt.Sprintf("v=0\r\n"+
		"o=- %d 0 IN IP4 %s\r\n"+
		"s=FancallGateway\r\n"+
		"c=IN IP4 %s\r\n"+
		"t=0 0\r\n"+
		"m=audio %d RTP/AVP 0 8 101\r\n"+
		"a=rtpmap:0 PCMU/8000\r\n"+
		"a=rtpmap:8 PCMA/8000\r\n"+
		"a=rtpmap:101 telephone-event/8000\r\n"+
		"a=fmtp:101 0-15\r\n"+
		"a=sendrecv\r\n",
		time.Now().Unix(), sipIP, sipIP, s.rtpPort)
}

// SIP header extraction helpers

func extractHeader(lines []string, name string) string {
	for _, line := range lines {
		if strings.HasPrefix(strings.ToLower(line), strings.ToLower(name)+":") {
			return strings.TrimSpace(line[len(name)+1:])
		}
	}
	return ""
}

func extractTag(from string) string {
	idx := strings.Index(from, "tag=")
	if idx == -1 {
		return ""
	}
	return strings.TrimSpace(from[idx+4:])
}

func extractCSeq(lines []string) int {
	cseq := extractHeader(lines, "CSeq")
	parts := strings.Fields(cseq)
	if len(parts) > 0 {
		n, _ := strconv.Atoi(parts[0])
		return n
	}
	return 0
}

// ID generators

func generateBranch() string {
	return fmt.Sprintf("z9hG4bK%d", time.Now().UnixNano())
}

func generateTag() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())[:8]
}

func generateCallID() string {
	return fmt.Sprintf("%d@%s", time.Now().UnixNano(), generateTag())
}

// parseSDP parses remote media IP and Port from SDP body
func parseSDP(msg string) (string, int) {
	var ip string
	var port int
	lines := strings.Split(msg, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		lowerLine := strings.ToLower(line)
		if strings.HasPrefix(lowerLine, "c=in ip4 ") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				ip = parts[2]
			}
		} else if strings.HasPrefix(lowerLine, "m=audio ") {
			// Format: m=audio 10242 RTP/AVP 0 ...
			parts := strings.Fields(line)
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &port)
			}
		}
	}
	return ip, port
}
