package main

import (
	"crypto/md5"
	"encoding/hex"
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

	mu           sync.RWMutex
	callID       string
	fromTag      string
	toTag        string
	cseq         int
	connected    bool
	registered   bool
	isSharedConn bool

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
func NewSIPBridge(config SIPConfig, room *Room, sharedConn *net.UDPConn) (*SIPBridge, error) {
	var conn *net.UDPConn
	isSharedConn := false

	if sharedConn != nil {
		conn = sharedConn
		isSharedConn = true
		config.LocalPort = 5062
	} else {
		// Fallback: bind own socket on port 5062 or dynamic port
		sipPort := 5062
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", config.LocalIP, sipPort))
		if err == nil {
			conn, err = net.ListenUDP("udp", addr)
		}
		if conn == nil || err != nil {
			addrDynamic, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:0", config.LocalIP))
			conn, err = net.ListenUDP("udp", addrDynamic)
			if err != nil {
				return nil, fmt.Errorf("failed to listen dynamic SIP UDP: %w", err)
			}
		}
		config.LocalPort = conn.LocalAddr().(*net.UDPAddr).Port
	}

	// Try binding RTP socket on 5064 or dynamic port
	rtpPort := 5064
	var rtpConn *net.UDPConn
	rtpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", config.LocalIP, rtpPort))
	if err == nil {
		rtpConn, err = net.ListenUDP("udp", rtpAddr)
	}

	if rtpConn == nil || err != nil {
		rtpAddrDynamic, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:0", config.LocalIP))
		rtpConn, err = net.ListenUDP("udp", rtpAddrDynamic)
		if err != nil {
			if !isSharedConn && conn != nil {
				conn.Close()
			}
			return nil, fmt.Errorf("failed to listen RTP UDP: %w", err)
		}
	}

	assignedRtpPort := rtpConn.LocalAddr().(*net.UDPAddr).Port

	bridge := &SIPBridge{
		config:       config,
		room:         room,
		conn:         conn,
		isSharedConn: isSharedConn,
		rtpConn:      rtpConn,
		rtpPort:      assignedRtpPort,
		rtpIn:        make(chan []byte, 200),
		rtpOut:       make(chan []byte, 200),
	}

	return bridge, nil
}

// Start starts the SIP server and sends REGISTER
func (s *SIPBridge) Start() error {
	// Only start individual sipMessageLoop if not using shared socket
	if !s.isSharedConn {
		go s.sipMessageLoop()
	}

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

	log.Printf("[SIP] Bridge started on %s:%d (RTP: %d, SharedSIP=%v)", s.config.LocalIP, s.config.LocalPort, s.rtpPort, s.isSharedConn)
	return nil
}

// Stop stops the SIP bridge
func (s *SIPBridge) Stop() {
	s.Hangup()
	if !s.isSharedConn && s.conn != nil {
		s.conn.Close()
	}
	if s.rtpConn != nil {
		s.rtpConn.Close()
	}
	close(s.rtpIn)
	close(s.rtpOut)
	log.Printf("[SIP] Bridge stopped")
}

// HandleIncomingSIP is called by RoomManager for shared socket messages
func (s *SIPBridge) HandleIncomingSIP(msg string, remoteAddr *net.UDPAddr) {
	s.handleSIPMessage(msg, remoteAddr)
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

		// Diagnostic: Log first line of every incoming SIP packet
		firstLine := ""
		if idx := strings.Index(msg, "\r\n"); idx != -1 {
			firstLine = msg[:idx]
		} else if idx := strings.Index(msg, "\n"); idx != -1 {
			firstLine = msg[:idx]
		} else {
			firstLine = msg
		}
		log.Printf("[SIP RAW] Received from %s: %s", remoteAddr, firstLine)

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
	} else if strings.Contains(firstLine, "401 Unauthorized") || strings.Contains(firstLine, "407 Proxy Authentication Required") {
		s.handleUnauthorized(msg, lines)
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
	log.Printf("[SIP] INVITE received from %s:\n%s", remoteAddr, msg)
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

	log.Printf("[SIP] Sent 200 OK, handshake pending ACK")
}

// handleAck processes ACK
func (s *SIPBridge) handleAck(msg string, lines []string, remoteAddr *net.UDPAddr) {
	log.Printf("[SIP] ACK received - call fully established")
	s.mu.Lock()
	s.connected = true
	s.mu.Unlock()

	// Notify room that the call is now fully connected (PSTN answered and handshake completed!)
	s.room.SendSignalingMessage(SignalingMessage{Type: "connected", RoomID: s.room.ID})
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
	s.room.SendSignalingMessage(SignalingMessage{Type: "left", RoomID: s.room.ID})
}

// handleRegisterResponse processes 200 OK for REGISTER
func (s *SIPBridge) handleRegisterResponse(msg string, lines []string) {
	log.Printf("[SIP] REGISTER 200 OK received - SIP endpoint registered successfully")
	s.mu.Lock()
	s.registered = true
	s.mu.Unlock()
}

// handleUnauthorized processes 401/407 challenges and re-sends REGISTER with Digest Authorization
func (s *SIPBridge) handleUnauthorized(msg string, lines []string) {
	log.Printf("[SIP] 401/407 Challenge received from Vobiz. Computing Digest Auth...")

	authHeader := extractHeader(lines, "WWW-Authenticate")
	if authHeader == "" {
		authHeader = extractHeader(lines, "Proxy-Authenticate")
	}

	if authHeader == "" {
		log.Printf("[SIP] Error: 401 response missing WWW-Authenticate header")
		return
	}

	realm := extractAuthParam(authHeader, "realm")
	nonce := extractAuthParam(authHeader, "nonce")
	qop := extractAuthParam(authHeader, "qop")
	opaque := extractAuthParam(authHeader, "opaque")

	log.Printf("[SIP] Digest Auth params: realm=%s, nonce=%s, qop=%s", realm, nonce, qop)

	registerURI := fmt.Sprintf("sip:%s", s.config.Domain)
	fromURI := fmt.Sprintf("sip:%s@%s", s.config.Username, s.config.Domain)
	sipIP := s.sipIP()
	contactURI := fmt.Sprintf("sip:%s@%s:%d", s.config.Username, sipIP, s.config.LocalPort)

	cnonce := generateRandomString(16)
	nc := "00000001"

	response := calculateDigestResponse(s.config.Username, s.config.Password, realm, nonce, registerURI, "REGISTER", qop, cnonce, nc)

	regMsg := fmt.Sprintf("REGISTER %s SIP/2.0\r\n", registerURI)
	regMsg += fmt.Sprintf("Via: SIP/2.0/UDP %s:%d;branch=z9hG4bK%s\r\n", sipIP, s.config.LocalPort, generateBranch())
	regMsg += fmt.Sprintf("From: \"%s\" <%s>;tag=%s\r\n", s.config.DisplayName, fromURI, generateTag())
	regMsg += fmt.Sprintf("To: \"%s\" <%s>\r\n", s.config.DisplayName, fromURI)
	regMsg += fmt.Sprintf("Call-ID: %s\r\n", generateCallID())
	regMsg += fmt.Sprintf("CSeq: 2 REGISTER\r\n")
	regMsg += fmt.Sprintf("Contact: <%s>\r\n", contactURI)
	regMsg += fmt.Sprintf("Max-Forwards: 70\r\n")
	regMsg += fmt.Sprintf("Expires: 3600\r\n")

	digestHeader := fmt.Sprintf(`Authorization: Digest username="%s", realm="%s", nonce="%s", uri="%s", response="%s", algorithm=MD5`,
		s.config.Username, realm, nonce, registerURI, response)
	if qop != "" {
		digestHeader += fmt.Sprintf(`, qop=%s, nc=%s, cnonce="%s"`, qop, nc, cnonce)
	}
	if opaque != "" {
		digestHeader += fmt.Sprintf(`, opaque="%s"`, opaque)
	}

	regMsg += digestHeader + "\r\n"
	regMsg += fmt.Sprintf("Content-Length: 0\r\n")
	regMsg += "\r\n"

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:5060", s.config.Domain))
	if err != nil {
		log.Printf("[SIP] Failed to resolve SIP domain: %v", err)
		return
	}

	_, err = s.conn.WriteToUDP([]byte(regMsg), addr)
	if err != nil {
		log.Printf("[SIP] Failed to send authenticated REGISTER: %v", err)
		return
	}

	log.Printf("[SIP] Authenticated REGISTER sent to %s with Digest response", addr)
}

func extractAuthParam(header, param string) string {
	pattern := param + `="`
	idx := strings.Index(header, pattern)
	if idx != -1 {
		start := idx + len(pattern)
		end := strings.Index(header[start:], `"`)
		if end != -1 {
			return header[start : start+end]
		}
	}
	patternUnquoted := param + `=`
	idx = strings.Index(header, patternUnquoted)
	if idx != -1 {
		start := idx + len(patternUnquoted)
		rest := header[start:]
		end := strings.IndexAny(rest, ", \r\n")
		if end != -1 {
			return rest[:end]
		}
		return rest
	}
	return ""
}

func calculateDigestResponse(username, password, realm, nonce, uri, method, qop, cnonce, nc string) string {
	ha1Buf := md5.Sum([]byte(fmt.Sprintf("%s:%s:%s", username, realm, password)))
	ha1 := hex.EncodeToString(ha1Buf[:])

	ha2Buf := md5.Sum([]byte(fmt.Sprintf("%s:%s", method, uri)))
	ha2 := hex.EncodeToString(ha2Buf[:])

	var respBuf [16]byte
	if qop != "" {
		respBuf = md5.Sum([]byte(fmt.Sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2)))
	} else {
		respBuf = md5.Sum([]byte(fmt.Sprintf("%s:%s:%s", ha1, nonce, ha2)))
	}

	return hex.EncodeToString(respBuf[:])
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
	var packetCount int
	for {
		n, addr, err := s.rtpConn.ReadFromUDP(buf)
		if err != nil {
			if s.room.GetStatus() == RoomStatusEnded || s.room.GetStatus() == RoomStatusFailed || strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		packetCount++
		if packetCount <= 5 || packetCount%100 == 0 {
			log.Printf("[RTP-IN] Received %d bytes from %s (packet count: %d)", n, addr.String(), packetCount)
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
			}
		}
	}()

	// SIP -> WebRTC
	go func() {
		var packetCount int
		for packet := range s.rtpOut {
			if !s.isConnected() {
				continue
			}
			localTrack := s.room.GetLocalTrack()
			if localTrack != nil {
				// Ensure the packet size is at least the size of standard RTP header (12 bytes)
				if len(packet) < 12 {
					continue
				}

				packetCount++
				if packetCount%100 == 0 {
					log.Printf("[SIP] SIP -> WebRTC: Forwarded %d RTP packets to WebRTC localTrack. Length=%d", packetCount, len(packet))
				}

				// Normalize the Payload Type of the outgoing WebRTC packet to PCMU (0)
				packet[1] = (packet[1] & 0x80) | 0

				// Forward packet directly to WebRTC
				_, err := localTrack.Write(packet)
				if err != nil {
					log.Printf("[SIP] Write RTP to WebRTC error: %v", err)
				}
			}
		}
	}()
}

// WriteRTP writes RTP to SIP side safely
func (s *SIPBridge) WriteRTP(packet []byte) {
	defer func() {
		if r := recover(); r != nil {
			// Prevent panic on closed rtpIn channel
		}
	}()
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
	from := extractHeader(requestLines, "From")
	to := extractHeader(requestLines, "To")
	callID := extractHeader(requestLines, "Call-ID")
	cseq := extractHeader(requestLines, "CSeq")

	s.mu.RLock()
	toTag := s.toTag
	s.mu.RUnlock()

	resp := fmt.Sprintf("SIP/2.0 %s\r\n", status)

	// Mirror ALL Via headers in the exact same order for proper transactional routing
	for _, line := range requestLines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(trimmed), "via:") {
			resp += fmt.Sprintf("%s\r\n", trimmed)
		}
	}

	resp += fmt.Sprintf("From: %s\r\n", from)

	if toTag != "" && !strings.Contains(strings.ToLower(to), "tag=") {
		resp += fmt.Sprintf("To: %s;tag=%s\r\n", to, toTag)
	} else {
		resp += fmt.Sprintf("To: %s\r\n", to)
	}

	resp += fmt.Sprintf("Call-ID: %s\r\n", callID)
	resp += fmt.Sprintf("CSeq: %s\r\n", cseq)

	// Mirror Record-Route headers from incoming request for proper proxy routing
	for _, line := range requestLines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(trimmed), "record-route:") {
			resp += fmt.Sprintf("%s\r\n", trimmed)
		}
	}

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

// parseSDP parses remote media IP and Port from SDP body, preferring media-level c= connection addresses
func parseSDP(msg string) (string, int) {
	msg = strings.ReplaceAll(msg, "\r\n", "\n")
	lines := strings.Split(msg, "\n")

	var sessionIP string
	var mediaIP string
	var port int
	inAudioSection := false

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if strings.HasPrefix(strings.ToLower(line), "c=") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				fields := strings.Fields(strings.TrimSpace(parts[1]))
				if len(fields) >= 3 {
					if inAudioSection {
						mediaIP = fields[2]
					} else {
						sessionIP = fields[2]
					}
				}
			}
		}

		if strings.HasPrefix(strings.ToLower(line), "m=audio") {
			inAudioSection = true
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				fields := strings.Fields(strings.TrimSpace(parts[1]))
				if len(fields) >= 2 {
					fmt.Sscanf(fields[1], "%d", &port)
				}
			}
		} else if strings.HasPrefix(strings.ToLower(line), "m=") {
			inAudioSection = false
		}
	}

	ip := sessionIP
	if mediaIP != "" {
		ip = mediaIP
	}
	return ip, port
}
