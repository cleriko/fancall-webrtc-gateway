package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

// Config holds gateway configuration
type Config struct {
	HTTPPort        string        `env:"HTTP_PORT" envDefault:"8080"`
	WebSocketPath   string        `env:"WS_PATH" envDefault:"/ws"`
	VobizAuthID     string        `env:"VOBIZ_AUTH_ID"`
	VobizAuthToken  string        `env:"VOBIZ_AUTH_TOKEN"`
	VobizBaseURL    string        `env:"VOBIZ_BASE_URL"`
	VobizFromNumber string        `env:"VOBIZ_FROM_NUMBER"`
	PublicURL       string        `env:"PUBLIC_URL"`
	SIPDomain       string        `env:"SIP_DOMAIN" envDefault:"registrar.vobiz.ai"`
	MaxCallDuration time.Duration `env:"MAX_CALL_DURATION" envDefault:"1h"`
	APIKey          string        `env:"API_KEY"`
	ICEServers      []webrtc.ICEServer
}

func main() {
	cfg := loadConfig()
	log.Printf("[Config] Loaded PublicURL: %q", cfg.PublicURL)

	// Initialize room manager
	roomManager := NewRoomManager(cfg)

	// Initialize WebSocket upgrader
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins (configure in production)
		},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	// Setup HTTP routes
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Create room (called by backend)
	mux.HandleFunc("/api/v1/rooms", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleCreateRoom(w, r, roomManager)
		case http.MethodOptions:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Room operations
	mux.HandleFunc("/api/v1/rooms/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetRoom(w, r, roomManager)
		case http.MethodDelete:
			handleDeleteRoom(w, r, roomManager)
		case http.MethodOptions:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// WebSocket signaling endpoint
	mux.HandleFunc(cfg.WebSocketPath, func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(w, r, upgrader, roomManager)
	})

	// Setup middleware chain
	apiKeyMiddleware := NewAPIKeyMiddleware([]string{cfg.APIKey})
	rateLimiter := newRateLimiter(100, 1*time.Minute)

	chain := recoveryMiddleware(
		loggingMiddleware(
			corsMiddleware(
				rateLimiter.Middleware(
					apiKeyMiddleware.Handler(mux),
				),
			),
		),
	)

	// Start server
	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: chain,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down gateway...")
		server.Shutdown(context.Background())
	}()

	log.Printf("Fancall WebRTC Gateway starting on port %s", cfg.HTTPPort)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}
}

func loadConfig() *Config {
	cfg := &Config{
		HTTPPort:      getEnv("HTTP_PORT", "8080"),
		WebSocketPath: getEnv("WS_PATH", "/ws"),
		SIPDomain:     getEnv("SIP_DOMAIN", getEnv("VOBIZ_SIP_DOMAIN", "registrar.vobiz.ai")),
		APIKey:        getEnv("API_KEY", ""),
		PublicURL:     getEnv("PUBLIC_URL", ""),
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	// Parse max call duration
	if dur := getEnv("MAX_CALL_DURATION", "1h"); dur != "" {
		if parsed, err := time.ParseDuration(dur); err == nil {
			cfg.MaxCallDuration = parsed
		}
	}

	if env := os.Getenv("ICE_SERVERS"); env != "" {
		// Parse custom ICE servers from env if provided
		cfg.ICEServers = append(cfg.ICEServers, webrtc.ICEServer{
			URLs: []string{env},
		})
	}

	return cfg
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
