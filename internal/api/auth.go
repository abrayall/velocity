package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"velocity/internal/log"
	"velocity/internal/storage"
)

const (
	adminUsername     = "velocity"
	adminPassword     = "V3l0c1ty@12345"
	sessionCookieName = "velocity_session"
	sessionDuration   = 24 * time.Hour
	sweepInterval     = 6 * time.Hour
)

// cachedSession holds an in-memory cache entry for a session
type cachedSession struct {
	ExpiresAt time.Time
}

// sessionStore manages sessions with S3 persistence and in-memory cache
type sessionStore struct {
	mu      sync.RWMutex
	cache   map[string]*cachedSession
	storage storage.Storage
}

func newSessionStore(s storage.Storage) *sessionStore {
	ss := &sessionStore{
		cache:   make(map[string]*cachedSession),
		storage: s,
	}
	go ss.sweepLoop()
	return ss
}

func (ss *sessionStore) create() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := hex.EncodeToString(b)
	expiresAt := time.Now().Add(sessionDuration)

	// Persist to S3
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ss.storage.PutSession(ctx, token, expiresAt); err != nil {
		log.Error("Failed to persist session to S3: %v", err)
		// Still cache it in memory so login works even if S3 hiccups
	}

	// Cache in memory
	ss.mu.Lock()
	ss.cache[token] = &cachedSession{ExpiresAt: expiresAt}
	ss.mu.Unlock()

	return token, nil
}

func (ss *sessionStore) validate(token string) bool {
	// Check in-memory cache first
	ss.mu.RLock()
	cached, ok := ss.cache[token]
	ss.mu.RUnlock()

	if ok {
		if time.Now().After(cached.ExpiresAt) {
			// Lazy delete expired session
			go ss.remove(token)
			return false
		}
		return true
	}

	// Not in cache — check S3
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	expiresAt, err := ss.storage.GetSession(ctx, token)
	if err != nil {
		return false
	}

	if time.Now().After(expiresAt) {
		// Lazy delete expired session
		go ss.remove(token)
		return false
	}

	// Cache it for future lookups
	ss.mu.Lock()
	ss.cache[token] = &cachedSession{ExpiresAt: expiresAt}
	ss.mu.Unlock()

	return true
}

func (ss *sessionStore) remove(token string) {
	// Remove from cache
	ss.mu.Lock()
	delete(ss.cache, token)
	ss.mu.Unlock()

	// Remove from S3
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ss.storage.DeleteSession(ctx, token); err != nil {
		log.Debug("Failed to delete session from S3: %v", err)
	}
}

// sweepLoop runs periodic cleanup of expired sessions
func (ss *sessionStore) sweepLoop() {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for range ticker.C {
		ss.sweep()
	}
}

func (ss *sessionStore) sweep() {
	log.Debug("Sweeping expired sessions...")

	// Clean in-memory cache
	now := time.Now()
	ss.mu.Lock()
	for token, cached := range ss.cache {
		if now.After(cached.ExpiresAt) {
			delete(ss.cache, token)
		}
	}
	ss.mu.Unlock()

	// Clean S3
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	deleted, err := ss.storage.DeleteExpiredSessions(ctx)
	if err != nil {
		log.Error("Session sweep error: %v", err)
		return
	}

	if deleted > 0 {
		log.Info("Session sweep: deleted %d expired sessions", deleted)
	} else {
		log.Debug("Session sweep: no expired sessions found")
	}
}

// loginHandler handles POST /api/login
func (s *Server) loginHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	if req.Username != adminUsername || req.Password != adminPassword {
		writeError(w, http.StatusUnauthorized, "invalid_credentials", "Invalid username or password")
		return
	}

	token, err := s.sessions.create()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "session_error", "Failed to create session")
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionDuration.Seconds()),
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"token":   token,
		"message": "Login successful",
	})
}

// logoutHandler handles POST /api/logout
func (s *Server) logoutHandler(w http.ResponseWriter, r *http.Request) {
	token := extractToken(r)
	if token != "" {
		s.sessions.remove(token)
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "Logged out successfully",
	})
}

// sessionHandler handles GET /api/session
func (s *Server) sessionHandler(w http.ResponseWriter, r *http.Request) {
	token := extractToken(r)
	valid := token != "" && s.sessions.validate(token)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"valid": valid,
	})
}

// adminAuthMiddleware checks for a valid session before allowing access to admin routes
func (s *Server) adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := extractToken(r)
		if token == "" || !s.sessions.validate(token) {
			http.Redirect(w, r, "/admin/login", http.StatusFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// extractToken gets the session token from cookie or Authorization header
func extractToken(r *http.Request) string {
	if cookie, err := r.Cookie(sessionCookieName); err == nil && cookie.Value != "" {
		return cookie.Value
	}

	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
	}

	return ""
}
