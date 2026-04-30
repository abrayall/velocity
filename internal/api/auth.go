package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	adminUsername     = "velocity"
	adminPassword     = "V3l0c1ty@12345"
	sessionCookieName = "velocity_session"
	sessionDuration   = 24 * time.Hour
)

type session struct {
	Token     string
	ExpiresAt time.Time
}

type sessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*session
}

var sessions = &sessionStore{
	sessions: make(map[string]*session),
}

func (ss *sessionStore) create() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := hex.EncodeToString(b)

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.sessions[token] = &session{
		Token:     token,
		ExpiresAt: time.Now().Add(sessionDuration),
	}
	return token, nil
}

func (ss *sessionStore) validate(token string) bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	s, ok := ss.sessions[token]
	if !ok {
		return false
	}
	if time.Now().After(s.ExpiresAt) {
		return false
	}
	return true
}

func (ss *sessionStore) remove(token string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.sessions, token)
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

	token, err := sessions.create()
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
		sessions.remove(token)
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
	valid := token != "" && sessions.validate(token)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"valid": valid,
	})
}

// adminAuthMiddleware checks for a valid session before allowing access to admin routes
func (s *Server) adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := extractToken(r)
		if token == "" || !sessions.validate(token) {
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
