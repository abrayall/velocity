package api

import (
	"embed"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/cors"

	"velocity/internal/log"
	"velocity/internal/storage"
	"velocity/internal/version"
)

// Server represents the API server
type Server struct {
	router   *mux.Router
	storage  storage.Storage
	sessions *sessionStore
	config   *ServerConfig
	wwwFS    embed.FS
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port string
}

// NewServer creates a new API server
func NewServer(storageClient storage.Storage, config *ServerConfig, wwwFS embed.FS) *Server {
	s := &Server{
		router:   mux.NewRouter(),
		storage:  storageClient,
		sessions: newSessionStore(storageClient),
		config:   config,
		wwwFS:    wwwFS,
	}

	s.setupRoutes()
	return s
}

// setupRoutes configures all API routes
func (s *Server) setupRoutes() {
	// Direct content URL (outside /api, no tenant header needed)
	// GET /content/{tenant}/{type}/{id} - Direct content access with correct mime type
	// NOTE: This route is intentionally outside /api and should remain:
	//   - Read-only (GET only)
	//   - Public (no authentication required)
	//   - Used for embeddable URLs (images, CSS, etc.)
	// {id:.+} allows nested IDs with slashes (e.g., /content/demo/images/hero/banner)
	s.router.HandleFunc("/content/{tenant}/{type}/{id:.+}", s.directContentHandler).Methods("GET")

	api := s.router.PathPrefix("/api").Subrouter()

	// Add request logging
	api.Use(s.loggingHandler)

	// Auth endpoints (public)
	// POST   /api/login             - Login and get session token
	// POST   /api/logout            - Logout and clear session
	// GET    /api/session           - Check session validity
	api.HandleFunc("/login", s.loginHandler).Methods("POST")
	api.HandleFunc("/logout", s.logoutHandler).Methods("POST")
	api.HandleFunc("/session", s.sessionHandler).Methods("GET")

	// Utility endpoints
	// GET    /api                   - API info (name, version)
	// GET    /api/health            - Health check
	// GET    /api/version           - Server version
	// GET    /api/types             - List available content types
	// GET    /api/tenants           - List all tenants
	api.HandleFunc("", s.infoHandler).Methods("GET")
	api.HandleFunc("/health", s.healthHandler).Methods("GET")
	api.HandleFunc("/version", s.versionHandler).Methods("GET")
	api.HandleFunc("/types", s.listTypesHandler).Methods("GET")
	api.HandleFunc("/types", s.createContentTypeHandler).Methods("POST")
	api.HandleFunc("/tenants", s.listTenantsHandler).Methods("GET")
	api.HandleFunc("/tenants", s.createTenantHandler).Methods("POST")

	// Content routes
	// IDs can contain slashes for nested/hierarchical content (e.g., "parent/child")
	// {id:.+} matches one or more path segments including slashes
	// {state:draft|pending|live} explicitly matches only valid state names
	//
	// ROUTE REGISTRATION ORDER MATTERS (gorilla mux matches in registration order):
	// 1. Bulk get (no {id})
	// 2. List by type (no {id})
	// 3. Literal suffix routes ({id:.+}/transition, /versions, /history, /diff, /metadata)
	// 4. State-specific routes with literal suffixes ({state}/metadata, {state}/comments)
	// 5. State-specific content routes ({state} only)
	// 6. Catch-all content routes ({id:.+} only) — MUST BE LAST

	// Bulk get
	// POST   /api/content                        - Bulk get multiple items
	api.HandleFunc("/content", s.bulkGetHandler).Methods("POST")

	// List by type
	// GET    /api/content/{type}                 - List all live items (or browse with ?prefix=)
	api.HandleFunc("/content/{type}", s.listContentHandler).Methods("GET")

	// Create folder
	// POST   /api/content/{type}/_mkdir          - Create a folder within a content type
	api.HandleFunc("/content/{type}/_mkdir", s.createFolderHandler).Methods("POST")

	// Directory index
	// GET    /api/content/{type}/_index          - Get directory index (order)
	// PUT    /api/content/{type}/_index          - Set directory index (order)
	api.HandleFunc("/content/{type}/_index", s.getDirectoryIndexHandler).Methods("GET")
	api.HandleFunc("/content/{type}/_index", s.putDirectoryIndexHandler).Methods("PUT")

	// Literal suffix routes (registered FIRST so they match before catch-all)
	// POST   /api/content/{type}/{id}/transition - Move content between states
	api.HandleFunc("/content/{type}/{id:.+}/transition", s.transitionHandler).Methods("POST")

	// Version routes
	api.HandleFunc("/content/{type}/{id:.+}/versions", s.listVersionsHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/versions/{version}", s.getVersionHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/versions/{version}/restore", s.restoreVersionHandler).Methods("POST")

	// History routes
	api.HandleFunc("/content/{type}/{id:.+}/history", s.listHistoryHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/history/{version}", s.getHistoryHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/diff", s.diffHandler).Methods("GET")

	// Metadata routes (live content)
	api.HandleFunc("/content/{type}/{id:.+}/metadata", s.getMetadataHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/metadata", s.setMetadataHandler).Methods("PUT")
	api.HandleFunc("/content/{type}/{id:.+}/metadata", s.updateMetadataHandler).Methods("PATCH")
	api.HandleFunc("/content/{type}/{id:.+}/metadata", s.deleteMetadataHandler).Methods("DELETE")

	// State-specific metadata routes (explicit state names)
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/metadata", s.getMetadataHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/metadata", s.setMetadataHandler).Methods("PUT")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/metadata", s.updateMetadataHandler).Methods("PATCH")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/metadata", s.deleteMetadataHandler).Methods("DELETE")

	// Schema routes (global schemas)
	// GET    /api/schemas              - List global schemas
	// GET    /api/schemas/{name}       - Get global schema
	// PUT    /api/schemas/{name}       - Create/update global schema
	// DELETE /api/schemas/{name}       - Delete global schema

	api.HandleFunc("/schemas", s.listGlobalSchemasHandler).Methods("GET")
	api.HandleFunc("/schemas/{name}", s.getGlobalSchemaHandler).Methods("GET")
	api.HandleFunc("/schemas/{name}", s.putGlobalSchemaHandler).Methods("PUT")
	api.HandleFunc("/schemas/{name}", s.deleteGlobalSchemaHandler).Methods("DELETE")

	// Tenant schema routes (tenant-specific overrides)
	// GET    /api/tenant/schemas              - List tenant schemas
	// GET    /api/tenant/schemas/{name}       - Get tenant schema
	// PUT    /api/tenant/schemas/{name}       - Create/update tenant schema
	// DELETE /api/tenant/schemas/{name}       - Delete tenant schema

	api.HandleFunc("/tenant/schemas", s.listTenantSchemasHandler).Methods("GET")
	api.HandleFunc("/tenant/schemas/{name}", s.getTenantSchemaHandler).Methods("GET")
	api.HandleFunc("/tenant/schemas/{name}", s.putTenantSchemaHandler).Methods("PUT")
	api.HandleFunc("/tenant/schemas/{name}", s.deleteTenantSchemaHandler).Methods("DELETE")

	// State-specific comment routes (explicit state names + literal /comments suffix)
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/comments", s.listCommentsHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/comments", s.createCommentHandler).Methods("POST")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/comments/{comment_id}", s.getCommentHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/comments/{comment_id}", s.updateCommentHandler).Methods("PUT")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}/comments/{comment_id}", s.deleteCommentHandler).Methods("DELETE")

	// State-specific content routes (explicit state names, after all literal suffix routes)
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}", s.getContentHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}", s.createContentHandler).Methods("POST")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}", s.updateContentHandler).Methods("PUT")
	api.HandleFunc("/content/{type}/{id:.+}/{state:draft|pending|live}", s.deleteContentHandler).Methods("DELETE")

	// Catch-all content routes (MUST BE LAST — {id:.+} matches anything)
	api.HandleFunc("/content/{type}/{id:.+}", s.getOrListContentHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id:.+}", s.createContentHandler).Methods("POST")
	api.HandleFunc("/content/{type}/{id:.+}", s.updateContentHandler).Methods("PUT")
	api.HandleFunc("/content/{type}/{id:.+}", s.deleteContentHandler).Methods("DELETE")

	// Webhook routes
	// GET    /api/webhooks              - List webhooks for tenant
	// GET    /api/webhooks/{id}         - Get webhook
	// PUT    /api/webhooks/{id}         - Create/update webhook
	// DELETE /api/webhooks/{id}         - Delete webhook

	api.HandleFunc("/webhooks", s.listWebhooksHandler).Methods("GET")
	api.HandleFunc("/webhooks/{id}", s.getWebhookHandler).Methods("GET")
	api.HandleFunc("/webhooks/{id}", s.putWebhookHandler).Methods("PUT")
	api.HandleFunc("/webhooks/{id}", s.deleteWebhookHandler).Methods("DELETE")

	// Serve static website files at root
	// Strip the "www" prefix from the embedded filesystem
	wwwContent, err := fs.Sub(s.wwwFS, "www")
	if err != nil {
		log.Error("Failed to create sub filesystem for www: %v", err)
		return
	}

	// Create file server for static assets
	fileServer := http.FileServer(http.FS(wwwContent))

	// Handle static assets (CSS, JS, images)
	s.router.PathPrefix("/assets/").Handler(fileServer)

	// Admin routes
	// /admin/login - unprotected login page
	s.router.HandleFunc("/admin/login", func(w http.ResponseWriter, r *http.Request) {
		data, err := fs.ReadFile(wwwContent, "admin/login.html")
		if err != nil {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	}).Methods("GET")

	// Admin static assets (CSS, JS) - unprotected so login page can load them
	s.router.PathPrefix("/admin/css/").Handler(http.StripPrefix("/admin/", http.FileServer(http.FS(func() fs.FS {
		sub, _ := fs.Sub(wwwContent, "admin")
		return sub
	}()))))
	s.router.PathPrefix("/admin/js/").Handler(http.StripPrefix("/admin/", http.FileServer(http.FS(func() fs.FS {
		sub, _ := fs.Sub(wwwContent, "admin")
		return sub
	}()))))

	// /admin/* - protected admin SPA (everything except /admin/login and static assets)
	adminHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := fs.ReadFile(wwwContent, "admin/index.html")
		if err != nil {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})
	s.router.PathPrefix("/admin").Handler(s.adminAuthMiddleware(adminHandler))

	// Handle root path - serve index.html with dynamic values
	s.router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Read and serve index.html
		data, err := fs.ReadFile(wwwContent, "index.html")
		if err != nil {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}

		// Replace placeholders with dynamic values
		html := string(data)
		html = strings.ReplaceAll(html, "{{VERSION}}", version.GetVersion())
		html = strings.ReplaceAll(html, "{{YEAR}}", strconv.Itoa(time.Now().Year()))

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(html))
	}).Methods("GET")
}

// Handler returns the HTTP handler with CORS support
func (s *Server) Handler() http.Handler {
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Authorization", "Content-Type", "X-Tenant"},
		AllowCredentials: true,
		MaxAge:           86400,
	})

	return c.Handler(s.router)
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// loggingHandler logs incoming requests
func (s *Server) loggingHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		log.Trace("%s %s %d %v", r.Method, r.URL.Path, wrapped.statusCode, duration)
	})
}

// getTenant extracts tenant from X-Tenant header or returns default
func (s *Server) getTenant(r *http.Request) string {
	if tenant := r.Header.Get("X-Tenant"); tenant != "" {
		return tenant
	}
	return "demo"
}

// infoHandler returns API info
func (s *Server) infoHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"name":    "Velocity",
		"version": version.GetVersion(),
	})
}

// healthHandler returns the health status
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// versionHandler returns the server version
func (s *Server) versionHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"version": version.GetVersion(),
		"service": "velocity",
	})
}
