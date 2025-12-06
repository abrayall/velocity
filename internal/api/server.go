package api

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/cors"

	"velocity/internal/log"
	"velocity/internal/storage"
	"velocity/internal/version"
)

// Server represents the API server
type Server struct {
	router  *mux.Router
	storage *storage.Client
	config  *ServerConfig
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port string
}

// NewServer creates a new API server
func NewServer(storageClient *storage.Client, config *ServerConfig) *Server {
	s := &Server{
		router:  mux.NewRouter(),
		storage: storageClient,
		config:  config,
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
	s.router.HandleFunc("/content/{tenant}/{type}/{id}", s.directContentHandler).Methods("GET")

	api := s.router.PathPrefix("/api").Subrouter()

	// Add request logging
	api.Use(s.loggingHandler)

	// Utility endpoints
	// GET    /api                   - API info (name, version)
	// GET    /api/health            - Health check
	// GET    /api/version           - Server version
	// GET    /api/types             - List available content types
	api.HandleFunc("", s.infoHandler).Methods("GET")
	api.HandleFunc("/health", s.healthHandler).Methods("GET")
	api.HandleFunc("/version", s.versionHandler).Methods("GET")
	api.HandleFunc("/types", s.listTypesHandler).Methods("GET")

	// Content routes
	// GET    /api/content/{type}                 - List all live items
	// GET    /api/content/{type}/draft           - List all draft items
	// GET    /api/content/{type}/pending         - List all pending items
	// POST   /api/content/{type}/{id}            - Create new item
	// GET    /api/content/{type}/{id}            - Get live item
	// GET    /api/content/{type}/{id}/draft      - Get draft item
	// GET    /api/content/{type}/{id}/pending    - Get pending item
	// PUT    /api/content/{type}/{id}            - Update live item
	// PUT    /api/content/{type}/{id}/{state}    - Update item in state
	// DELETE /api/content/{type}/{id}            - Delete live item
	// DELETE /api/content/{type}/{id}/{state}    - Delete item in state

	// Bulk get
	// POST   /api/content                        - Bulk get multiple items
	api.HandleFunc("/content", s.bulkGetHandler).Methods("POST")

	api.HandleFunc("/content/{type}", s.listContentHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}", s.getOrListContentHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}", s.createContentHandler).Methods("POST")
	api.HandleFunc("/content/{type}/{id}", s.updateContentHandler).Methods("PUT")
	api.HandleFunc("/content/{type}/{id}", s.deleteContentHandler).Methods("DELETE")

	// State-specific content routes
	api.HandleFunc("/content/{type}/{id}/{state}", s.getContentHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}/{state}", s.createContentHandler).Methods("POST")
	api.HandleFunc("/content/{type}/{id}/{state}", s.updateContentHandler).Methods("PUT")
	api.HandleFunc("/content/{type}/{id}/{state}", s.deleteContentHandler).Methods("DELETE")

	// State transition route
	// POST   /api/content/{type}/{id}/transition - Move content between states (draft->pending->live)
	api.HandleFunc("/content/{type}/{id}/transition", s.transitionHandler).Methods("POST")

	// Version routes
	// GET    /api/content/{type}/{id}/versions           - List all versions
	// GET    /api/content/{type}/{id}/versions/{version} - Get specific version
	// POST   /api/content/{type}/{id}/versions/{version}/restore - Restore version

	api.HandleFunc("/content/{type}/{id}/versions", s.listVersionsHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}/versions/{version}", s.getVersionHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}/versions/{version}/restore", s.restoreVersionHandler).Methods("POST")

	// History routes
	// GET    /api/content/{type}/{id}/history           - List history records
	// GET    /api/content/{type}/{id}/history/{version} - Get specific history record
	// GET    /api/content/{type}/{id}/diff?from=v1&to=v2 - Compute diff between versions

	api.HandleFunc("/content/{type}/{id}/history", s.listHistoryHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}/history/{version}", s.getHistoryHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}/diff", s.diffHandler).Methods("GET")

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

	// Comment routes (only on draft/pending content)
	// GET    /api/content/{type}/{id}/{state}/comments              - List comments
	// POST   /api/content/{type}/{id}/{state}/comments              - Create comment
	// GET    /api/content/{type}/{id}/{state}/comments/{comment_id} - Get comment
	// PUT    /api/content/{type}/{id}/{state}/comments/{comment_id} - Update/resolve comment
	// DELETE /api/content/{type}/{id}/{state}/comments/{comment_id} - Delete comment

	api.HandleFunc("/content/{type}/{id}/{state}/comments", s.listCommentsHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}/{state}/comments", s.createCommentHandler).Methods("POST")
	api.HandleFunc("/content/{type}/{id}/{state}/comments/{comment_id}", s.getCommentHandler).Methods("GET")
	api.HandleFunc("/content/{type}/{id}/{state}/comments/{comment_id}", s.updateCommentHandler).Methods("PUT")
	api.HandleFunc("/content/{type}/{id}/{state}/comments/{comment_id}", s.deleteCommentHandler).Methods("DELETE")

	// Webhook routes
	// GET    /api/webhooks              - List webhooks for tenant
	// GET    /api/webhooks/{id}         - Get webhook
	// PUT    /api/webhooks/{id}         - Create/update webhook
	// DELETE /api/webhooks/{id}         - Delete webhook

	api.HandleFunc("/webhooks", s.listWebhooksHandler).Methods("GET")
	api.HandleFunc("/webhooks/{id}", s.getWebhookHandler).Methods("GET")
	api.HandleFunc("/webhooks/{id}", s.putWebhookHandler).Methods("PUT")
	api.HandleFunc("/webhooks/{id}", s.deleteWebhookHandler).Methods("DELETE")
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
