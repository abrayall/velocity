package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"velocity/internal/log"
	"velocity/internal/models"
	"velocity/internal/storage"
)

// MIME type to extension mapping
var mimeToExt = map[string]string{
	"application/json":  "json",
	"text/html":         "html",
	"text/xml":          "xml",
	"application/xml":   "xml",
	"text/php":          "php",
	"application/x-php": "php",
	"image/png":         "png",
	"image/jpeg":        "jpg",
	"image/gif":         "gif",
	"image/webp":        "webp",
	"image/svg+xml":     "svg",
	"application/pdf":   "pdf",
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeError writes an error response
func writeError(w http.ResponseWriter, status int, err string, message string) {
	writeJSON(w, status, models.ErrorResponse{
		Error:   err,
		Message: message,
		Code:    status,
	})
}

// getExtensionFromSchema returns the file extension for a content type based on schema
func (s *Server) getExtensionFromSchema(ctx interface{}, contentType string) string {
	// Default to json for most content
	return "json"
}

// getExtensionFromMime returns the file extension from MIME type
func getExtensionFromMime(mimeType string) string {
	// Strip charset and other parameters (e.g., "text/html; charset=utf-8" -> "text/html")
	if idx := strings.Index(mimeType, ";"); idx != -1 {
		mimeType = strings.TrimSpace(mimeType[:idx])
	}
	if ext, ok := mimeToExt[mimeType]; ok {
		return ext
	}
	return "bin"
}

// extractIDAndExt extracts the content ID and extension from a storage key
func extractIDAndExt(key string, contentType string, state storage.State) (string, string) {
	// Key format: {root}/tenants/{tenant}/content/{type}/{id}.{ext}
	// or: {root}/tenants/{tenant}/content/{type}/_draft/{id}.{ext}
	filename := filepath.Base(key)
	if idx := strings.LastIndex(filename, "."); idx != -1 {
		return filename[:idx], filename[idx+1:]
	}
	return filename, ""
}

// getState extracts state from path parameter, defaults to live
func getState(r *http.Request) storage.State {
	vars := mux.Vars(r)
	stateStr := vars["state"]
	if stateStr == "" {
		return storage.StateLive
	}
	if storage.ValidState(stateStr) {
		return storage.State(stateStr)
	}
	return storage.StateLive
}

// =============================================================================
// Types/Schema Handlers
// =============================================================================

// listTypesHandler returns all available content types (from schemas)
func (s *Server) listTypesHandler(w http.ResponseWriter, r *http.Request) {
	tenant := s.getTenant(r)

	schemas, err := s.storage.ListAllSchemas(r.Context(), tenant)
	if err != nil || schemas == nil {
		schemas = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"types": schemas,
		"count": len(schemas),
	})
}

// =============================================================================
// Content Handlers
// =============================================================================

// extractMetadata extracts X-Meta-* headers into a metadata map
func extractMetadata(r *http.Request) map[string]string {
	metadata := make(map[string]string)
	for key, values := range r.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-meta-") && len(values) > 0 {
			// Strip "X-Meta-" prefix and lowercase the key
			metaKey := strings.ToLower(key[7:])
			metadata[metaKey] = values[0]
		}
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

// bulkGetHandler fetches multiple content items in parallel
func (s *Server) bulkGetHandler(w http.ResponseWriter, r *http.Request) {
	tenant := s.getTenant(r)

	// Parse request
	var req struct {
		Items []struct {
			Type        string `json:"type"`
			ID          string `json:"id"`
			Attribute   string `json:"attribute,omitempty"`   // "content" (default), "metadata", or "url"
			ContentType string `json:"content-type,omitempty"` // MIME type hint (e.g., "image/png")
		} `json:"items"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	if len(req.Items) == 0 {
		writeError(w, http.StatusBadRequest, "missing_items", "No items requested")
		return
	}

	// Result structure
	type itemResult struct {
		key    string
		result map[string]interface{}
	}

	// Fetch all items in parallel
	results := make(chan itemResult, len(req.Items))

	for _, item := range req.Items {
		go func(contentType, id, attribute, mimeHint string) {
			key := contentType + "/" + id
			res := map[string]interface{}{
				"type": contentType,
				"id":   id,
			}

			// Get extension hint from content-type MIME hint
			extHint := ""
			if mimeHint != "" {
				extHint = getExtensionFromMime(mimeHint)
				if extHint == "bin" {
					extHint = ""
				}
			}

			// If requesting URL only, just return the URL without fetching content
			if attribute == "url" {
				res["attribute"] = "url"
				res["url"] = fmt.Sprintf("/content/%s/%s/%s", tenant, contentType, id)
				results <- itemResult{key: key, result: res}
				return
			}

			// If requesting metadata only, fetch just the metadata
			if attribute == "metadata" {
				// Find the actual content file
				stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, extHint, storage.StateLive)
				if err != nil {
					res["error"] = "not_found"
					res["message"] = fmt.Sprintf("Content '%s' not found", id)
					results <- itemResult{key: key, result: res}
					return
				}
				stream.Body.Close()

				res["attribute"] = "metadata"
				res["content-type"] = stream.ContentType
				if stream.Metadata == nil {
					res["metadata"] = make(map[string]string)
				} else {
					res["metadata"] = stream.Metadata
				}
				results <- itemResult{key: key, result: res}
				return
			}

			// Default: fetch full content
			stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, extHint, storage.StateLive)
			if err != nil {
				res["error"] = "not_found"
				res["message"] = fmt.Sprintf("Content '%s' not found", id)
				results <- itemResult{key: key, result: res}
				return
			}
			defer stream.Body.Close()

			// Read content
			content, err := io.ReadAll(stream.Body)
			if err != nil {
				res["error"] = "read_error"
				res["message"] = "Failed to read content"
				results <- itemResult{key: key, result: res}
				return
			}

			res["content-type"] = stream.ContentType
			res["version"] = stream.VersionID
			res["last_modified"] = stream.LastModified.UTC().Format(time.RFC3339)

			// Include metadata if available
			if stream.Metadata != nil && len(stream.Metadata) > 0 {
				res["metadata"] = stream.Metadata
			}

			// Handle content based on type
			if strings.HasPrefix(stream.ContentType, "application/json") {
				// Parse JSON content
				var jsonContent interface{}
				if err := json.Unmarshal(content, &jsonContent); err == nil {
					res["content"] = jsonContent
				} else {
					res["content"] = string(content)
				}
			} else if strings.HasPrefix(stream.ContentType, "text/") {
				// Text content as string
				res["content"] = string(content)
			} else {
				// Binary content as base64
				res["content"] = "base64:" + base64.StdEncoding.EncodeToString(content)
			}

			results <- itemResult{key: key, result: res}
		}(item.Type, item.ID, item.Attribute, item.ContentType)
	}

	// Collect results
	items := make(map[string]interface{})
	errorCount := 0

	for i := 0; i < len(req.Items); i++ {
		result := <-results
		items[result.key] = result.result
		if _, hasError := result.result["error"]; hasError {
			errorCount++
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items":  items,
		"count":  len(req.Items),
		"errors": errorCount,
	})
}

// listContentHandler lists all content of a type for a tenant
func (s *Server) listContentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	stateOrID := vars["id"] // Could be state or empty

	tenant := s.getTenant(r)

	// Check if second param is a state
	state := storage.StateLive
	if stateOrID != "" && storage.ValidState(stateOrID) {
		state = storage.State(stateOrID)
	}

	items, err := s.storage.List(r.Context(), tenant, contentType, state)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	// Ensure non-nil slice
	if items == nil {
		items = []*storage.ContentItem{}
	}

	// Convert to response format
	responseItems := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		// Extract ID from key
		parts := strings.Split(item.Key, "/")
		filename := parts[len(parts)-1]
		id := strings.TrimSuffix(filename, filepath.Ext(filename))

		responseItems = append(responseItems, map[string]interface{}{
			"id":            id,
			"last_modified": item.LastModified,
			"size":          item.Size,
		})
	}

	writeJSON(w, http.StatusOK, models.ListResponse{
		Items: func() []interface{} {
			result := make([]interface{}, len(responseItems))
			for i, v := range responseItems {
				result[i] = v
			}
			return result
		}(),
		Count: len(responseItems),
	})
}

// createContentHandler creates new content (handles both JSON and file uploads)
func (s *Server) createContentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_id", "Content ID is required in URL path")
		return
	}

	tenant := s.getTenant(r)
	state := getState(r)

	// Extract metadata from X-Meta-* headers
	metadata := extractMetadata(r)

	// Check if ID already has an extension
	if idx := strings.LastIndex(id, "."); idx != -1 && idx < len(id)-1 {
		ext := id[idx+1:]
		id = id[:idx]
		// Use extension from ID, get mime type from that
		mimeType := r.Header.Get("Content-Type")
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		contentLength := r.ContentLength
		body := r.Body
		defer r.Body.Close()

		item, err := s.storage.PutStream(r.Context(), tenant, contentType, id, ext, body, contentLength, mimeType, state, metadata)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
			return
		}

		log.Debug("Created content: %s (%s, %d bytes)", item.Key, mimeType, item.Size)
		s.triggerWebhooks(tenant, "create", contentType, id, mimeType)

		writeJSON(w, http.StatusCreated, map[string]interface{}{
			"id":      id,
			"state":   string(state),
			"version": item.VersionID,
			"message": "Content created successfully",
		})
		return
	}

	var body io.Reader
	var contentLength int64
	var mimeType string
	var ext string

	// Check if this is a multipart form upload
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		// For multipart, we need to parse to get the file
		// Use MaxBytesReader to limit memory usage
		r.Body = http.MaxBytesReader(w, r.Body, 10<<30) // 10GB max

		mr, err := r.MultipartReader()
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_form", "Failed to parse multipart form")
			return
		}

		part, err := mr.NextPart()
		if err != nil {
			writeError(w, http.StatusBadRequest, "missing_file", "No file provided")
			return
		}

		mimeType = part.Header.Get("Content-Type")
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		ext = getExtensionFromMime(mimeType)

		// Content-Length not available for multipart parts, use -1 for unknown
		contentLength = -1
		body = part
		defer part.Close()
	} else {
		// Stream body directly
		contentLength = r.ContentLength
		mimeType = r.Header.Get("Content-Type")
		if mimeType == "" {
			mimeType = "application/json"
		}
		ext = getExtensionFromMime(mimeType)
		body = r.Body
		defer r.Body.Close()
	}

	// Store content via streaming
	item, err := s.storage.PutStream(r.Context(), tenant, contentType, id, ext, body, contentLength, mimeType, state, metadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	log.Debug("Created content: %s (%s, %d bytes)", item.Key, mimeType, item.Size)

	// Trigger webhooks
	s.triggerWebhooks(tenant, "create", contentType, id, mimeType)

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":      id,
		"state":   string(state),
		"version": item.VersionID,
		"message": "Content created successfully",
	})
}

// getOrListContentHandler routes to list or get based on whether id is a state
func (s *Server) getOrListContentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	// If id is a valid state, this is a list request
	if storage.ValidState(id) {
		s.listContentHandler(w, r)
		return
	}

	// Otherwise it's a get request
	s.getContentHandler(w, r)
}

// getContentHandler gets content by ID
func (s *Server) getContentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	// Check for attribute query param: "content" (default), "metadata", or "url"
	attribute := r.URL.Query().Get("attribute")

	// Get extension hint from Accept header
	extHint := ""
	if accept := r.Header.Get("Accept"); accept != "" {
		extHint = getExtensionFromMime(accept)
		if extHint == "bin" {
			extHint = "" // Don't use "bin" as a hint
		}
	}

	// If requesting URL only, return JSON with the URL
	if attribute == "url" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"id":        id,
			"attribute": "url",
			"url":       fmt.Sprintf("/content/%s/%s/%s", tenant, contentType, id),
		})
		return
	}

	// If requesting metadata only, return JSON with metadata
	if attribute == "metadata" {
		stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, extHint, state)
		if err != nil {
			writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Content '%s' not found", id))
			return
		}
		stream.Body.Close()

		metadata := stream.Metadata
		if metadata == nil {
			metadata = make(map[string]string)
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"id":           id,
			"attribute":    "metadata",
			"content-type": stream.ContentType,
			"metadata":     metadata,
		})
		return
	}

	// Default: return full content
	stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, extHint, state)
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Content '%s' not found", id))
			return
		}
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}
	defer stream.Body.Close()

	// Check conditional request headers for caching
	if checkNotModified(r, stream.ETag, stream.LastModified) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Set caching headers
	w.Header().Set("ETag", stream.ETag)
	w.Header().Set("Last-Modified", stream.LastModified.UTC().Format(time.RFC1123))
	w.Header().Set("Cache-Control", "public, max-age=60, must-revalidate")

	// Set content headers
	if stream.VersionID != "" {
		w.Header().Set("X-Version-ID", stream.VersionID)
	}
	w.Header().Set("X-Content-State", string(state))
	w.Header().Set("Content-Type", stream.ContentType)
	if stream.Size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", stream.Size))
	}

	// Stream content directly to response
	w.WriteHeader(http.StatusOK)
	io.Copy(w, stream.Body)
}

// directContentHandler serves content directly via /content/{tenant}/{type}/{id}
func (s *Server) directContentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tenant := vars["tenant"]
	contentType := vars["type"]
	id := vars["id"]

	// Get extension hint from Accept header
	extHint := ""
	if accept := r.Header.Get("Accept"); accept != "" {
		extHint = getExtensionFromMime(accept)
		if extHint == "bin" {
			extHint = ""
		}
	}

	stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, extHint, storage.StateLive)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Content '%s' not found", id))
		return
	}
	defer stream.Body.Close()

	// Check conditional request headers for caching
	if checkNotModified(r, stream.ETag, stream.LastModified) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Set caching headers
	w.Header().Set("ETag", stream.ETag)
	w.Header().Set("Last-Modified", stream.LastModified.UTC().Format(time.RFC1123))
	w.Header().Set("Cache-Control", "public, max-age=3600, must-revalidate")

	// Set content headers
	w.Header().Set("Content-Type", stream.ContentType)
	if stream.Size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", stream.Size))
	}

	// Stream content directly to response
	w.WriteHeader(http.StatusOK)
	io.Copy(w, stream.Body)
}

// checkNotModified checks If-None-Match and If-Modified-Since headers
func checkNotModified(r *http.Request, etag string, lastModified time.Time) bool {
	// Check If-None-Match (ETag)
	if match := r.Header.Get("If-None-Match"); match != "" {
		if match == etag || match == "*" {
			return true
		}
	}

	// Check If-Modified-Since
	if since := r.Header.Get("If-Modified-Since"); since != "" {
		sinceTime, err := time.Parse(time.RFC1123, since)
		if err == nil && !lastModified.After(sinceTime) {
			return true
		}
	}

	return false
}

// updateContentHandler updates existing content
func (s *Server) updateContentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	// Extract metadata from X-Meta-* headers (nil if not provided means keep existing)
	metadata := extractMetadata(r)

	// Check if ID already has an extension
	var ext string
	if idx := strings.LastIndex(id, "."); idx != -1 && idx < len(id)-1 {
		ext = id[idx+1:]
		id = id[:idx]
	} else {
		ext = getExtensionFromMime(r.Header.Get("Content-Type"))
	}

	contentLength := r.ContentLength
	mimeType := r.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/json"
	}

	// Store content via streaming (S3 versioning handles the update for live content)
	item, err := s.storage.PutStream(r.Context(), tenant, contentType, id, ext, r.Body, contentLength, mimeType, state, metadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}
	defer r.Body.Close()

	log.Debug("Updated content: %s (%d bytes)", item.Key, item.Size)

	// Trigger webhooks
	s.triggerWebhooks(tenant, "update", contentType, id, mimeType)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      id,
		"state":   string(state),
		"version": item.VersionID,
		"message": "Content updated successfully",
	})
}

// deleteContentHandler deletes content (creates delete marker with versioning for live)
func (s *Server) deleteContentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	ext := s.getExtensionFromSchema(r.Context(), contentType)

	err := s.storage.Delete(r.Context(), tenant, contentType, id, ext, state)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	log.Debug("Deleted content: %s/%s/%s.%s (state: %s)", tenant, contentType, id, ext, state)

	// Trigger webhooks
	s.triggerWebhooks(tenant, "delete", contentType, id, "")

	msg := "Content deleted successfully"
	if state == storage.StateLive {
		msg = "Content deleted successfully (versioning preserves history)"
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      id,
		"state":   string(state),
		"message": msg,
	})
}

// transitionHandler moves content from one state to another
func (s *Server) transitionHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)

	// Parse request body
	var req struct {
		From    string `json:"from"`
		To      string `json:"to"`
		Author  string `json:"author,omitempty"`
		Message string `json:"message,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	if !storage.ValidState(req.From) {
		writeError(w, http.StatusBadRequest, "invalid_state", fmt.Sprintf("Invalid 'from' state: %s", req.From))
		return
	}
	if !storage.ValidState(req.To) {
		writeError(w, http.StatusBadRequest, "invalid_state", fmt.Sprintf("Invalid 'to' state: %s", req.To))
		return
	}

	fromState := storage.State(req.From)
	toState := storage.State(req.To)

	ext := s.getExtensionFromSchema(r.Context(), contentType)

	// Get parent version before transition (for history)
	parentVersion, _ := s.storage.GetLatestHistoryVersion(r.Context(), tenant, contentType, id)

	item, err := s.storage.Transition(r.Context(), tenant, contentType, id, ext, fromState, toState)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "transition_error", err.Error())
		return
	}

	msg := fmt.Sprintf("Content transitioned from %s to %s", req.From, req.To)
	if toState == storage.StateLive {
		msg = fmt.Sprintf("Content published (transitioned from %s to live)", req.From)

		// Create history record when publishing to live
		record := &storage.HistoryRecord{
			Version:   item.VersionID,
			Parent:    parentVersion,
			Author:    req.Author,
			Message:   req.Message,
			Timestamp: item.LastModified,
			Size:      item.Size,
		}
		if record.Timestamp.IsZero() {
			record.Timestamp = time.Now()
		}

		if err := s.storage.PutHistoryRecord(r.Context(), tenant, contentType, id, record); err != nil {
			// Log but don't fail the transition
			log.Error("Failed to create history record: %v", err)
		}

		// Trigger publish webhook
		s.triggerWebhooks(tenant, "publish", contentType, id, item.ContentType)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      id,
		"from":    req.From,
		"to":      req.To,
		"version": item.VersionID,
		"message": msg,
	})
}

// =============================================================================
// Version Handlers
// =============================================================================

// listVersionsHandler lists all versions of content
func (s *Server) listVersionsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)

	ext := s.getExtensionFromSchema(r.Context(), contentType)

	versions, err := s.storage.ListVersions(r.Context(), tenant, contentType, id, ext)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}
	if versions == nil {
		versions = []*storage.ContentVersion{}
	}

	responseVersions := make([]models.Version, 0, len(versions))
	for _, v := range versions {
		responseVersions = append(responseVersions, models.Version{
			VersionID:    v.VersionID,
			LastModified: v.LastModified,
			Size:         v.Size,
			IsLatest:     v.IsLatest,
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":       id,
		"versions": responseVersions,
		"count":    len(responseVersions),
	})
}

// getVersionHandler gets a specific version of content
func (s *Server) getVersionHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]
	versionID := vars["version"]

	tenant := s.getTenant(r)

	ext := s.getExtensionFromSchema(r.Context(), contentType)

	stream, err := s.storage.GetVersionStream(r.Context(), tenant, contentType, id, ext, versionID)
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Version '%s' not found", versionID))
			return
		}
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}
	defer stream.Body.Close()

	// Check conditional request headers for caching
	if checkNotModified(r, stream.ETag, stream.LastModified) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Set caching headers - versions are immutable, cache longer
	w.Header().Set("ETag", stream.ETag)
	w.Header().Set("Last-Modified", stream.LastModified.UTC().Format(time.RFC1123))
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")

	// Set content headers
	w.Header().Set("X-Version-ID", stream.VersionID)
	w.Header().Set("Content-Type", stream.ContentType)
	if stream.Size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", stream.Size))
	}

	// Stream content directly to response
	w.WriteHeader(http.StatusOK)
	io.Copy(w, stream.Body)
}

// restoreVersionHandler restores a specific version as the current version
func (s *Server) restoreVersionHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]
	versionID := vars["version"]

	tenant := s.getTenant(r)

	ext := s.getExtensionFromSchema(r.Context(), contentType)

	item, err := s.storage.RestoreVersion(r.Context(), tenant, contentType, id, ext, versionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":               id,
		"restored_version": versionID,
		"new_version":   item.VersionID,
		"message":          "Version restored successfully",
	})
}

// =============================================================================
// History Handlers
// =============================================================================

// listHistoryHandler lists history records for a content item
func (s *Server) listHistoryHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)

	records, err := s.storage.ListHistoryRecords(r.Context(), tenant, contentType, id)
	if err != nil || records == nil {
		records = []*storage.HistoryRecord{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      id,
		"history": records,
		"count":   len(records),
	})
}

// getHistoryHandler gets a specific history record
func (s *Server) getHistoryHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]
	version := vars["version"]

	tenant := s.getTenant(r)

	record, err := s.storage.GetHistoryRecord(r.Context(), tenant, contentType, id, version)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("History record for version '%s' not found", version))
		return
	}

	writeJSON(w, http.StatusOK, record)
}

// diffHandler computes diff between two versions
func (s *Server) diffHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	fromVersion := r.URL.Query().Get("from")
	toVersion := r.URL.Query().Get("to")

	if fromVersion == "" || toVersion == "" {
		writeError(w, http.StatusBadRequest, "missing_params", "Both 'from' and 'to' query params required")
		return
	}

	tenant := s.getTenant(r)
	ext := s.getExtensionFromSchema(r.Context(), contentType)

	// Get both versions
	fromItem, err := s.storage.GetVersion(r.Context(), tenant, contentType, id, ext, fromVersion)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Version '%s' not found", fromVersion))
		return
	}

	toItem, err := s.storage.GetVersion(r.Context(), tenant, contentType, id, ext, toVersion)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Version '%s' not found", toVersion))
		return
	}

	// Determine content type and compute appropriate diff
	diff := computeContentDiff(fromItem.Content, toItem.Content, fromItem.ContentType)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":           id,
		"from":         fromVersion,
		"to":           toVersion,
		"content_type": fromItem.ContentType,
		"diff":         diff,
	})
}

// computeContentDiff determines content type and computes appropriate diff
func computeContentDiff(from, to []byte, contentType string) map[string]interface{} {
	// Check if it's JSON
	if isJSONContent(contentType) {
		var fromData, toData map[string]interface{}
		if err := json.Unmarshal(from, &fromData); err == nil {
			if err := json.Unmarshal(to, &toData); err == nil {
				return map[string]interface{}{
					"type":    "json",
					"changes": computeJSONDiff(fromData, toData),
				}
			}
		}
	}

	// Check if it's text content
	if isTextContent(contentType) {
		return map[string]interface{}{
			"type":    "text",
			"changes": computeLineDiff(string(from), string(to)),
		}
	}

	// Binary content - just show metadata
	return map[string]interface{}{
		"type": "binary",
		"changes": map[string]interface{}{
			"from_size": len(from),
			"to_size":   len(to),
			"size_diff": len(to) - len(from),
		},
	}
}

// isJSONContent checks if content type is JSON
func isJSONContent(contentType string) bool {
	return strings.Contains(contentType, "json")
}

// isTextContent checks if content type is text-based
func isTextContent(contentType string) bool {
	textTypes := []string{
		"text/",
		"application/xml",
		"application/xhtml",
		"application/javascript",
		"application/x-javascript",
	}
	for _, t := range textTypes {
		if strings.Contains(contentType, t) {
			return true
		}
	}
	return false
}

// computeJSONDiff computes a field-level diff between two JSON objects
func computeJSONDiff(from, to map[string]interface{}) map[string]interface{} {
	diff := map[string]interface{}{
		"added":   map[string]interface{}{},
		"removed": map[string]interface{}{},
		"changed": map[string]interface{}{},
	}

	added := diff["added"].(map[string]interface{})
	removed := diff["removed"].(map[string]interface{})
	changed := diff["changed"].(map[string]interface{})

	// Find removed and changed
	for key, fromVal := range from {
		if toVal, exists := to[key]; exists {
			if !jsonEqual(fromVal, toVal) {
				changed[key] = map[string]interface{}{
					"from": fromVal,
					"to":   toVal,
				}
			}
		} else {
			removed[key] = fromVal
		}
	}

	// Find added
	for key, toVal := range to {
		if _, exists := from[key]; !exists {
			added[key] = toVal
		}
	}

	return diff
}

// computeLineDiff computes a line-by-line diff for text content
func computeLineDiff(from, to string) map[string]interface{} {
	fromLines := strings.Split(from, "\n")
	toLines := strings.Split(to, "\n")

	// Simple diff: find added and removed lines
	fromSet := make(map[string]int)
	toSet := make(map[string]int)

	for i, line := range fromLines {
		fromSet[line] = i + 1
	}
	for i, line := range toLines {
		toSet[line] = i + 1
	}

	var added, removed []map[string]interface{}

	for line, lineNum := range fromSet {
		if _, exists := toSet[line]; !exists {
			removed = append(removed, map[string]interface{}{
				"line":    lineNum,
				"content": line,
			})
		}
	}

	for line, lineNum := range toSet {
		if _, exists := fromSet[line]; !exists {
			added = append(added, map[string]interface{}{
				"line":    lineNum,
				"content": line,
			})
		}
	}

	return map[string]interface{}{
		"from_lines": len(fromLines),
		"to_lines":   len(toLines),
		"added":      added,
		"removed":    removed,
	}
}

// jsonEqual compares two JSON values for equality
func jsonEqual(a, b interface{}) bool {
	aBytes, _ := json.Marshal(a)
	bBytes, _ := json.Marshal(b)
	return string(aBytes) == string(bBytes)
}

// =============================================================================
// Schema Handlers
// =============================================================================

// listGlobalSchemasHandler lists all global schemas
func (s *Server) listGlobalSchemasHandler(w http.ResponseWriter, r *http.Request) {
	schemas, err := s.storage.ListGlobalSchemas(r.Context())
	if err != nil || schemas == nil {
		schemas = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"schemas": schemas,
		"count":   len(schemas),
	})
}

// getGlobalSchemaHandler gets a global schema
func (s *Server) getGlobalSchemaHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	schema, err := s.storage.GetGlobalSchema(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Schema '%s' not found", name))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(schema.Content)
}

// putGlobalSchemaHandler creates or updates a global schema
func (s *Server) putGlobalSchemaHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "Failed to read request body")
		return
	}
	defer r.Body.Close()

	// Validate JSON
	var schema models.Schema
	if err := json.Unmarshal(body, &schema); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON schema")
		return
	}

	if err := s.storage.PutGlobalSchema(r.Context(), name, body); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":    name,
		"message": "Schema saved successfully",
	})
}

// deleteGlobalSchemaHandler deletes a global schema
func (s *Server) deleteGlobalSchemaHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	if err := s.storage.DeleteGlobalSchema(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":    name,
		"message": "Schema deleted successfully",
	})
}

// listTenantSchemasHandler lists tenant-specific schemas
func (s *Server) listTenantSchemasHandler(w http.ResponseWriter, r *http.Request) {
	tenant := s.getTenant(r)

	schemas, err := s.storage.ListTenantSchemas(r.Context(), tenant)
	if err != nil || schemas == nil {
		schemas = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"schemas": schemas,
		"count":   len(schemas),
	})
}

// getTenantSchemaHandler gets a tenant-specific schema
func (s *Server) getTenantSchemaHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	tenant := s.getTenant(r)

	schema, err := s.storage.GetTenantSchema(r.Context(), tenant, name)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Tenant schema '%s' not found", name))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(schema.Content)
}

// putTenantSchemaHandler creates or updates a tenant-specific schema
func (s *Server) putTenantSchemaHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	tenant := s.getTenant(r)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "Failed to read request body")
		return
	}
	defer r.Body.Close()

	// Validate JSON
	var schema models.Schema
	if err := json.Unmarshal(body, &schema); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON schema")
		return
	}

	if err := s.storage.PutTenantSchema(r.Context(), tenant, name, body); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":    name,
		"tenant":  tenant,
		"message": "Tenant schema saved successfully",
	})
}

// deleteTenantSchemaHandler deletes a tenant-specific schema
func (s *Server) deleteTenantSchemaHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	tenant := s.getTenant(r)

	if err := s.storage.DeleteTenantSchema(r.Context(), tenant, name); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":    name,
		"tenant":  tenant,
		"message": "Tenant schema deleted successfully",
	})
}

// =============================================================================
// Comment Handlers
// =============================================================================

// listCommentsHandler lists all comments for a content item in a state
func (s *Server) listCommentsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	contentID := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	if state == storage.StateLive {
		writeError(w, http.StatusBadRequest, "invalid_state", "Comments are only available on draft or pending content")
		return
	}

	comments, err := s.storage.ListComments(r.Context(), tenant, contentType, contentID, state)
	if err != nil || comments == nil {
		comments = []*storage.Comment{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"content_id": contentID,
		"state":      string(state),
		"comments":   comments,
		"count":      len(comments),
	})
}

// createCommentHandler creates a new comment on content
func (s *Server) createCommentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	contentID := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	if state == storage.StateLive {
		writeError(w, http.StatusBadRequest, "invalid_state", "Comments are only allowed on draft or pending content")
		return
	}

	var req struct {
		Author  string `json:"author"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	if req.Message == "" {
		writeError(w, http.StatusBadRequest, "missing_message", "Comment message is required")
		return
	}

	comment := &storage.Comment{
		ID:        uuid.New().String(),
		Author:    req.Author,
		Message:   req.Message,
		CreatedAt: time.Now(),
		Resolved:  false,
	}

	if err := s.storage.PutComment(r.Context(), tenant, contentType, contentID, state, comment); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, comment)
}

// getCommentHandler gets a specific comment
func (s *Server) getCommentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	contentID := vars["id"]
	commentID := vars["comment_id"]

	tenant := s.getTenant(r)
	state := getState(r)

	comment, err := s.storage.GetComment(r.Context(), tenant, contentType, contentID, state, commentID)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Comment '%s' not found", commentID))
		return
	}

	writeJSON(w, http.StatusOK, comment)
}

// updateCommentHandler updates a comment (primarily for resolving)
func (s *Server) updateCommentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	contentID := vars["id"]
	commentID := vars["comment_id"]

	tenant := s.getTenant(r)
	state := getState(r)

	// Get existing comment
	comment, err := s.storage.GetComment(r.Context(), tenant, contentType, contentID, state, commentID)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Comment '%s' not found", commentID))
		return
	}

	var req struct {
		Resolved   *bool  `json:"resolved,omitempty"`
		ResolvedBy string `json:"resolved_by,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	// Update resolved status
	if req.Resolved != nil {
		comment.Resolved = *req.Resolved
		if *req.Resolved {
			comment.ResolvedBy = req.ResolvedBy
			comment.ResolvedAt = time.Now()
		} else {
			comment.ResolvedBy = ""
			comment.ResolvedAt = time.Time{}
		}
	}

	if err := s.storage.PutComment(r.Context(), tenant, contentType, contentID, state, comment); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, comment)
}

// deleteCommentHandler deletes a comment
func (s *Server) deleteCommentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	contentID := vars["id"]
	commentID := vars["comment_id"]

	tenant := s.getTenant(r)
	state := getState(r)

	if err := s.storage.DeleteComment(r.Context(), tenant, contentType, contentID, state, commentID); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      commentID,
		"message": "Comment deleted successfully",
	})
}

// =============================================================================
// Upload Handler
// =============================================================================

// uploadHandler handles binary file uploads (images, files)
func (s *Server) uploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	// Parse multipart form (max 32MB)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_form", "Failed to parse multipart form")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing_file", "No file provided")
		return
	}
	defer file.Close()

	// Read file content
	content, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read_error", "Failed to read file")
		return
	}

	// Determine MIME type and extension
	mimeType := header.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = http.DetectContentType(content)
	}

	ext := getExtensionFromMime(mimeType)

	// Store content
	item, err := s.storage.Put(r.Context(), tenant, contentType, id, ext, content, mimeType, state)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":         id,
		"state":      string(state),
		"version": item.VersionID,
		"key":        item.Key,
		"filename":   header.Filename,
		"mime_type":  mimeType,
		"size":       len(content),
		"message":    "File uploaded successfully",
	})
}

// =============================================================================
// Webhook Trigger
// =============================================================================

// triggerWebhooks fires webhooks asynchronously for a content event
func (s *Server) triggerWebhooks(tenant, event, contentType, id, mimeType string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		webhooks, err := s.storage.ListWebhooks(ctx, tenant)
		if err != nil || len(webhooks) == 0 {
			return
		}

		payload := storage.WebhookEvent{
			Event:       event,
			Tenant:      tenant,
			Type:        contentType,
			ID:          id,
			ContentType: mimeType,
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
		}

		jsonPayload, err := json.Marshal(payload)
		if err != nil {
			log.Error("Failed to marshal webhook payload: %v", err)
			return
		}

		client := &http.Client{Timeout: 10 * time.Second}

		for _, webhook := range webhooks {
			// Check if webhook is subscribed to this event
			subscribed := false
			for _, e := range webhook.Events {
				if e == event {
					subscribed = true
					break
				}
			}
			if !subscribed {
				continue
			}

			// Fire webhook
			go func(url string) {
				resp, err := client.Post(url, "application/json", bytes.NewReader(jsonPayload))
				if err != nil {
					log.Debug("Webhook failed for %s: %v", url, err)
					return
				}
				defer resp.Body.Close()
				log.Debug("Webhook sent to %s: %d", url, resp.StatusCode)
			}(webhook.URL)
		}
	}()
}

// =============================================================================
// Webhook Handlers
// =============================================================================

// listWebhooksHandler lists all webhooks for a tenant
func (s *Server) listWebhooksHandler(w http.ResponseWriter, r *http.Request) {
	tenant := s.getTenant(r)

	webhooks, err := s.storage.ListWebhooks(r.Context(), tenant)
	if err != nil || webhooks == nil {
		webhooks = []*storage.Webhook{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"webhooks": webhooks,
		"count":    len(webhooks),
	})
}

// getWebhookHandler gets a webhook by ID
func (s *Server) getWebhookHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	webhookID := vars["id"]
	tenant := s.getTenant(r)

	webhook, err := s.storage.GetWebhook(r.Context(), tenant, webhookID)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Webhook '%s' not found", webhookID))
		return
	}

	writeJSON(w, http.StatusOK, webhook)
}

// putWebhookHandler creates or updates a webhook
func (s *Server) putWebhookHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	webhookID := vars["id"]
	tenant := s.getTenant(r)

	var req struct {
		URL    string   `json:"url"`
		Events []string `json:"events"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	if req.URL == "" {
		writeError(w, http.StatusBadRequest, "missing_url", "Webhook URL is required")
		return
	}

	webhook := &storage.Webhook{
		ID:     webhookID,
		URL:    req.URL,
		Events: req.Events,
	}

	// Default to all events if none specified
	if len(webhook.Events) == 0 {
		webhook.Events = []string{"create", "update", "delete", "publish"}
	}

	if err := s.storage.PutWebhook(r.Context(), tenant, webhook); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      webhookID,
		"url":     req.URL,
		"events":  webhook.Events,
		"message": "Webhook saved successfully",
	})
}

// deleteWebhookHandler deletes a webhook
func (s *Server) deleteWebhookHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	webhookID := vars["id"]
	tenant := s.getTenant(r)

	if err := s.storage.DeleteWebhook(r.Context(), tenant, webhookID); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":      webhookID,
		"message": "Webhook deleted successfully",
	})
}

// =============================================================================
// Metadata Handlers
// =============================================================================

// getMetadataHandler gets metadata only for a content item
func (s *Server) getMetadataHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	// Find the actual content file
	stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, "", state)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Content '%s' not found", id))
		return
	}
	stream.Body.Close()

	if stream.Metadata == nil {
		writeJSON(w, http.StatusOK, map[string]string{})
		return
	}

	writeJSON(w, http.StatusOK, stream.Metadata)
}

// setMetadataHandler replaces all metadata on a content item
func (s *Server) setMetadataHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	var metadata map[string]string
	if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	// Find the actual content file
	stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, "", state)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Content '%s' not found", id))
		return
	}
	stream.Body.Close()

	// Extract id and ext from the found key
	foundID, ext := extractIDAndExt(stream.Key, contentType, state)

	if err := s.storage.SetMetadata(r.Context(), tenant, contentType, foundID, ext, state, metadata); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":       id,
		"metadata": metadata,
		"message":  "Metadata updated successfully",
	})
}

// updateMetadataHandler merges new metadata with existing
func (s *Server) updateMetadataHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	var updates map[string]string
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	// Find the actual content file
	stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, "", state)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Content '%s' not found", id))
		return
	}
	stream.Body.Close()

	// Extract id and ext from the found key
	foundID, ext := extractIDAndExt(stream.Key, contentType, state)

	if err := s.storage.UpdateMetadata(r.Context(), tenant, contentType, foundID, ext, state, updates); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	// Get updated metadata to return
	metadata, _ := s.storage.GetMetadata(r.Context(), tenant, contentType, foundID, ext, state)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":       id,
		"metadata": metadata,
		"message":  "Metadata updated successfully",
	})
}

// deleteMetadataHandler removes specific metadata keys
func (s *Server) deleteMetadataHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contentType := vars["type"]
	id := vars["id"]

	tenant := s.getTenant(r)
	state := getState(r)

	var req struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid JSON body")
		return
	}

	if len(req.Keys) == 0 {
		writeError(w, http.StatusBadRequest, "missing_keys", "No keys specified to delete")
		return
	}

	// Find the actual content file
	stream, err := s.storage.FindContentStream(r.Context(), tenant, contentType, id, "", state)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Content '%s' not found", id))
		return
	}
	stream.Body.Close()

	// Extract id and ext from the found key
	foundID, ext := extractIDAndExt(stream.Key, contentType, state)

	if err := s.storage.DeleteMetadataKeys(r.Context(), tenant, contentType, foundID, ext, state, req.Keys); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}

	// Get updated metadata to return
	metadata, _ := s.storage.GetMetadata(r.Context(), tenant, contentType, foundID, ext, state)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":       id,
		"metadata": metadata,
		"message":  "Metadata keys deleted successfully",
	})
}
