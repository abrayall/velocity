package storage

import (
	"context"
	"io"
	"time"
)

// Environment represents the deployment environment
type Environment string

const (
	Production  Environment = "production"
	Development Environment = "development"
)

// State represents the content workflow state
type State string

const (
	StateDraft   State = "draft"
	StatePending State = "pending"
	StateLive    State = "live"
)

// ValidState checks if a state string is valid
func ValidState(s string) bool {
	switch State(s) {
	case StateDraft, StatePending, StateLive:
		return true
	}
	return false
}

// ContentItem represents a stored content item
type ContentItem struct {
	Key          string
	Content      []byte // Only populated for non-streaming reads
	ContentType  string
	VersionID    string
	LastModified time.Time
	Size         int64
	ETag         string
	Metadata     map[string]string
}

// ContentStream represents a streamable content item
type ContentStream struct {
	Key          string
	Body         io.ReadCloser
	ContentType  string
	VersionID    string
	LastModified time.Time
	Size         int64
	ETag         string
	Metadata     map[string]string
}

// ContentVersion represents a specific version of content
type ContentVersion struct {
	VersionID    string
	LastModified time.Time
	Size         int64
	IsLatest     bool
}

// Schema represents a content type schema
type Schema struct {
	Name     string
	Content  []byte
	IsGlobal bool // true if global, false if tenant-specific
}

// HistoryRecord represents metadata about a version
type HistoryRecord struct {
	Version   string    `json:"version"`
	Parent    string    `json:"parent,omitempty"`
	Author    string    `json:"author,omitempty"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Size      int64     `json:"size,omitempty"`
}

// Comment represents a review comment on content in draft/pending state
type Comment struct {
	ID         string    `json:"id"`
	Author     string    `json:"author"`
	Message    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
	Resolved   bool      `json:"resolved"`
	ResolvedBy string    `json:"resolved_by,omitempty"`
	ResolvedAt time.Time `json:"resolved_at,omitempty"`
}

// Webhook represents a webhook configuration for a tenant
type Webhook struct {
	ID     string   `json:"id"`
	URL    string   `json:"url"`
	Events []string `json:"events"` // create, update, delete, publish
}

// WebhookEvent represents an event payload sent to webhooks
type WebhookEvent struct {
	Event       string `json:"event"`
	Tenant      string `json:"tenant"`
	Type        string `json:"type"`
	ID          string `json:"id"`
	Name        string `json:"name,omitempty"`
	ContentType string `json:"content-type,omitempty"`
	Timestamp   string `json:"timestamp"`
}

// Storage defines the interface for content storage backends.
// Implementations must be safe for concurrent use.
type Storage interface {
	// Connection
	CheckConnection(ctx context.Context) error

	// Content Operations
	Put(ctx context.Context, tenant, contentType, id, ext string, content []byte, mimeType string, state State) (*ContentItem, error)
	PutStream(ctx context.Context, tenant, contentType, id, ext string, body io.Reader, contentLength int64, mimeType string, state State, metadata map[string]string) (*ContentItem, error)
	Get(ctx context.Context, tenant, contentType, id, ext string, state State) (*ContentItem, error)
	GetStream(ctx context.Context, tenant, contentType, id, ext string, state State) (*ContentStream, error)
	FindContentStream(ctx context.Context, tenant, contentType, id, extHint string, state State) (*ContentStream, error)
	Delete(ctx context.Context, tenant, contentType, id, ext string, state State) error
	List(ctx context.Context, tenant, contentType string, state State) ([]*ContentItem, error)
	Exists(ctx context.Context, tenant, contentType, id, ext string, state State) (bool, error)
	Transition(ctx context.Context, tenant, contentType, id, ext string, fromState, toState State) (*ContentItem, error)

	// Versioning
	ListVersions(ctx context.Context, tenant, contentType, id, ext string) ([]*ContentVersion, error)
	GetVersion(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentItem, error)
	GetVersionStream(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentStream, error)
	RestoreVersion(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentItem, error)

	// History
	PutHistoryRecord(ctx context.Context, tenant, contentType, id string, record *HistoryRecord) error
	GetHistoryRecord(ctx context.Context, tenant, contentType, id, version string) (*HistoryRecord, error)
	ListHistoryRecords(ctx context.Context, tenant, contentType, id string) ([]*HistoryRecord, error)
	GetLatestHistoryVersion(ctx context.Context, tenant, contentType, id string) (string, error)

	// Schemas
	GetSchema(ctx context.Context, tenant, schemaName string) (*Schema, error)
	GetGlobalSchema(ctx context.Context, schemaName string) (*Schema, error)
	GetTenantSchema(ctx context.Context, tenant, schemaName string) (*Schema, error)
	PutGlobalSchema(ctx context.Context, schemaName string, content []byte) error
	PutTenantSchema(ctx context.Context, tenant, schemaName string, content []byte) error
	DeleteGlobalSchema(ctx context.Context, schemaName string) error
	DeleteTenantSchema(ctx context.Context, tenant, schemaName string) error
	ListGlobalSchemas(ctx context.Context) ([]string, error)
	ListTenantSchemas(ctx context.Context, tenant string) ([]string, error)
	ListAllSchemas(ctx context.Context, tenant string) ([]string, error)
	SchemaExists(ctx context.Context, tenant, schemaName string) (bool, error)

	// Comments
	PutComment(ctx context.Context, tenant, contentType, contentID string, state State, comment *Comment) error
	GetComment(ctx context.Context, tenant, contentType, contentID string, state State, id string) (*Comment, error)
	ListComments(ctx context.Context, tenant, contentType, contentID string, state State) ([]*Comment, error)
	DeleteComment(ctx context.Context, tenant, contentType, contentID string, state State, id string) error
	DeleteAllComments(ctx context.Context, tenant, contentType, contentID string, state State) error
	HasUnresolvedComments(ctx context.Context, tenant, contentType, contentID string, state State) (bool, error)

	// Webhooks
	ListWebhooks(ctx context.Context, tenant string) ([]*Webhook, error)
	GetWebhook(ctx context.Context, tenant, webhookID string) (*Webhook, error)
	PutWebhook(ctx context.Context, tenant string, webhook *Webhook) error
	DeleteWebhook(ctx context.Context, tenant, webhookID string) error

	// Metadata
	GetMetadata(ctx context.Context, tenant, contentType, id, ext string, state State) (map[string]string, error)
	SetMetadata(ctx context.Context, tenant, contentType, id, ext string, state State, metadata map[string]string) error
	UpdateMetadata(ctx context.Context, tenant, contentType, id, ext string, state State, updates map[string]string) error
	DeleteMetadataKeys(ctx context.Context, tenant, contentType, id, ext string, state State, keys []string) error
}
