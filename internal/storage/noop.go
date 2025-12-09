package storage

import (
	"context"
	"errors"
	"io"
)

// ErrStorageNotConfigured is returned when storage operations are attempted without configuration
var ErrStorageNotConfigured = errors.New("storage not configured")

// NoopStorage is a storage implementation that returns errors for all operations.
// Used when running without S3 credentials configured.
type NoopStorage struct{}

// Ensure NoopStorage implements Storage interface
var _ Storage = (*NoopStorage)(nil)

// NewNoopStorage creates a new noop storage client
func NewNoopStorage() *NoopStorage {
	return &NoopStorage{}
}

// CheckConnection always succeeds for noop storage
func (s *NoopStorage) CheckConnection(ctx context.Context) error {
	return nil
}

// Content Operations - all return ErrStorageNotConfigured

func (s *NoopStorage) Put(ctx context.Context, tenant, contentType, id, ext string, content []byte, mimeType string, state State) (*ContentItem, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) PutStream(ctx context.Context, tenant, contentType, id, ext string, body io.Reader, contentLength int64, mimeType string, state State, metadata map[string]string) (*ContentItem, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) Get(ctx context.Context, tenant, contentType, id, ext string, state State) (*ContentItem, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) GetStream(ctx context.Context, tenant, contentType, id, ext string, state State) (*ContentStream, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) FindContentStream(ctx context.Context, tenant, contentType, id, extHint string, state State) (*ContentStream, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) Delete(ctx context.Context, tenant, contentType, id, ext string, state State) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) List(ctx context.Context, tenant, contentType string, state State) ([]*ContentItem, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) Exists(ctx context.Context, tenant, contentType, id, ext string, state State) (bool, error) {
	return false, ErrStorageNotConfigured
}

func (s *NoopStorage) Transition(ctx context.Context, tenant, contentType, id, ext string, fromState, toState State) (*ContentItem, error) {
	return nil, ErrStorageNotConfigured
}

// Versioning - all return ErrStorageNotConfigured

func (s *NoopStorage) ListVersions(ctx context.Context, tenant, contentType, id, ext string) ([]*ContentVersion, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) GetVersion(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentItem, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) GetVersionStream(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentStream, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) RestoreVersion(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentItem, error) {
	return nil, ErrStorageNotConfigured
}

// History - all return ErrStorageNotConfigured

func (s *NoopStorage) PutHistoryRecord(ctx context.Context, tenant, contentType, id string, record *HistoryRecord) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) GetHistoryRecord(ctx context.Context, tenant, contentType, id, version string) (*HistoryRecord, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) ListHistoryRecords(ctx context.Context, tenant, contentType, id string) ([]*HistoryRecord, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) GetLatestHistoryVersion(ctx context.Context, tenant, contentType, id string) (string, error) {
	return "", ErrStorageNotConfigured
}

// Schemas - all return ErrStorageNotConfigured

func (s *NoopStorage) GetSchema(ctx context.Context, tenant, schemaName string) (*Schema, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) GetGlobalSchema(ctx context.Context, schemaName string) (*Schema, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) GetTenantSchema(ctx context.Context, tenant, schemaName string) (*Schema, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) PutGlobalSchema(ctx context.Context, schemaName string, content []byte) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) PutTenantSchema(ctx context.Context, tenant, schemaName string, content []byte) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) DeleteGlobalSchema(ctx context.Context, schemaName string) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) DeleteTenantSchema(ctx context.Context, tenant, schemaName string) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) ListGlobalSchemas(ctx context.Context) ([]string, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) ListTenantSchemas(ctx context.Context, tenant string) ([]string, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) ListAllSchemas(ctx context.Context, tenant string) ([]string, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) SchemaExists(ctx context.Context, tenant, schemaName string) (bool, error) {
	return false, ErrStorageNotConfigured
}

// Comments - all return ErrStorageNotConfigured

func (s *NoopStorage) PutComment(ctx context.Context, tenant, contentType, contentID string, state State, comment *Comment) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) GetComment(ctx context.Context, tenant, contentType, contentID string, state State, id string) (*Comment, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) ListComments(ctx context.Context, tenant, contentType, contentID string, state State) ([]*Comment, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) DeleteComment(ctx context.Context, tenant, contentType, contentID string, state State, id string) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) DeleteAllComments(ctx context.Context, tenant, contentType, contentID string, state State) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) HasUnresolvedComments(ctx context.Context, tenant, contentType, contentID string, state State) (bool, error) {
	return false, ErrStorageNotConfigured
}

// Webhooks - all return ErrStorageNotConfigured

func (s *NoopStorage) ListWebhooks(ctx context.Context, tenant string) ([]*Webhook, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) GetWebhook(ctx context.Context, tenant, webhookID string) (*Webhook, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) PutWebhook(ctx context.Context, tenant string, webhook *Webhook) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) DeleteWebhook(ctx context.Context, tenant, webhookID string) error {
	return ErrStorageNotConfigured
}

// Metadata - all return ErrStorageNotConfigured

func (s *NoopStorage) GetMetadata(ctx context.Context, tenant, contentType, id, ext string, state State) (map[string]string, error) {
	return nil, ErrStorageNotConfigured
}

func (s *NoopStorage) SetMetadata(ctx context.Context, tenant, contentType, id, ext string, state State, metadata map[string]string) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) UpdateMetadata(ctx context.Context, tenant, contentType, id, ext string, state State, updates map[string]string) error {
	return ErrStorageNotConfigured
}

func (s *NoopStorage) DeleteMetadataKeys(ctx context.Context, tenant, contentType, id, ext string, state State, keys []string) error {
	return ErrStorageNotConfigured
}
