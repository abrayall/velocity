package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velocity/internal/log"
)

// CacheConfig controls the behavior of the in-memory cache.
type CacheConfig struct {
	MaxTTL         time.Duration // idle TTL before eviction (default 1h)
	MaxContentSize int64         // max body size to cache per entry (default 1MB)
	MaxMemory      int64         // total memory budget for cached content (default 256MB)
	SweepInterval  time.Duration // background cleanup interval (default 1m)
}

// DefaultCacheConfig returns sensible defaults.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MaxTTL:         1 * time.Hour,
		MaxContentSize: 1 << 20,   // 1MB
		MaxMemory:      256 << 20, // 256MB
		SweepInterval:  1 * time.Minute,
	}
}

// CacheInvalidator provides a hook for multi-instance cache invalidation.
// The default implementation is a no-op; future implementations could use
// Redis pub/sub, SQS, etc.
type CacheInvalidator interface {
	Publish(ctx context.Context, keys []string) error
	Subscribe(handler func(keys []string)) error
	Close() error
}

// noopInvalidator does nothing.
type noopInvalidator struct{}

func (n *noopInvalidator) Publish(_ context.Context, _ []string) error { return nil }
func (n *noopInvalidator) Subscribe(_ func(keys []string)) error       { return nil }
func (n *noopInvalidator) Close() error                                { return nil }

// cacheEntry holds a cached value and tracking metadata.
type cacheEntry struct {
	value        interface{}
	lastAccessed time.Time
	size         int64
}

// CachedStorage wraps a Storage implementation with an in-memory cache.
// It implements the Storage interface via the decorator pattern.
type CachedStorage struct {
	inner         Storage
	mu            sync.RWMutex
	cache         map[string]*cacheEntry
	config        CacheConfig
	invalidator   CacheInvalidator
	currentMemory int64
	stopCh        chan struct{}
}

// Ensure CachedStorage implements Storage interface
var _ Storage = (*CachedStorage)(nil)

// NewCachedStorage wraps the given Storage with an in-memory caching layer.
func NewCachedStorage(inner Storage, config CacheConfig) *CachedStorage {
	if config.MaxTTL == 0 {
		config.MaxTTL = DefaultCacheConfig().MaxTTL
	}
	if config.MaxContentSize == 0 {
		config.MaxContentSize = DefaultCacheConfig().MaxContentSize
	}
	if config.MaxMemory == 0 {
		config.MaxMemory = DefaultCacheConfig().MaxMemory
	}
	if config.SweepInterval == 0 {
		config.SweepInterval = DefaultCacheConfig().SweepInterval
	}

	cs := &CachedStorage{
		inner:       inner,
		cache:       make(map[string]*cacheEntry),
		config:      config,
		invalidator: &noopInvalidator{},
		stopCh:      make(chan struct{}),
	}

	// Start background sweep goroutine
	go cs.sweepLoop()


	return cs
}

// SetInvalidator sets a custom cache invalidator for multi-instance support.
func (cs *CachedStorage) SetInvalidator(inv CacheInvalidator) {
	cs.invalidator = inv
	inv.Subscribe(func(keys []string) {
		cs.mu.Lock()
		defer cs.mu.Unlock()
		for _, key := range keys {
			cs.deleteEntry(key)
		}
	})
}

// Stop shuts down the background sweep goroutine.
func (cs *CachedStorage) Stop() {
	close(cs.stopCh)
	cs.invalidator.Close()
}

// --- Cache internals ---

func (cs *CachedStorage) sweepLoop() {
	ticker := time.NewTicker(cs.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cs.sweep()
		case <-cs.stopCh:
			return
		}
	}
}

func (cs *CachedStorage) sweep() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	now := time.Now()
	for key, entry := range cs.cache {
		if now.Sub(entry.lastAccessed) > cs.config.MaxTTL {
			cs.deleteEntry(key)
		}
	}
}

func (cs *CachedStorage) get(key string) (interface{}, bool) {
	cs.mu.RLock()
	entry, ok := cs.cache[key]
	if ok {
		entry.lastAccessed = time.Now()
	}
	cs.mu.RUnlock()
	return entryValue(entry, ok)
}

func entryValue(entry *cacheEntry, ok bool) (interface{}, bool) {
	if !ok {
		return nil, false
	}
	return entry.value, true
}

func (cs *CachedStorage) set(key string, value interface{}, size int64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Check per-entry size limit
	if size > cs.config.MaxContentSize {
		return
	}

	// Delete existing entry if present (reclaim memory)
	if existing, ok := cs.cache[key]; ok {
		cs.currentMemory -= existing.size
		delete(cs.cache, key)
	}

	// Check total memory budget
	if cs.currentMemory+size > cs.config.MaxMemory {
		return
	}

	cs.cache[key] = &cacheEntry{
		value:        value,
		lastAccessed: time.Now(),
		size:         size,
	}
	cs.currentMemory += size
}

func (cs *CachedStorage) deleteEntry(key string) {
	if entry, ok := cs.cache[key]; ok {
		cs.currentMemory -= entry.size
		delete(cs.cache, key)
	}
}

func (cs *CachedStorage) invalidate(keys []string) {
	cs.mu.Lock()
	for _, key := range keys {
		cs.deleteEntry(key)
	}
	cs.mu.Unlock()
	cs.invalidator.Publish(context.Background(), keys)
}

func (cs *CachedStorage) invalidatePrefix(prefix string) {
	cs.mu.Lock()
	var keys []string
	for key := range cs.cache {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		cs.deleteEntry(key)
	}
	cs.mu.Unlock()
	if len(keys) > 0 {
		cs.invalidator.Publish(context.Background(), keys)
	}
}

// --- Cache key builders ---

func contentKey(tenant, contentType, id, ext string, state State) string {
	return fmt.Sprintf("content:%s:%s:%s:%s:%s", tenant, contentType, id, ext, state)
}

func browseKey(tenant, contentType, prefix string, state State) string {
	return fmt.Sprintf("browse:%s:%s:%s:%s", tenant, contentType, prefix, state)
}

func listKey(tenant, contentType string, state State) string {
	return fmt.Sprintf("list:%s:%s:%s", tenant, contentType, state)
}

func indexKey(tenant, contentType, prefix string, state State) string {
	return fmt.Sprintf("index:%s:%s:%s:%s", tenant, contentType, prefix, state)
}

func existsKey(tenant, contentType, id, ext string, state State) string {
	return fmt.Sprintf("exists:%s:%s:%s:%s:%s", tenant, contentType, id, ext, state)
}

func schemaKey(scope, tenant, name string) string {
	return fmt.Sprintf("schema:%s:%s:%s", scope, tenant, name)
}

// invalidation helper: returns keys that should be invalidated on a write
func writeInvalidationKeys(tenant, contentType, id, ext string, state State) []string {
	return []string{
		contentKey(tenant, contentType, id, ext, state),
		existsKey(tenant, contentType, id, ext, state),
	}
}

func (cs *CachedStorage) invalidateOnWrite(tenant, contentType, id, ext string, state State) {
	keys := writeInvalidationKeys(tenant, contentType, id, ext, state)
	cs.invalidate(keys)
	// Also invalidate browse and list for this type+state (prefix match)
	cs.invalidatePrefix(fmt.Sprintf("browse:%s:%s:", tenant, contentType))
	cs.invalidatePrefix(fmt.Sprintf("list:%s:%s:", tenant, contentType))
}

// --- Cached read methods ---

func (cs *CachedStorage) Get(ctx context.Context, tenant, contentType, id, ext string, state State) (*ContentItem, error) {
	key := contentKey(tenant, contentType, id, ext, state)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit: %s", key)
		return cached.(*ContentItem), nil
	}

	item, err := cs.inner.Get(ctx, tenant, contentType, id, ext, state)
	if err != nil {
		return nil, err
	}

	size := int64(len(item.Content)) + int64(len(item.Key)) + 128 // approx overhead
	cs.set(key, item, size)
	return item, nil
}

func (cs *CachedStorage) GetStream(ctx context.Context, tenant, contentType, id, ext string, state State) (*ContentStream, error) {
	key := contentKey(tenant, contentType, id, ext, state)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit (stream): %s", key)
		item := cached.(*ContentItem)
		return &ContentStream{
			Key:          item.Key,
			Body:         io.NopCloser(bytes.NewReader(item.Content)),
			ContentType:  item.ContentType,
			VersionID:    item.VersionID,
			LastModified: item.LastModified,
			Size:         item.Size,
			ETag:         item.ETag,
			Metadata:     item.Metadata,
		}, nil
	}

	stream, err := cs.inner.GetStream(ctx, tenant, contentType, id, ext, state)
	if err != nil {
		return nil, err
	}

	// Wrap body with caching reader
	cr := &cachingReader{
		inner:   stream.Body,
		maxSize: cs.config.MaxContentSize,
		buf:     &bytes.Buffer{},
	}
	cr.onClose = func(data []byte) {
		if data != nil {
			item := &ContentItem{
				Key:          stream.Key,
				Content:      data,
				ContentType:  stream.ContentType,
				VersionID:    stream.VersionID,
				LastModified: stream.LastModified,
				Size:         stream.Size,
				ETag:         stream.ETag,
				Metadata:     stream.Metadata,
			}
			size := int64(len(data)) + int64(len(stream.Key)) + 128
			cs.set(key, item, size)
		}
	}

	return &ContentStream{
		Key:          stream.Key,
		Body:         cr,
		ContentType:  stream.ContentType,
		VersionID:    stream.VersionID,
		LastModified: stream.LastModified,
		Size:         stream.Size,
		ETag:         stream.ETag,
		Metadata:     stream.Metadata,
	}, nil
}

func (cs *CachedStorage) FindContentStream(ctx context.Context, tenant, contentType, id, extHint string, state State) (*ContentStream, error) {
	key := contentKey(tenant, contentType, id, extHint, state)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit (find): %s", key)
		item := cached.(*ContentItem)
		return &ContentStream{
			Key:          item.Key,
			Body:         io.NopCloser(bytes.NewReader(item.Content)),
			ContentType:  item.ContentType,
			VersionID:    item.VersionID,
			LastModified: item.LastModified,
			Size:         item.Size,
			ETag:         item.ETag,
			Metadata:     item.Metadata,
		}, nil
	}

	stream, err := cs.inner.FindContentStream(ctx, tenant, contentType, id, extHint, state)
	if err != nil {
		return nil, err
	}

	cr := &cachingReader{
		inner:   stream.Body,
		maxSize: cs.config.MaxContentSize,
		buf:     &bytes.Buffer{},
	}
	cr.onClose = func(data []byte) {
		if data != nil {
			item := &ContentItem{
				Key:          stream.Key,
				Content:      data,
				ContentType:  stream.ContentType,
				VersionID:    stream.VersionID,
				LastModified: stream.LastModified,
				Size:         stream.Size,
				ETag:         stream.ETag,
				Metadata:     stream.Metadata,
			}
			size := int64(len(data)) + int64(len(stream.Key)) + 128
			cs.set(key, item, size)
		}
	}

	return &ContentStream{
		Key:          stream.Key,
		Body:         cr,
		ContentType:  stream.ContentType,
		VersionID:    stream.VersionID,
		LastModified: stream.LastModified,
		Size:         stream.Size,
		ETag:         stream.ETag,
		Metadata:     stream.Metadata,
	}, nil
}

func (cs *CachedStorage) Browse(ctx context.Context, tenant, contentType, prefix string, state State) (*BrowseResult, error) {
	key := browseKey(tenant, contentType, prefix, state)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit: %s", key)
		return copyBrowseResult(cached.(*BrowseResult)), nil
	}

	result, err := cs.inner.Browse(ctx, tenant, contentType, prefix, state)
	if err != nil {
		return nil, err
	}

	// Estimate size: folders + items metadata
	var size int64
	for _, f := range result.Folders {
		size += int64(len(f))
	}
	for _, item := range result.Items {
		size += int64(len(item.Key)) + item.Size + 128
	}
	size += 64 // struct overhead
	cs.set(key, result, size)
	return result, nil
}

// copyBrowseResult returns a shallow copy so callers can't mutate the cached slices.
func copyBrowseResult(src *BrowseResult) *BrowseResult {
	folders := make([]string, len(src.Folders))
	copy(folders, src.Folders)
	items := make([]*ContentItem, len(src.Items))
	copy(items, src.Items)
	return &BrowseResult{
		Folders: folders,
		Items:   items,
	}
}

func (cs *CachedStorage) List(ctx context.Context, tenant, contentType string, state State) ([]*ContentItem, error) {
	key := listKey(tenant, contentType, state)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit: %s", key)
		return cached.([]*ContentItem), nil
	}

	items, err := cs.inner.List(ctx, tenant, contentType, state)
	if err != nil {
		return nil, err
	}

	var size int64
	for _, item := range items {
		size += int64(len(item.Key)) + 128
	}
	cs.set(key, items, size)
	return items, nil
}

func (cs *CachedStorage) Exists(ctx context.Context, tenant, contentType, id, ext string, state State) (bool, error) {
	key := existsKey(tenant, contentType, id, ext, state)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit: %s", key)
		return cached.(bool), nil
	}

	exists, err := cs.inner.Exists(ctx, tenant, contentType, id, ext, state)
	if err != nil {
		return false, err
	}

	cs.set(key, exists, 16)
	return exists, nil
}

func (cs *CachedStorage) GetDirectoryIndex(ctx context.Context, tenant, contentType, prefix string, state State) (*DirectoryIndex, error) {
	key := indexKey(tenant, contentType, prefix, state)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit: %s", key)
		return cached.(*DirectoryIndex), nil
	}

	index, err := cs.inner.GetDirectoryIndex(ctx, tenant, contentType, prefix, state)
	if err != nil {
		return nil, err
	}

	var size int64
	for _, o := range index.Order {
		size += int64(len(o))
	}
	size += 64
	cs.set(key, index, size)
	return index, nil
}

func (cs *CachedStorage) GetSchema(ctx context.Context, tenant, schemaName string) (*Schema, error) {
	key := schemaKey("combined", tenant, schemaName)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit: %s", key)
		return cached.(*Schema), nil
	}

	schema, err := cs.inner.GetSchema(ctx, tenant, schemaName)
	if err != nil {
		return nil, err
	}

	size := int64(len(schema.Content)) + int64(len(schema.Name)) + 64
	cs.set(key, schema, size)
	return schema, nil
}

func (cs *CachedStorage) GetGlobalSchema(ctx context.Context, schemaName string) (*Schema, error) {
	key := schemaKey("global", "", schemaName)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit: %s", key)
		return cached.(*Schema), nil
	}

	schema, err := cs.inner.GetGlobalSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	size := int64(len(schema.Content)) + int64(len(schema.Name)) + 64
	cs.set(key, schema, size)
	return schema, nil
}

func (cs *CachedStorage) GetTenantSchema(ctx context.Context, tenant, schemaName string) (*Schema, error) {
	key := schemaKey("tenant", tenant, schemaName)
	if cached, ok := cs.get(key); ok {
		log.Debug("Cache hit: %s", key)
		return cached.(*Schema), nil
	}

	schema, err := cs.inner.GetTenantSchema(ctx, tenant, schemaName)
	if err != nil {
		return nil, err
	}

	size := int64(len(schema.Content)) + int64(len(schema.Name)) + 64
	cs.set(key, schema, size)
	return schema, nil
}

// --- Write methods with invalidation ---

func (cs *CachedStorage) Put(ctx context.Context, tenant, contentType, id, ext string, content []byte, mimeType string, state State) (*ContentItem, error) {
	item, err := cs.inner.Put(ctx, tenant, contentType, id, ext, content, mimeType, state)
	if err != nil {
		return nil, err
	}
	cs.invalidateOnWrite(tenant, contentType, id, ext, state)
	return item, nil
}

func (cs *CachedStorage) PutStream(ctx context.Context, tenant, contentType, id, ext string, body io.Reader, contentLength int64, mimeType string, state State, metadata map[string]string) (*ContentItem, error) {
	item, err := cs.inner.PutStream(ctx, tenant, contentType, id, ext, body, contentLength, mimeType, state, metadata)
	if err != nil {
		return nil, err
	}
	cs.invalidateOnWrite(tenant, contentType, id, ext, state)
	return item, nil
}

func (cs *CachedStorage) Delete(ctx context.Context, tenant, contentType, id, ext string, state State) error {
	err := cs.inner.Delete(ctx, tenant, contentType, id, ext, state)
	if err != nil {
		return err
	}
	cs.invalidateOnWrite(tenant, contentType, id, ext, state)
	return nil
}

func (cs *CachedStorage) Transition(ctx context.Context, tenant, contentType, id, ext string, fromState, toState State) (*ContentItem, error) {
	item, err := cs.inner.Transition(ctx, tenant, contentType, id, ext, fromState, toState)
	if err != nil {
		return nil, err
	}
	// Invalidate both states
	cs.invalidateOnWrite(tenant, contentType, id, ext, fromState)
	cs.invalidateOnWrite(tenant, contentType, id, ext, toState)
	return item, nil
}

func (cs *CachedStorage) PutDirectoryIndex(ctx context.Context, tenant, contentType, prefix string, state State, index *DirectoryIndex) error {
	err := cs.inner.PutDirectoryIndex(ctx, tenant, contentType, prefix, state, index)
	if err != nil {
		return err
	}
	cs.invalidate([]string{
		indexKey(tenant, contentType, prefix, state),
		browseKey(tenant, contentType, prefix, state),
	})
	return nil
}

func (cs *CachedStorage) CreateFolder(ctx context.Context, tenant, contentType, folderPath string, state State) error {
	err := cs.inner.CreateFolder(ctx, tenant, contentType, folderPath, state)
	if err != nil {
		return err
	}
	cs.invalidatePrefix(fmt.Sprintf("browse:%s:%s:", tenant, contentType))
	cs.invalidatePrefix(fmt.Sprintf("list:%s:%s:", tenant, contentType))
	return nil
}

func (cs *CachedStorage) RestoreVersion(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentItem, error) {
	item, err := cs.inner.RestoreVersion(ctx, tenant, contentType, id, ext, versionID)
	if err != nil {
		return nil, err
	}
	cs.invalidateOnWrite(tenant, contentType, id, ext, StateLive)
	return item, nil
}

func (cs *CachedStorage) PutGlobalSchema(ctx context.Context, schemaName string, content []byte) error {
	err := cs.inner.PutGlobalSchema(ctx, schemaName, content)
	if err != nil {
		return err
	}
	cs.invalidatePrefix("schema:")
	return nil
}

func (cs *CachedStorage) PutTenantSchema(ctx context.Context, tenant, schemaName string, content []byte) error {
	err := cs.inner.PutTenantSchema(ctx, tenant, schemaName, content)
	if err != nil {
		return err
	}
	cs.invalidatePrefix("schema:")
	return nil
}

func (cs *CachedStorage) DeleteGlobalSchema(ctx context.Context, schemaName string) error {
	err := cs.inner.DeleteGlobalSchema(ctx, schemaName)
	if err != nil {
		return err
	}
	cs.invalidatePrefix("schema:")
	return nil
}

func (cs *CachedStorage) DeleteTenantSchema(ctx context.Context, tenant, schemaName string) error {
	err := cs.inner.DeleteTenantSchema(ctx, tenant, schemaName)
	if err != nil {
		return err
	}
	cs.invalidatePrefix("schema:")
	return nil
}

func (cs *CachedStorage) SetMetadata(ctx context.Context, tenant, contentType, id, ext string, state State, metadata map[string]string) error {
	err := cs.inner.SetMetadata(ctx, tenant, contentType, id, ext, state, metadata)
	if err != nil {
		return err
	}
	cs.invalidate([]string{contentKey(tenant, contentType, id, ext, state)})
	return nil
}

func (cs *CachedStorage) UpdateMetadata(ctx context.Context, tenant, contentType, id, ext string, state State, updates map[string]string) error {
	err := cs.inner.UpdateMetadata(ctx, tenant, contentType, id, ext, state, updates)
	if err != nil {
		return err
	}
	cs.invalidate([]string{contentKey(tenant, contentType, id, ext, state)})
	return nil
}

func (cs *CachedStorage) DeleteMetadataKeys(ctx context.Context, tenant, contentType, id, ext string, state State, keys []string) error {
	err := cs.inner.DeleteMetadataKeys(ctx, tenant, contentType, id, ext, state, keys)
	if err != nil {
		return err
	}
	cs.invalidate([]string{contentKey(tenant, contentType, id, ext, state)})
	return nil
}

// --- Pass-through methods (no caching) ---

func (cs *CachedStorage) CheckConnection(ctx context.Context) error {
	return cs.inner.CheckConnection(ctx)
}

func (cs *CachedStorage) ListVersions(ctx context.Context, tenant, contentType, id, ext string) ([]*ContentVersion, error) {
	return cs.inner.ListVersions(ctx, tenant, contentType, id, ext)
}

func (cs *CachedStorage) GetVersion(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentItem, error) {
	return cs.inner.GetVersion(ctx, tenant, contentType, id, ext, versionID)
}

func (cs *CachedStorage) GetVersionStream(ctx context.Context, tenant, contentType, id, ext, versionID string) (*ContentStream, error) {
	return cs.inner.GetVersionStream(ctx, tenant, contentType, id, ext, versionID)
}

func (cs *CachedStorage) PutHistoryRecord(ctx context.Context, tenant, contentType, id string, record *HistoryRecord) error {
	return cs.inner.PutHistoryRecord(ctx, tenant, contentType, id, record)
}

func (cs *CachedStorage) GetHistoryRecord(ctx context.Context, tenant, contentType, id, version string) (*HistoryRecord, error) {
	return cs.inner.GetHistoryRecord(ctx, tenant, contentType, id, version)
}

func (cs *CachedStorage) ListHistoryRecords(ctx context.Context, tenant, contentType, id string) ([]*HistoryRecord, error) {
	return cs.inner.ListHistoryRecords(ctx, tenant, contentType, id)
}

func (cs *CachedStorage) GetLatestHistoryVersion(ctx context.Context, tenant, contentType, id string) (string, error) {
	return cs.inner.GetLatestHistoryVersion(ctx, tenant, contentType, id)
}

func (cs *CachedStorage) ListGlobalSchemas(ctx context.Context) ([]string, error) {
	return cs.inner.ListGlobalSchemas(ctx)
}

func (cs *CachedStorage) ListTenantSchemas(ctx context.Context, tenant string) ([]string, error) {
	return cs.inner.ListTenantSchemas(ctx, tenant)
}

func (cs *CachedStorage) ListAllSchemas(ctx context.Context, tenant string) ([]string, error) {
	return cs.inner.ListAllSchemas(ctx, tenant)
}

func (cs *CachedStorage) SchemaExists(ctx context.Context, tenant, schemaName string) (bool, error) {
	return cs.inner.SchemaExists(ctx, tenant, schemaName)
}

func (cs *CachedStorage) PutComment(ctx context.Context, tenant, contentType, contentID string, state State, comment *Comment) error {
	return cs.inner.PutComment(ctx, tenant, contentType, contentID, state, comment)
}

func (cs *CachedStorage) GetComment(ctx context.Context, tenant, contentType, contentID string, state State, id string) (*Comment, error) {
	return cs.inner.GetComment(ctx, tenant, contentType, contentID, state, id)
}

func (cs *CachedStorage) ListComments(ctx context.Context, tenant, contentType, contentID string, state State) ([]*Comment, error) {
	return cs.inner.ListComments(ctx, tenant, contentType, contentID, state)
}

func (cs *CachedStorage) DeleteComment(ctx context.Context, tenant, contentType, contentID string, state State, id string) error {
	return cs.inner.DeleteComment(ctx, tenant, contentType, contentID, state, id)
}

func (cs *CachedStorage) DeleteAllComments(ctx context.Context, tenant, contentType, contentID string, state State) error {
	return cs.inner.DeleteAllComments(ctx, tenant, contentType, contentID, state)
}

func (cs *CachedStorage) HasUnresolvedComments(ctx context.Context, tenant, contentType, contentID string, state State) (bool, error) {
	return cs.inner.HasUnresolvedComments(ctx, tenant, contentType, contentID, state)
}

func (cs *CachedStorage) ListWebhooks(ctx context.Context, tenant string) ([]*Webhook, error) {
	return cs.inner.ListWebhooks(ctx, tenant)
}

func (cs *CachedStorage) GetWebhook(ctx context.Context, tenant, webhookID string) (*Webhook, error) {
	return cs.inner.GetWebhook(ctx, tenant, webhookID)
}

func (cs *CachedStorage) PutWebhook(ctx context.Context, tenant string, webhook *Webhook) error {
	return cs.inner.PutWebhook(ctx, tenant, webhook)
}

func (cs *CachedStorage) DeleteWebhook(ctx context.Context, tenant, webhookID string) error {
	return cs.inner.DeleteWebhook(ctx, tenant, webhookID)
}

func (cs *CachedStorage) ListTenants(ctx context.Context) ([]string, error) {
	return cs.inner.ListTenants(ctx)
}

func (cs *CachedStorage) ListContentTypes(ctx context.Context, tenant string) ([]string, error) {
	return cs.inner.ListContentTypes(ctx, tenant)
}

func (cs *CachedStorage) CreateTenant(ctx context.Context, tenant string) error {
	return cs.inner.CreateTenant(ctx, tenant)
}

func (cs *CachedStorage) CreateContentType(ctx context.Context, tenant, contentType string) error {
	return cs.inner.CreateContentType(ctx, tenant, contentType)
}

func (cs *CachedStorage) PutSession(ctx context.Context, token string, expiresAt time.Time) error {
	return cs.inner.PutSession(ctx, token, expiresAt)
}

func (cs *CachedStorage) GetSession(ctx context.Context, token string) (time.Time, error) {
	return cs.inner.GetSession(ctx, token)
}

func (cs *CachedStorage) DeleteSession(ctx context.Context, token string) error {
	return cs.inner.DeleteSession(ctx, token)
}

func (cs *CachedStorage) DeleteExpiredSessions(ctx context.Context) (int, error) {
	return cs.inner.DeleteExpiredSessions(ctx)
}

func (cs *CachedStorage) GetMetadata(ctx context.Context, tenant, contentType, id, ext string, state State) (map[string]string, error) {
	return cs.inner.GetMetadata(ctx, tenant, contentType, id, ext, state)
}

// --- cachingReader ---

// cachingReader wraps an io.ReadCloser and buffers content up to a max size.
// On Close, if the content fit within the limit, it calls onClose with the data.
type cachingReader struct {
	inner    io.ReadCloser
	buf      *bytes.Buffer
	maxSize  int64
	overflow atomic.Bool
	onClose  func(data []byte)
}

func (cr *cachingReader) Read(p []byte) (int, error) {
	n, err := cr.inner.Read(p)
	if n > 0 && !cr.overflow.Load() {
		if int64(cr.buf.Len()+n) > cr.maxSize {
			cr.overflow.Store(true)
			cr.buf.Reset() // free memory
		} else {
			cr.buf.Write(p[:n])
		}
	}
	return n, err
}

func (cr *cachingReader) Close() error {
	err := cr.inner.Close()
	if !cr.overflow.Load() && cr.buf.Len() > 0 && cr.onClose != nil {
		// Make a copy so the buffer can be reused
		data := make([]byte, cr.buf.Len())
		copy(data, cr.buf.Bytes())
		cr.onClose(data)
	}
	return err
}
