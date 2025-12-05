package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"velocity/internal/log"
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

// Config holds the S3/Wasabi configuration
type Config struct {
	Endpoint        string // Wasabi endpoint (e.g., s3.wasabisys.com)
	Region          string // Region (e.g., us-east-1)
	Bucket          string // Bucket name
	AccessKeyID     string
	SecretAccessKey string
	Root            string // Root path prefix (e.g., "development" or "production")
}

// Client provides S3/Wasabi storage operations
type Client struct {
	s3Client *s3.Client
	bucket   string
	root     string // Root path prefix
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

// NewClient creates a new S3/Wasabi storage client
func NewClient(cfg Config) (*Client, error) {
	// Custom endpoint resolver for Wasabi
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if cfg.Endpoint != "" {
			return aws.Endpoint{
				URL:               fmt.Sprintf("https://%s", cfg.Endpoint),
				SigningRegion:     cfg.Region,
				HostnameImmutable: true,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true // Required for Wasabi
	})

	// Clean up root path (remove leading/trailing slashes)
	root := strings.Trim(cfg.Root, "/")

	return &Client{
		s3Client: s3Client,
		bucket:   cfg.Bucket,
		root:     root,
	}, nil
}

// CheckConnection verifies connectivity to S3/Wasabi by checking if the bucket exists
func (c *Client) CheckConnection(ctx context.Context) error {
	_, err := c.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		return fmt.Errorf("cannot connect to bucket '%s': %w", c.bucket, err)
	}
	return nil
}

// Path builders for storage structure:
// /{root}/schemas/{type}.json                              - Global schemas
// /{root}/tenants/{tenant}/schemas/{type}.json             - Tenant schemas
// /{root}/tenants/{tenant}/content/{type}/{id}.{ext}       - Live content
// /{root}/tenants/{tenant}/content/{type}/_draft/{id}.{ext}   - Draft content
// /{root}/tenants/{tenant}/content/{type}/_pending/{id}.{ext} - Pending content

// contentKey constructs the S3 key for a content item with state
func (c *Client) contentKey(tenant string, contentType string, id string, ext string, state State) string {
	if state == StateLive || state == "" {
		return path.Join(c.root, "tenants", tenant, "content", contentType, fmt.Sprintf("%s.%s", id, ext))
	}
	return path.Join(c.root, "tenants", tenant, "content", contentType, fmt.Sprintf("_%s", state), fmt.Sprintf("%s.%s", id, ext))
}

// contentPrefix returns the prefix for listing content of a type with state
func (c *Client) contentPrefix(tenant string, contentType string, state State) string {
	if state == StateLive || state == "" {
		return path.Join(c.root, "tenants", tenant, "content", contentType) + "/"
	}
	return path.Join(c.root, "tenants", tenant, "content", contentType, fmt.Sprintf("_%s", state)) + "/"
}

// historyKey constructs the S3 key for a history record
func (c *Client) historyKey(tenant string, contentType string, id string, version string) string {
	return path.Join(c.root, "tenants", tenant, "content", contentType, "_history", id, fmt.Sprintf("%s.json", version))
}

// historyPrefix returns the prefix for listing history of an item
func (c *Client) historyPrefix(tenant string, contentType string, id string) string {
	return path.Join(c.root, "tenants", tenant, "content", contentType, "_history", id) + "/"
}

// commentKey constructs the S3 key for a comment (within a state directory)
func (c *Client) commentKey(tenant string, contentType string, contentID string, state State, id string) string {
	return path.Join(c.root, "tenants", tenant, "content", contentType, fmt.Sprintf("_%s", state), "_comments", contentID, fmt.Sprintf("%s.json", id))
}

// commentPrefix returns the prefix for listing comments on an item in a state
func (c *Client) commentPrefix(tenant string, contentType string, contentID string, state State) string {
	return path.Join(c.root, "tenants", tenant, "content", contentType, fmt.Sprintf("_%s", state), "_comments", contentID) + "/"
}

// globalSchemaKey constructs the S3 key for a global schema
func (c *Client) globalSchemaKey(schemaName string) string {
	return path.Join(c.root, "schemas", fmt.Sprintf("%s.json", schemaName))
}

// tenantSchemaKey constructs the S3 key for a tenant-specific schema
func (c *Client) tenantSchemaKey(tenant string, schemaName string) string {
	return path.Join(c.root, "tenants", tenant, "schemas", fmt.Sprintf("%s.json", schemaName))
}

// globalSchemasPrefix returns the prefix for listing global schemas
func (c *Client) globalSchemasPrefix() string {
	return path.Join(c.root, "schemas") + "/"
}

// tenantSchemasPrefix returns the prefix for listing tenant schemas
func (c *Client) tenantSchemasPrefix(tenant string) string {
	return path.Join(c.root, "tenants", tenant, "schemas") + "/"
}

// =============================================================================
// Content Operations
// =============================================================================

// Put stores content in S3/Wasabi with a specific state
func (c *Client) Put(ctx context.Context, tenant string, contentType string, id string, ext string, content []byte, mimeType string, state State) (*ContentItem, error) {
	if state == "" {
		state = StateLive
	}
	key := c.contentKey(tenant, contentType, id, ext, state)

	input := &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(content),
		ContentType: aws.String(mimeType),
	}

	result, err := c.s3Client.PutObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to put object: %w", err)
	}

	versionID := ""
	if result.VersionId != nil {
		versionID = *result.VersionId
	}

	return &ContentItem{
		Key:         key,
		Content:     content,
		ContentType: mimeType,
		VersionID:   versionID,
	}, nil
}

// PutStream stores content from a reader in S3/Wasabi with a specific state
func (c *Client) PutStream(ctx context.Context, tenant string, contentType string, id string, ext string, body io.Reader, contentLength int64, mimeType string, state State) (*ContentItem, error) {
	if state == "" {
		state = StateLive
	}
	key := c.contentKey(tenant, contentType, id, ext, state)

	input := &s3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(contentLength),
		ContentType:   aws.String(mimeType),
	}

	result, err := c.s3Client.PutObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to put object: %w", err)
	}

	versionID := ""
	if result.VersionId != nil {
		versionID = *result.VersionId
	}

	return &ContentItem{
		Key:         key,
		ContentType: mimeType,
		VersionID:   versionID,
		Size:        contentLength,
	}, nil
}

// Get retrieves content from S3/Wasabi with a specific state (defaults to live)
func (c *Client) Get(ctx context.Context, tenant string, contentType string, id string, ext string, state State) (*ContentItem, error) {
	if state == "" {
		state = StateLive
	}
	key := c.contentKey(tenant, contentType, id, ext, state)
	return c.getByKey(ctx, key, "")
}

// GetStream retrieves content as a stream from S3/Wasabi (caller must close Body)
func (c *Client) GetStream(ctx context.Context, tenant string, contentType string, id string, ext string, state State) (*ContentStream, error) {
	if state == "" {
		state = StateLive
	}
	key := c.contentKey(tenant, contentType, id, ext, state)
	return c.getStreamByKey(ctx, key, "")
}

// GetVersionStream retrieves a specific version as a stream (caller must close Body)
func (c *Client) GetVersionStream(ctx context.Context, tenant string, contentType string, id string, ext string, versionID string) (*ContentStream, error) {
	key := c.contentKey(tenant, contentType, id, ext, StateLive)
	return c.getStreamByKey(ctx, key, versionID)
}

// getStreamByKey retrieves content as a stream by its full S3 key
func (c *Client) getStreamByKey(ctx context.Context, key string, versionID string) (*ContentStream, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}

	result, err := c.s3Client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}

	vid := ""
	if result.VersionId != nil {
		vid = *result.VersionId
	}

	ct := "application/octet-stream"
	if result.ContentType != nil {
		ct = *result.ContentType
	}

	var lastMod time.Time
	if result.LastModified != nil {
		lastMod = *result.LastModified
	}

	var size int64
	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	etag := ""
	if result.ETag != nil {
		etag = *result.ETag
	}

	return &ContentStream{
		Key:          key,
		Body:         result.Body,
		ContentType:  ct,
		VersionID:    vid,
		LastModified: lastMod,
		Size:         size,
		ETag:         etag,
	}, nil
}

// GetVersion retrieves a specific version of live content
func (c *Client) GetVersion(ctx context.Context, tenant string, contentType string, id string, ext string, versionID string) (*ContentItem, error) {
	key := c.contentKey(tenant, contentType, id, ext, StateLive)
	return c.getByKey(ctx, key, versionID)
}

// getByKey retrieves content by its full S3 key
func (c *Client) getByKey(ctx context.Context, key string, versionID string) (*ContentItem, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}

	result, err := c.s3Client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	content, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	vid := ""
	if result.VersionId != nil {
		vid = *result.VersionId
	}

	ct := "application/octet-stream"
	if result.ContentType != nil {
		ct = *result.ContentType
	}

	var lastMod time.Time
	if result.LastModified != nil {
		lastMod = *result.LastModified
	}

	var size int64
	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	etag := ""
	if result.ETag != nil {
		etag = *result.ETag
	}

	return &ContentItem{
		Key:          key,
		Content:      content,
		ContentType:  ct,
		VersionID:    vid,
		LastModified: lastMod,
		Size:         size,
		ETag:         etag,
	}, nil
}

// Delete removes content from S3/Wasabi with a specific state
func (c *Client) Delete(ctx context.Context, tenant string, contentType string, id string, ext string, state State) error {
	if state == "" {
		state = StateLive
	}
	key := c.contentKey(tenant, contentType, id, ext, state)

	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

// List returns all content items of a specific type for a tenant with state (defaults to live)
func (c *Client) List(ctx context.Context, tenant string, contentType string, state State) ([]*ContentItem, error) {
	if state == "" {
		state = StateLive
	}
	prefix := c.contentPrefix(tenant, contentType, state)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}

	var items []*ContentItem
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := ""
			if obj.Key != nil {
				key = *obj.Key
			}

			// Skip state directories when listing live content
			if state == StateLive && (strings.Contains(key, "/_draft/") || strings.Contains(key, "/_pending/")) {
				continue
			}

			var lastMod time.Time
			if obj.LastModified != nil {
				lastMod = *obj.LastModified
			}

			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}

			etag := ""
			if obj.ETag != nil {
				etag = *obj.ETag
			}

			items = append(items, &ContentItem{
				Key:          key,
				LastModified: lastMod,
				Size:         size,
				ETag:         etag,
			})
		}
	}

	return items, nil
}

// ListVersions returns all versions of a specific live content item (only live content is versioned)
func (c *Client) ListVersions(ctx context.Context, tenant string, contentType string, id string, ext string) ([]*ContentVersion, error) {
	key := c.contentKey(tenant, contentType, id, ext, StateLive)

	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(key),
	}

	result, err := c.s3Client.ListObjectVersions(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}

	var versions []*ContentVersion
	for _, v := range result.Versions {
		if v.Key != nil && *v.Key == key {
			vid := ""
			if v.VersionId != nil {
				vid = *v.VersionId
			}

			var lastMod time.Time
			if v.LastModified != nil {
				lastMod = *v.LastModified
			}

			var size int64
			if v.Size != nil {
				size = *v.Size
			}

			versions = append(versions, &ContentVersion{
				VersionID:    vid,
				LastModified: lastMod,
				Size:         size,
				IsLatest:     v.IsLatest != nil && *v.IsLatest,
			})
		}
	}

	return versions, nil
}

// RestoreVersion restores a specific version of live content by copying it as the latest version
func (c *Client) RestoreVersion(ctx context.Context, tenant string, contentType string, id string, ext string, versionID string) (*ContentItem, error) {
	key := c.contentKey(tenant, contentType, id, ext, StateLive)

	// Get the specific version
	item, err := c.getByKey(ctx, key, versionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	// Copy it as the new latest version
	copySource := fmt.Sprintf("%s/%s?versionId=%s", c.bucket, key, versionID)

	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(c.bucket),
		CopySource:        aws.String(copySource),
		Key:               aws.String(key),
		MetadataDirective: types.MetadataDirectiveCopy,
	}

	copyResult, err := c.s3Client.CopyObject(ctx, copyInput)
	if err != nil {
		return nil, fmt.Errorf("failed to restore version: %w", err)
	}

	newVersionID := ""
	if copyResult.VersionId != nil {
		newVersionID = *copyResult.VersionId
	}

	return &ContentItem{
		Key:         key,
		Content:     item.Content,
		ContentType: item.ContentType,
		VersionID:   newVersionID,
	}, nil
}

// Exists checks if a content item exists with a specific state
func (c *Client) Exists(ctx context.Context, tenant string, contentType string, id string, ext string, state State) (bool, error) {
	if state == "" {
		state = StateLive
	}
	key := c.contentKey(tenant, contentType, id, ext, state)

	_, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return false, nil
	}

	return true, nil
}

// Transition moves content from one state to another
// When transitioning to live, the content is copied (creating a new version via S3 versioning)
// The source state content is deleted after successful transition
// Transition is blocked if there are unresolved comments on the source state
// All comments are deleted from source state after successful transition
func (c *Client) Transition(ctx context.Context, tenant string, contentType string, id string, ext string, fromState State, toState State) (*ContentItem, error) {
	if fromState == toState {
		return nil, fmt.Errorf("source and target states are the same")
	}

	// Check for unresolved comments (only on draft/pending states)
	if fromState != StateLive {
		hasUnresolved, err := c.HasUnresolvedComments(ctx, tenant, contentType, id, fromState)
		if err != nil {
			return nil, fmt.Errorf("failed to check comments: %w", err)
		}
		if hasUnresolved {
			return nil, fmt.Errorf("cannot transition: unresolved comments on %s content", fromState)
		}
	}

	// Get the content from source state
	sourceItem, err := c.Get(ctx, tenant, contentType, id, ext, fromState)
	if err != nil {
		return nil, fmt.Errorf("failed to get content from %s state: %w", fromState, err)
	}

	// Put it in the target state
	targetItem, err := c.Put(ctx, tenant, contentType, id, ext, sourceItem.Content, sourceItem.ContentType, toState)
	if err != nil {
		return nil, fmt.Errorf("failed to put content to %s state: %w", toState, err)
	}

	// Delete from source state
	if err := c.Delete(ctx, tenant, contentType, id, ext, fromState); err != nil {
		// Log but don't fail - content is already in target state
		log.Error("Failed to delete content from %s state: %v", fromState, err)
	}

	// Delete all comments from source state (they were all resolved)
	if fromState != StateLive {
		if err := c.DeleteAllComments(ctx, tenant, contentType, id, fromState); err != nil {
			log.Error("Failed to delete comments from %s state: %v", fromState, err)
		}
	}

	return targetItem, nil
}

// =============================================================================
// History Operations
// =============================================================================

// PutHistoryRecord stores a history record for a version
func (c *Client) PutHistoryRecord(ctx context.Context, tenant string, contentType string, id string, record *HistoryRecord) error {
	key := c.historyKey(tenant, contentType, id, record.Version)

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal history record: %w", err)
	}

	_, err = c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("failed to put history record: %w", err)
	}

	return nil
}

// GetHistoryRecord retrieves a history record for a version
func (c *Client) GetHistoryRecord(ctx context.Context, tenant string, contentType string, id string, version string) (*HistoryRecord, error) {
	key := c.historyKey(tenant, contentType, id, version)

	item, err := c.getByKey(ctx, key, "")
	if err != nil {
		return nil, fmt.Errorf("history record not found: %w", err)
	}

	var record HistoryRecord
	if err := json.Unmarshal(item.Content, &record); err != nil {
		return nil, fmt.Errorf("failed to parse history record: %w", err)
	}

	return &record, nil
}

// ListHistoryRecords returns all history records for a content item
func (c *Client) ListHistoryRecords(ctx context.Context, tenant string, contentType string, id string) ([]*HistoryRecord, error) {
	prefix := c.historyPrefix(tenant, contentType, id)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}

	var records []*HistoryRecord
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list history: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			// Get the full record
			item, err := c.getByKey(ctx, *obj.Key, "")
			if err != nil {
				continue
			}

			var record HistoryRecord
			if err := json.Unmarshal(item.Content, &record); err != nil {
				continue
			}

			records = append(records, &record)
		}
	}

	return records, nil
}

// GetLatestHistoryVersion returns the most recent version ID from history
func (c *Client) GetLatestHistoryVersion(ctx context.Context, tenant string, contentType string, id string) (string, error) {
	records, err := c.ListHistoryRecords(ctx, tenant, contentType, id)
	if err != nil {
		return "", err
	}

	if len(records) == 0 {
		return "", nil
	}

	// Find the most recent by timestamp
	var latest *HistoryRecord
	for _, r := range records {
		if latest == nil || r.Timestamp.After(latest.Timestamp) {
			latest = r
		}
	}

	return latest.Version, nil
}

// =============================================================================
// Schema Operations
// =============================================================================

// GetSchema retrieves a schema, checking tenant-specific first then falling back to global
func (c *Client) GetSchema(ctx context.Context, tenant string, schemaName string) (*Schema, error) {
	// Try tenant-specific schema first
	tenantKey := c.tenantSchemaKey(tenant, schemaName)
	item, err := c.getByKey(ctx, tenantKey, "")
	if err == nil {
		return &Schema{
			Name:     schemaName,
			Content:  item.Content,
			IsGlobal: false,
		}, nil
	}

	// Fall back to global schema
	globalKey := c.globalSchemaKey(schemaName)
	item, err = c.getByKey(ctx, globalKey, "")
	if err != nil {
		return nil, fmt.Errorf("schema not found: %s", schemaName)
	}

	return &Schema{
		Name:     schemaName,
		Content:  item.Content,
		IsGlobal: true,
	}, nil
}

// GetGlobalSchema retrieves a global schema
func (c *Client) GetGlobalSchema(ctx context.Context, schemaName string) (*Schema, error) {
	key := c.globalSchemaKey(schemaName)
	item, err := c.getByKey(ctx, key, "")
	if err != nil {
		return nil, fmt.Errorf("global schema not found: %s", schemaName)
	}

	return &Schema{
		Name:     schemaName,
		Content:  item.Content,
		IsGlobal: true,
	}, nil
}

// GetTenantSchema retrieves a tenant-specific schema (not falling back to global)
func (c *Client) GetTenantSchema(ctx context.Context, tenant string, schemaName string) (*Schema, error) {
	key := c.tenantSchemaKey(tenant, schemaName)
	item, err := c.getByKey(ctx, key, "")
	if err != nil {
		return nil, fmt.Errorf("tenant schema not found: %s", schemaName)
	}

	return &Schema{
		Name:     schemaName,
		Content:  item.Content,
		IsGlobal: false,
	}, nil
}

// PutGlobalSchema stores a global schema
func (c *Client) PutGlobalSchema(ctx context.Context, schemaName string, content []byte) error {
	key := c.globalSchemaKey(schemaName)

	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("failed to put global schema: %w", err)
	}

	return nil
}

// PutTenantSchema stores a tenant-specific schema
func (c *Client) PutTenantSchema(ctx context.Context, tenant string, schemaName string, content []byte) error {
	key := c.tenantSchemaKey(tenant, schemaName)

	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("failed to put tenant schema: %w", err)
	}

	return nil
}

// DeleteGlobalSchema removes a global schema
func (c *Client) DeleteGlobalSchema(ctx context.Context, schemaName string) error {
	key := c.globalSchemaKey(schemaName)

	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete global schema: %w", err)
	}

	return nil
}

// DeleteTenantSchema removes a tenant-specific schema
func (c *Client) DeleteTenantSchema(ctx context.Context, tenant string, schemaName string) error {
	key := c.tenantSchemaKey(tenant, schemaName)

	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete tenant schema: %w", err)
	}

	return nil
}

// ListGlobalSchemas returns all global schema names
func (c *Client) ListGlobalSchemas(ctx context.Context) ([]string, error) {
	prefix := c.globalSchemasPrefix()
	return c.listSchemaNames(ctx, prefix)
}

// ListTenantSchemas returns all tenant-specific schema names
func (c *Client) ListTenantSchemas(ctx context.Context, tenant string) ([]string, error) {
	prefix := c.tenantSchemasPrefix(tenant)
	return c.listSchemaNames(ctx, prefix)
}

// ListAllSchemas returns all available schemas for a tenant (global + tenant overrides merged)
func (c *Client) ListAllSchemas(ctx context.Context, tenant string) ([]string, error) {
	// Get global schemas
	globalSchemas, err := c.ListGlobalSchemas(ctx)
	if err != nil {
		globalSchemas = []string{}
	}

	// Get tenant schemas
	tenantSchemas, err := c.ListTenantSchemas(ctx, tenant)
	if err != nil {
		tenantSchemas = []string{}
	}

	// Merge (tenant schemas can override global)
	schemaMap := make(map[string]bool)
	for _, s := range globalSchemas {
		schemaMap[s] = true
	}
	for _, s := range tenantSchemas {
		schemaMap[s] = true
	}

	var schemas []string
	for s := range schemaMap {
		schemas = append(schemas, s)
	}

	return schemas, nil
}

// listSchemaNames helper to list schema names from a prefix
func (c *Client) listSchemaNames(ctx context.Context, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}

	var schemas []string
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list schemas: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key != nil {
				// Extract schema name from key
				filename := strings.TrimPrefix(*obj.Key, prefix)
				schemaName := strings.TrimSuffix(filename, ".json")
				if schemaName != "" {
					schemas = append(schemas, schemaName)
				}
			}
		}
	}

	return schemas, nil
}

// SchemaExists checks if a schema exists (tenant or global)
func (c *Client) SchemaExists(ctx context.Context, tenant string, schemaName string) (bool, error) {
	_, err := c.GetSchema(ctx, tenant, schemaName)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// =============================================================================
// Comment Operations
// =============================================================================

// PutComment stores a comment for a content item in a specific state
func (c *Client) PutComment(ctx context.Context, tenant string, contentType string, contentID string, state State, comment *Comment) error {
	if state == StateLive {
		return fmt.Errorf("comments are only allowed on draft or pending content")
	}

	key := c.commentKey(tenant, contentType, contentID, state, comment.ID)

	data, err := json.Marshal(comment)
	if err != nil {
		return fmt.Errorf("failed to marshal comment: %w", err)
	}

	_, err = c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("failed to put comment: %w", err)
	}

	return nil
}

// GetComment retrieves a specific comment
func (c *Client) GetComment(ctx context.Context, tenant string, contentType string, contentID string, state State, id string) (*Comment, error) {
	key := c.commentKey(tenant, contentType, contentID, state, id)

	item, err := c.getByKey(ctx, key, "")
	if err != nil {
		return nil, fmt.Errorf("comment not found: %w", err)
	}

	var comment Comment
	if err := json.Unmarshal(item.Content, &comment); err != nil {
		return nil, fmt.Errorf("failed to parse comment: %w", err)
	}

	return &comment, nil
}

// ListComments returns all comments for a content item in a specific state
func (c *Client) ListComments(ctx context.Context, tenant string, contentType string, contentID string, state State) ([]*Comment, error) {
	prefix := c.commentPrefix(tenant, contentType, contentID, state)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}

	var comments []*Comment
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list comments: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			item, err := c.getByKey(ctx, *obj.Key, "")
			if err != nil {
				continue
			}

			var comment Comment
			if err := json.Unmarshal(item.Content, &comment); err != nil {
				continue
			}

			comments = append(comments, &comment)
		}
	}

	return comments, nil
}

// DeleteComment removes a comment
func (c *Client) DeleteComment(ctx context.Context, tenant string, contentType string, contentID string, state State, id string) error {
	key := c.commentKey(tenant, contentType, contentID, state, id)

	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete comment: %w", err)
	}

	return nil
}

// DeleteAllComments removes all comments for a content item in a state
func (c *Client) DeleteAllComments(ctx context.Context, tenant string, contentType string, contentID string, state State) error {
	comments, err := c.ListComments(ctx, tenant, contentType, contentID, state)
	if err != nil {
		return err
	}

	for _, comment := range comments {
		if err := c.DeleteComment(ctx, tenant, contentType, contentID, state, comment.ID); err != nil {
			// Log but continue deleting others
			log.Error("Failed to delete comment %s: %v", comment.ID, err)
		}
	}

	return nil
}

// HasUnresolvedComments checks if there are any unresolved comments
func (c *Client) HasUnresolvedComments(ctx context.Context, tenant string, contentType string, contentID string, state State) (bool, error) {
	comments, err := c.ListComments(ctx, tenant, contentType, contentID, state)
	if err != nil {
		return false, err
	}

	for _, comment := range comments {
		if !comment.Resolved {
			return true, nil
		}
	}

	return false, nil
}
