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

// S3Config holds the S3/Wasabi configuration
type S3Config struct {
	Endpoint        string // S3 endpoint (e.g., s3.wasabisys.com)
	Region          string // Region (e.g., us-east-1)
	Bucket          string // Bucket name
	AccessKeyID     string
	SecretAccessKey string
	Root            string // Root path prefix (e.g., "development" or "production")
	MaxVersions     int    // Max versions to keep (0 or negative means unlimited, default 10)
}

// S3Storage provides S3-compatible storage operations.
// Implements the Storage interface.
type S3Storage struct {
	s3Client    *s3.Client
	bucket      string
	root        string // Root path prefix
	maxVersions int    // Max versions to keep (0 or negative means unlimited)
}

// Ensure S3Storage implements Storage interface
var _ Storage = (*S3Storage)(nil)

// NewS3Storage creates a new S3-compatible storage client
func NewS3Storage(cfg S3Config) (*S3Storage, error) {
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
		o.UsePathStyle = true // Required for Wasabi and many S3-compatible stores
	})

	// Clean up root path (remove leading/trailing slashes)
	root := strings.Trim(cfg.Root, "/")

	// Default to 10 versions if not specified
	maxVersions := cfg.MaxVersions
	if maxVersions == 0 {
		maxVersions = 10
	}

	return &S3Storage{
		s3Client:    s3Client,
		bucket:      cfg.Bucket,
		root:        root,
		maxVersions: maxVersions,
	}, nil
}

// CheckConnection verifies connectivity to S3/Wasabi by checking if the bucket exists
func (s *S3Storage) CheckConnection(ctx context.Context) error {
	_, err := s.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return fmt.Errorf("cannot connect to bucket '%s': %w", s.bucket, err)
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
func (s *S3Storage) contentKey(tenant string, contentType string, id string, ext string, state State) string {
	if state == StateLive || state == "" {
		return path.Join(s.root, "tenants", tenant, "content", contentType, fmt.Sprintf("%s.%s", id, ext))
	}
	return path.Join(s.root, "tenants", tenant, "content", contentType, fmt.Sprintf("_%s", state), fmt.Sprintf("%s.%s", id, ext))
}

// contentPrefix returns the prefix for listing content of a type with state
func (s *S3Storage) contentPrefix(tenant string, contentType string, state State) string {
	if state == StateLive || state == "" {
		return path.Join(s.root, "tenants", tenant, "content", contentType) + "/"
	}
	return path.Join(s.root, "tenants", tenant, "content", contentType, fmt.Sprintf("_%s", state)) + "/"
}

// historyKey constructs the S3 key for a history record
func (s *S3Storage) historyKey(tenant string, contentType string, id string, version string) string {
	return path.Join(s.root, "tenants", tenant, "content", contentType, "_history", id, fmt.Sprintf("%s.json", version))
}

// historyPrefix returns the prefix for listing history of an item
func (s *S3Storage) historyPrefix(tenant string, contentType string, id string) string {
	return path.Join(s.root, "tenants", tenant, "content", contentType, "_history", id) + "/"
}

// commentKey constructs the S3 key for a comment (within a state directory)
func (s *S3Storage) commentKey(tenant string, contentType string, contentID string, state State, id string) string {
	return path.Join(s.root, "tenants", tenant, "content", contentType, fmt.Sprintf("_%s", state), "_comments", contentID, fmt.Sprintf("%s.json", id))
}

// commentPrefix returns the prefix for listing comments on an item in a state
func (s *S3Storage) commentPrefix(tenant string, contentType string, contentID string, state State) string {
	return path.Join(s.root, "tenants", tenant, "content", contentType, fmt.Sprintf("_%s", state), "_comments", contentID) + "/"
}

// globalSchemaKey constructs the S3 key for a global schema
func (s *S3Storage) globalSchemaKey(schemaName string) string {
	return path.Join(s.root, "schemas", fmt.Sprintf("%s.json", schemaName))
}

// tenantSchemaKey constructs the S3 key for a tenant-specific schema
func (s *S3Storage) tenantSchemaKey(tenant string, schemaName string) string {
	return path.Join(s.root, "tenants", tenant, "schemas", fmt.Sprintf("%s.json", schemaName))
}

// globalSchemasPrefix returns the prefix for listing global schemas
func (s *S3Storage) globalSchemasPrefix() string {
	return path.Join(s.root, "schemas") + "/"
}

// tenantSchemasPrefix returns the prefix for listing tenant schemas
func (s *S3Storage) tenantSchemasPrefix(tenant string) string {
	return path.Join(s.root, "tenants", tenant, "schemas") + "/"
}

// =============================================================================
// Content Operations
// =============================================================================

// Put stores content in S3/Wasabi with a specific state
func (s *S3Storage) Put(ctx context.Context, tenant string, contentType string, id string, ext string, content []byte, mimeType string, state State) (*ContentItem, error) {
	if state == "" {
		state = StateLive
	}
	key := s.contentKey(tenant, contentType, id, ext, state)

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(content),
		ContentType: aws.String(mimeType),
	}

	result, err := s.s3Client.PutObject(ctx, input)
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
func (s *S3Storage) PutStream(ctx context.Context, tenant string, contentType string, id string, ext string, body io.Reader, contentLength int64, mimeType string, state State, metadata map[string]string) (*ContentItem, error) {
	if state == "" {
		state = StateLive
	}
	key := s.contentKey(tenant, contentType, id, ext, state)

	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(contentLength),
		ContentType:   aws.String(mimeType),
	}

	// Add metadata if provided
	if len(metadata) > 0 {
		input.Metadata = metadata
	}

	result, err := s.s3Client.PutObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to put object: %w", err)
	}

	versionID := ""
	if result.VersionId != nil {
		versionID = *result.VersionId
	}

	// Prune old versions if this is live content
	if state == StateLive && s.maxVersions > 0 {
		go s.pruneVersions(context.Background(), key)
	}

	return &ContentItem{
		Key:         key,
		ContentType: mimeType,
		VersionID:   versionID,
		Metadata:    metadata,
		Size:        contentLength,
	}, nil
}

// pruneVersions deletes old versions beyond maxVersions
func (s *S3Storage) pruneVersions(ctx context.Context, key string) {
	versions, err := s.s3Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(key),
	})
	if err != nil {
		log.Error("Failed to list versions for pruning: %v", err)
		return
	}

	// Filter to only versions of this exact key (not prefix matches)
	var objectVersions []types.ObjectVersion
	for _, v := range versions.Versions {
		if aws.ToString(v.Key) == key {
			objectVersions = append(objectVersions, v)
		}
	}

	// If we have more versions than allowed, delete the oldest ones
	if len(objectVersions) > s.maxVersions {
		// Versions are returned newest first, so skip the first maxVersions
		toDelete := objectVersions[s.maxVersions:]
		for _, v := range toDelete {
			_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    aws.String(s.bucket),
				Key:       aws.String(key),
				VersionId: v.VersionId,
			})
			if err != nil {
				log.Error("Failed to delete old version %s: %v", aws.ToString(v.VersionId), err)
			} else {
				log.Debug("Pruned old version %s of %s", aws.ToString(v.VersionId), key)
			}
		}
	}
}

// Get retrieves content from S3/Wasabi with a specific state (defaults to live)
func (s *S3Storage) Get(ctx context.Context, tenant string, contentType string, id string, ext string, state State) (*ContentItem, error) {
	if state == "" {
		state = StateLive
	}
	key := s.contentKey(tenant, contentType, id, ext, state)
	return s.getByKey(ctx, key, "")
}

// GetStream retrieves content as a stream from S3/Wasabi (caller must close Body)
func (s *S3Storage) GetStream(ctx context.Context, tenant string, contentType string, id string, ext string, state State) (*ContentStream, error) {
	if state == "" {
		state = StateLive
	}
	key := s.contentKey(tenant, contentType, id, ext, state)
	return s.getStreamByKey(ctx, key, "")
}

// FindContentStream finds and retrieves content with flexible extension resolution.
// Resolution order:
// 1. If id has extension (e.g., "bio.html"), use it directly
// 2. If extHint provided, try that extension
// 3. Try common extensions: .html, .json, then no extension
// 4. Fall back to first matching file with any extension
func (s *S3Storage) FindContentStream(ctx context.Context, tenant string, contentType string, id string, extHint string, state State) (*ContentStream, error) {
	if state == "" {
		state = StateLive
	}

	// Check if id already has an extension
	if idx := strings.LastIndex(id, "."); idx != -1 && idx < len(id)-1 {
		ext := id[idx+1:]
		baseID := id[:idx]
		key := s.contentKey(tenant, contentType, baseID, ext, state)
		return s.getStreamByKey(ctx, key, "")
	}

	// If hint provided, try it first
	if extHint != "" {
		key := s.contentKey(tenant, contentType, id, extHint, state)
		stream, err := s.getStreamByKey(ctx, key, "")
		if err == nil {
			return stream, nil
		}
	}

	// List prefix to find actual file
	prefix := s.contentPrefix(tenant, contentType, state) + id + "."
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(10),
	}

	result, err := s.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	if len(result.Contents) > 0 {
		// If multiple matches, prefer non-json (json might be legacy metadata)
		var bestKey string
		for _, obj := range result.Contents {
			key := aws.ToString(obj.Key)
			if bestKey == "" {
				bestKey = key
			}
			if !strings.HasSuffix(key, ".json") {
				bestKey = key
				break
			}
		}
		return s.getStreamByKey(ctx, bestKey, "")
	}

	return nil, fmt.Errorf("content '%s' not found", id)
}

// GetVersionStream retrieves a specific version as a stream (caller must close Body)
func (s *S3Storage) GetVersionStream(ctx context.Context, tenant string, contentType string, id string, ext string, versionID string) (*ContentStream, error) {
	key := s.contentKey(tenant, contentType, id, ext, StateLive)
	return s.getStreamByKey(ctx, key, versionID)
}

// getStreamByKey retrieves content as a stream by its full S3 key
func (s *S3Storage) getStreamByKey(ctx context.Context, key string, versionID string) (*ContentStream, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}

	result, err := s.s3Client.GetObject(ctx, input)
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
		Metadata:     result.Metadata,
	}, nil
}

// GetVersion retrieves a specific version of live content
func (s *S3Storage) GetVersion(ctx context.Context, tenant string, contentType string, id string, ext string, versionID string) (*ContentItem, error) {
	key := s.contentKey(tenant, contentType, id, ext, StateLive)
	return s.getByKey(ctx, key, versionID)
}

// getByKey retrieves content by its full S3 key
func (s *S3Storage) getByKey(ctx context.Context, key string, versionID string) (*ContentItem, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}

	result, err := s.s3Client.GetObject(ctx, input)
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
func (s *S3Storage) Delete(ctx context.Context, tenant string, contentType string, id string, ext string, state State) error {
	if state == "" {
		state = StateLive
	}
	key := s.contentKey(tenant, contentType, id, ext, state)

	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

// List returns all content items of a specific type for a tenant with state (defaults to live)
func (s *S3Storage) List(ctx context.Context, tenant string, contentType string, state State) ([]*ContentItem, error) {
	if state == "" {
		state = StateLive
	}
	prefix := s.contentPrefix(tenant, contentType, state)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}

	var items []*ContentItem
	paginator := s3.NewListObjectsV2Paginator(s.s3Client, input)

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
func (s *S3Storage) ListVersions(ctx context.Context, tenant string, contentType string, id string, ext string) ([]*ContentVersion, error) {
	key := s.contentKey(tenant, contentType, id, ext, StateLive)

	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(key),
	}

	result, err := s.s3Client.ListObjectVersions(ctx, input)
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
func (s *S3Storage) RestoreVersion(ctx context.Context, tenant string, contentType string, id string, ext string, versionID string) (*ContentItem, error) {
	key := s.contentKey(tenant, contentType, id, ext, StateLive)

	// Get the specific version
	item, err := s.getByKey(ctx, key, versionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	// Copy it as the new latest version
	copySource := fmt.Sprintf("%s/%s?versionId=%s", s.bucket, key, versionID)

	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(s.bucket),
		CopySource:        aws.String(copySource),
		Key:               aws.String(key),
		MetadataDirective: types.MetadataDirectiveCopy,
	}

	copyResult, err := s.s3Client.CopyObject(ctx, copyInput)
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
func (s *S3Storage) Exists(ctx context.Context, tenant string, contentType string, id string, ext string, state State) (bool, error) {
	if state == "" {
		state = StateLive
	}
	key := s.contentKey(tenant, contentType, id, ext, state)

	_, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
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
func (s *S3Storage) Transition(ctx context.Context, tenant string, contentType string, id string, ext string, fromState State, toState State) (*ContentItem, error) {
	if fromState == toState {
		return nil, fmt.Errorf("source and target states are the same")
	}

	// Check for unresolved comments (only on draft/pending states)
	if fromState != StateLive {
		hasUnresolved, err := s.HasUnresolvedComments(ctx, tenant, contentType, id, fromState)
		if err != nil {
			return nil, fmt.Errorf("failed to check comments: %w", err)
		}
		if hasUnresolved {
			return nil, fmt.Errorf("cannot transition: unresolved comments on %s content", fromState)
		}
	}

	// Get the content from source state
	sourceItem, err := s.Get(ctx, tenant, contentType, id, ext, fromState)
	if err != nil {
		return nil, fmt.Errorf("failed to get content from %s state: %w", fromState, err)
	}

	// Put it in the target state
	targetItem, err := s.Put(ctx, tenant, contentType, id, ext, sourceItem.Content, sourceItem.ContentType, toState)
	if err != nil {
		return nil, fmt.Errorf("failed to put content to %s state: %w", toState, err)
	}

	// Delete from source state
	if err := s.Delete(ctx, tenant, contentType, id, ext, fromState); err != nil {
		// Log but don't fail - content is already in target state
		log.Error("Failed to delete content from %s state: %v", fromState, err)
	}

	// Delete all comments from source state (they were all resolved)
	if fromState != StateLive {
		if err := s.DeleteAllComments(ctx, tenant, contentType, id, fromState); err != nil {
			log.Error("Failed to delete comments from %s state: %v", fromState, err)
		}
	}

	return targetItem, nil
}

// =============================================================================
// History Operations
// =============================================================================

// PutHistoryRecord stores a history record for a version
func (s *S3Storage) PutHistoryRecord(ctx context.Context, tenant string, contentType string, id string, record *HistoryRecord) error {
	key := s.historyKey(tenant, contentType, id, record.Version)

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal history record: %w", err)
	}

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
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
func (s *S3Storage) GetHistoryRecord(ctx context.Context, tenant string, contentType string, id string, version string) (*HistoryRecord, error) {
	key := s.historyKey(tenant, contentType, id, version)

	item, err := s.getByKey(ctx, key, "")
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
func (s *S3Storage) ListHistoryRecords(ctx context.Context, tenant string, contentType string, id string) ([]*HistoryRecord, error) {
	prefix := s.historyPrefix(tenant, contentType, id)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}

	var records []*HistoryRecord
	paginator := s3.NewListObjectsV2Paginator(s.s3Client, input)

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
			item, err := s.getByKey(ctx, *obj.Key, "")
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
func (s *S3Storage) GetLatestHistoryVersion(ctx context.Context, tenant string, contentType string, id string) (string, error) {
	records, err := s.ListHistoryRecords(ctx, tenant, contentType, id)
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
func (s *S3Storage) GetSchema(ctx context.Context, tenant string, schemaName string) (*Schema, error) {
	// Try tenant-specific schema first
	tenantKey := s.tenantSchemaKey(tenant, schemaName)
	item, err := s.getByKey(ctx, tenantKey, "")
	if err == nil {
		return &Schema{
			Name:     schemaName,
			Content:  item.Content,
			IsGlobal: false,
		}, nil
	}

	// Fall back to global schema
	globalKey := s.globalSchemaKey(schemaName)
	item, err = s.getByKey(ctx, globalKey, "")
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
func (s *S3Storage) GetGlobalSchema(ctx context.Context, schemaName string) (*Schema, error) {
	key := s.globalSchemaKey(schemaName)
	item, err := s.getByKey(ctx, key, "")
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
func (s *S3Storage) GetTenantSchema(ctx context.Context, tenant string, schemaName string) (*Schema, error) {
	key := s.tenantSchemaKey(tenant, schemaName)
	item, err := s.getByKey(ctx, key, "")
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
func (s *S3Storage) PutGlobalSchema(ctx context.Context, schemaName string, content []byte) error {
	key := s.globalSchemaKey(schemaName)

	_, err := s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
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
func (s *S3Storage) PutTenantSchema(ctx context.Context, tenant string, schemaName string, content []byte) error {
	key := s.tenantSchemaKey(tenant, schemaName)

	_, err := s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
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
func (s *S3Storage) DeleteGlobalSchema(ctx context.Context, schemaName string) error {
	key := s.globalSchemaKey(schemaName)

	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete global schema: %w", err)
	}

	return nil
}

// DeleteTenantSchema removes a tenant-specific schema
func (s *S3Storage) DeleteTenantSchema(ctx context.Context, tenant string, schemaName string) error {
	key := s.tenantSchemaKey(tenant, schemaName)

	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete tenant schema: %w", err)
	}

	return nil
}

// ListGlobalSchemas returns all global schema names
func (s *S3Storage) ListGlobalSchemas(ctx context.Context) ([]string, error) {
	prefix := s.globalSchemasPrefix()
	return s.listSchemaNames(ctx, prefix)
}

// ListTenantSchemas returns all tenant-specific schema names
func (s *S3Storage) ListTenantSchemas(ctx context.Context, tenant string) ([]string, error) {
	prefix := s.tenantSchemasPrefix(tenant)
	return s.listSchemaNames(ctx, prefix)
}

// ListAllSchemas returns all available schemas for a tenant (global + tenant overrides merged)
func (s *S3Storage) ListAllSchemas(ctx context.Context, tenant string) ([]string, error) {
	// Get global schemas
	globalSchemas, err := s.ListGlobalSchemas(ctx)
	if err != nil {
		globalSchemas = []string{}
	}

	// Get tenant schemas
	tenantSchemas, err := s.ListTenantSchemas(ctx, tenant)
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
func (s *S3Storage) listSchemaNames(ctx context.Context, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}

	var schemas []string
	paginator := s3.NewListObjectsV2Paginator(s.s3Client, input)

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
func (s *S3Storage) SchemaExists(ctx context.Context, tenant string, schemaName string) (bool, error) {
	_, err := s.GetSchema(ctx, tenant, schemaName)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// =============================================================================
// Comment Operations
// =============================================================================

// PutComment stores a comment for a content item in a specific state
func (s *S3Storage) PutComment(ctx context.Context, tenant string, contentType string, contentID string, state State, comment *Comment) error {
	if state == StateLive {
		return fmt.Errorf("comments are only allowed on draft or pending content")
	}

	key := s.commentKey(tenant, contentType, contentID, state, comment.ID)

	data, err := json.Marshal(comment)
	if err != nil {
		return fmt.Errorf("failed to marshal comment: %w", err)
	}

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
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
func (s *S3Storage) GetComment(ctx context.Context, tenant string, contentType string, contentID string, state State, id string) (*Comment, error) {
	key := s.commentKey(tenant, contentType, contentID, state, id)

	item, err := s.getByKey(ctx, key, "")
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
func (s *S3Storage) ListComments(ctx context.Context, tenant string, contentType string, contentID string, state State) ([]*Comment, error) {
	prefix := s.commentPrefix(tenant, contentType, contentID, state)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}

	var comments []*Comment
	paginator := s3.NewListObjectsV2Paginator(s.s3Client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list comments: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			item, err := s.getByKey(ctx, *obj.Key, "")
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
func (s *S3Storage) DeleteComment(ctx context.Context, tenant string, contentType string, contentID string, state State, id string) error {
	key := s.commentKey(tenant, contentType, contentID, state, id)

	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete comment: %w", err)
	}

	return nil
}

// DeleteAllComments removes all comments for a content item in a state
func (s *S3Storage) DeleteAllComments(ctx context.Context, tenant string, contentType string, contentID string, state State) error {
	comments, err := s.ListComments(ctx, tenant, contentType, contentID, state)
	if err != nil {
		return err
	}

	for _, comment := range comments {
		if err := s.DeleteComment(ctx, tenant, contentType, contentID, state, comment.ID); err != nil {
			// Log but continue deleting others
			log.Error("Failed to delete comment %s: %v", comment.ID, err)
		}
	}

	return nil
}

// HasUnresolvedComments checks if there are any unresolved comments
func (s *S3Storage) HasUnresolvedComments(ctx context.Context, tenant string, contentType string, contentID string, state State) (bool, error) {
	comments, err := s.ListComments(ctx, tenant, contentType, contentID, state)
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

// =============================================================================
// Webhook Operations
// =============================================================================

// webhookKey constructs the S3 key for a webhook
func (s *S3Storage) webhookKey(tenant string, webhookID string) string {
	return path.Join(s.root, "tenants", tenant, "webhooks", webhookID+".json")
}

// webhookPrefix returns the prefix for listing webhooks
func (s *S3Storage) webhookPrefix(tenant string) string {
	return path.Join(s.root, "tenants", tenant, "webhooks") + "/"
}

// ListWebhooks lists all webhooks for a tenant
func (s *S3Storage) ListWebhooks(ctx context.Context, tenant string) ([]*Webhook, error) {
	prefix := s.webhookPrefix(tenant)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}

	result, err := s.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list webhooks: %w", err)
	}

	var webhooks []*Webhook
	for _, obj := range result.Contents {
		key := aws.ToString(obj.Key)
		// Extract webhook ID from key
		filename := path.Base(key)
		if !strings.HasSuffix(filename, ".json") {
			continue
		}
		webhookID := strings.TrimSuffix(filename, ".json")

		webhook, err := s.GetWebhook(ctx, tenant, webhookID)
		if err != nil {
			log.Error("Failed to get webhook %s: %v", webhookID, err)
			continue
		}
		webhooks = append(webhooks, webhook)
	}

	return webhooks, nil
}

// GetWebhook retrieves a webhook by ID
func (s *S3Storage) GetWebhook(ctx context.Context, tenant string, webhookID string) (*Webhook, error) {
	key := s.webhookKey(tenant, webhookID)

	result, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("webhook not found: %w", err)
	}
	defer result.Body.Close()

	var webhook Webhook
	if err := json.NewDecoder(result.Body).Decode(&webhook); err != nil {
		return nil, fmt.Errorf("failed to decode webhook: %w", err)
	}

	webhook.ID = webhookID
	return &webhook, nil
}

// PutWebhook creates or updates a webhook
func (s *S3Storage) PutWebhook(ctx context.Context, tenant string, webhook *Webhook) error {
	key := s.webhookKey(tenant, webhook.ID)

	data, err := json.Marshal(webhook)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook: %w", err)
	}

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("failed to save webhook: %w", err)
	}

	return nil
}

// DeleteWebhook removes a webhook
func (s *S3Storage) DeleteWebhook(ctx context.Context, tenant string, webhookID string) error {
	key := s.webhookKey(tenant, webhookID)

	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete webhook: %w", err)
	}

	return nil
}

// =============================================================================
// Metadata Operations
// =============================================================================

// GetMetadata retrieves only the metadata for a content item
func (s *S3Storage) GetMetadata(ctx context.Context, tenant string, contentType string, id string, ext string, state State) (map[string]string, error) {
	if state == "" {
		state = StateLive
	}
	key := s.contentKey(tenant, contentType, id, ext, state)

	result, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	return result.Metadata, nil
}

// SetMetadata replaces all metadata on a content item (copies object to itself)
func (s *S3Storage) SetMetadata(ctx context.Context, tenant string, contentType string, id string, ext string, state State, metadata map[string]string) error {
	if state == "" {
		state = StateLive
	}
	key := s.contentKey(tenant, contentType, id, ext, state)

	// Copy object to itself with new metadata
	copySource := fmt.Sprintf("%s/%s", s.bucket, key)

	_, err := s.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(s.bucket),
		CopySource:        aws.String(copySource),
		Key:               aws.String(key),
		Metadata:          metadata,
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	if err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}

	return nil
}

// UpdateMetadata merges new metadata with existing (copies object to itself)
func (s *S3Storage) UpdateMetadata(ctx context.Context, tenant string, contentType string, id string, ext string, state State, updates map[string]string) error {
	if state == "" {
		state = StateLive
	}

	// Get existing metadata
	existing, err := s.GetMetadata(ctx, tenant, contentType, id, ext, state)
	if err != nil {
		return err
	}

	// Merge updates
	if existing == nil {
		existing = make(map[string]string)
	}
	for k, v := range updates {
		existing[k] = v
	}

	// Set merged metadata
	return s.SetMetadata(ctx, tenant, contentType, id, ext, state, existing)
}

// DeleteMetadataKeys removes specific metadata keys (copies object to itself)
func (s *S3Storage) DeleteMetadataKeys(ctx context.Context, tenant string, contentType string, id string, ext string, state State, keys []string) error {
	if state == "" {
		state = StateLive
	}

	// Get existing metadata
	existing, err := s.GetMetadata(ctx, tenant, contentType, id, ext, state)
	if err != nil {
		return err
	}

	// Remove specified keys
	for _, k := range keys {
		delete(existing, k)
	}

	// Set updated metadata
	return s.SetMetadata(ctx, tenant, contentType, id, ext, state, existing)
}
