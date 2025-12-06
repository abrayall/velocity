# Storage Backend Implementation Guide

Velocity uses a pluggable storage interface that allows different backend implementations. This document describes the storage architecture, the current S3 implementation, and guidelines for implementing alternative backends.

## Architecture

```
velocity/internal/storage/
├── storage.go       # Storage interface and shared types
└── s3.go           # S3-compatible implementation
```

### Storage Interface

The `Storage` interface in `storage.go` defines all operations required by a storage backend:

```go
type Storage interface {
    // Connection
    CheckConnection(ctx context.Context) error

    // Content Operations
    Put(ctx, tenant, contentType, id, ext string, content []byte, mimeType string, state State) (*ContentItem, error)
    PutStream(ctx, tenant, contentType, id, ext string, body io.Reader, contentLength int64, mimeType string, state State, metadata map[string]string) (*ContentItem, error)
    Get(ctx, tenant, contentType, id, ext string, state State) (*ContentItem, error)
    GetStream(ctx, tenant, contentType, id, ext string, state State) (*ContentStream, error)
    FindContentStream(ctx, tenant, contentType, id, extHint string, state State) (*ContentStream, error)
    Delete(ctx, tenant, contentType, id, ext string, state State) error
    List(ctx, tenant, contentType string, state State) ([]*ContentItem, error)
    Exists(ctx, tenant, contentType, id, ext string, state State) (bool, error)
    Transition(ctx, tenant, contentType, id, ext string, fromState, toState State) (*ContentItem, error)

    // Versioning
    ListVersions(ctx, tenant, contentType, id, ext string) ([]*ContentVersion, error)
    GetVersion(ctx, tenant, contentType, id, ext, versionID string) (*ContentItem, error)
    GetVersionStream(ctx, tenant, contentType, id, ext, versionID string) (*ContentStream, error)
    RestoreVersion(ctx, tenant, contentType, id, ext, versionID string) (*ContentItem, error)

    // History, Schemas, Comments, Webhooks, Metadata...
}
```

## Current Implementation: S3Storage

The `S3Storage` implementation supports any S3-compatible object storage:
- AWS S3
- Wasabi
- MinIO
- DigitalOcean Spaces
- Backblaze B2
- Cloudflare R2

### Key Features
- **Native Versioning**: Uses S3 bucket versioning for content history
- **Streaming**: Direct streaming to/from S3 without buffering
- **Metadata**: Uses S3 user metadata (x-amz-meta-* headers)
- **Hierarchical Keys**: Content organized by `/{root}/tenants/{tenant}/content/{type}/{id}.{ext}`

---

## Alternative Implementation Ideas

### 1. PostgreSQL Storage

Store content in a relational database using BLOB/JSONB columns.

**Schema Design:**
```sql
-- Content table
CREATE TABLE content (
    id SERIAL PRIMARY KEY,
    tenant VARCHAR(255) NOT NULL,
    content_type VARCHAR(255) NOT NULL,
    content_id VARCHAR(255) NOT NULL,
    extension VARCHAR(50) NOT NULL,
    state VARCHAR(20) NOT NULL DEFAULT 'live',
    mime_type VARCHAR(255),
    content BYTEA,
    metadata JSONB DEFAULT '{}',
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(tenant, content_type, content_id, extension, state)
);

-- Version history table
CREATE TABLE content_versions (
    id SERIAL PRIMARY KEY,
    content_id INT REFERENCES content(id),
    version INT NOT NULL,
    content BYTEA,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- History records table
CREATE TABLE content_history (
    id SERIAL PRIMARY KEY,
    tenant VARCHAR(255) NOT NULL,
    content_type VARCHAR(255) NOT NULL,
    content_id VARCHAR(255) NOT NULL,
    version VARCHAR(255) NOT NULL,
    parent_version VARCHAR(255),
    author VARCHAR(255),
    message TEXT,
    size BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Comments table
CREATE TABLE comments (
    id VARCHAR(255) PRIMARY KEY,
    tenant VARCHAR(255) NOT NULL,
    content_type VARCHAR(255) NOT NULL,
    content_id VARCHAR(255) NOT NULL,
    state VARCHAR(20) NOT NULL,
    author VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP
);

-- Schemas table
CREATE TABLE schemas (
    id SERIAL PRIMARY KEY,
    tenant VARCHAR(255), -- NULL for global schemas
    name VARCHAR(255) NOT NULL,
    content JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(tenant, name)
);

-- Webhooks table
CREATE TABLE webhooks (
    id VARCHAR(255) PRIMARY KEY,
    tenant VARCHAR(255) NOT NULL,
    url VARCHAR(2048) NOT NULL,
    events TEXT[] NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_content_lookup ON content(tenant, content_type, content_id, state);
CREATE INDEX idx_content_list ON content(tenant, content_type, state);
CREATE INDEX idx_versions_content ON content_versions(content_id);
CREATE INDEX idx_history_lookup ON content_history(tenant, content_type, content_id);
```

**Implementation Hints:**

```go
type PostgresStorage struct {
    db *sql.DB
}

func NewPostgresStorage(connString string) (*PostgresStorage, error) {
    db, err := sql.Open("postgres", connString)
    if err != nil {
        return nil, err
    }
    return &PostgresStorage{db: db}, nil
}

func (p *PostgresStorage) Put(ctx context.Context, tenant, contentType, id, ext string,
    content []byte, mimeType string, state State) (*ContentItem, error) {

    query := `
        INSERT INTO content (tenant, content_type, content_id, extension, state, mime_type, content)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (tenant, content_type, content_id, extension, state)
        DO UPDATE SET content = $7, mime_type = $6, updated_at = NOW(), version = content.version + 1
        RETURNING version, updated_at
    `

    var version int
    var updatedAt time.Time
    err := p.db.QueryRowContext(ctx, query, tenant, contentType, id, ext, state, mimeType, content).
        Scan(&version, &updatedAt)

    // Also insert into versions table for history...

    return &ContentItem{
        Key:          fmt.Sprintf("%s/%s/%s.%s", tenant, contentType, id, ext),
        Content:      content,
        ContentType:  mimeType,
        VersionID:    strconv.Itoa(version),
        LastModified: updatedAt,
        Size:         int64(len(content)),
    }, err
}
```

**Challenges & Solutions:**

| Challenge | Solution |
|-----------|----------|
| Large binary files | Use PostgreSQL Large Objects (lo_*) or external blob storage |
| Streaming | Read content in chunks using COPY or cursor |
| Versioning | Manual version tracking in separate table with triggers |
| Concurrent access | Use row-level locking and transactions |
| Metadata | JSONB column with GIN index for fast queries |

**Pros:**
- ACID transactions
- Complex queries (search by metadata)
- Familiar tooling
- Single dependency

**Cons:**
- Large files can bloat database
- Backup complexity with large blobs
- No native streaming

---

### 2. Git Storage

Use a Git repository for content with commits as versions.

**Directory Structure:**
```
/{repo}/
├── tenants/
│   └── {tenant}/
│       ├── content/
│       │   └── {type}/
│       │       ├── {id}.{ext}           # Live content
│       │       ├── _draft/
│       │       │   └── {id}.{ext}       # Draft content
│       │       └── _pending/
│       │           └── {id}.{ext}       # Pending content
│       ├── schemas/
│       │   └── {type}.json
│       └── webhooks/
│           └── {id}.json
└── schemas/                              # Global schemas
    └── {type}.json
```

**Implementation Hints:**

```go
type GitStorage struct {
    repoPath string
    repo     *git.Repository
    mu       sync.RWMutex
}

func NewGitStorage(repoPath string) (*GitStorage, error) {
    repo, err := git.PlainOpen(repoPath)
    if err != nil {
        // Initialize new repo if doesn't exist
        repo, err = git.PlainInit(repoPath, false)
        if err != nil {
            return nil, err
        }
    }
    return &GitStorage{repoPath: repoPath, repo: repo}, nil
}

func (g *GitStorage) Put(ctx context.Context, tenant, contentType, id, ext string,
    content []byte, mimeType string, state State) (*ContentItem, error) {

    g.mu.Lock()
    defer g.mu.Unlock()

    // Build file path
    filePath := g.contentPath(tenant, contentType, id, ext, state)
    fullPath := filepath.Join(g.repoPath, filePath)

    // Ensure directory exists
    os.MkdirAll(filepath.Dir(fullPath), 0755)

    // Write file
    if err := os.WriteFile(fullPath, content, 0644); err != nil {
        return nil, err
    }

    // Store metadata in .meta file
    metaPath := fullPath + ".meta"
    meta := map[string]string{"content-type": mimeType}
    metaJSON, _ := json.Marshal(meta)
    os.WriteFile(metaPath, metaJSON, 0644)

    // Git add and commit
    w, _ := g.repo.Worktree()
    w.Add(filePath)
    w.Add(filePath + ".meta")

    commit, err := w.Commit(fmt.Sprintf("Update %s/%s/%s", tenant, contentType, id), &git.CommitOptions{
        Author: &object.Signature{
            Name:  "velocity",
            Email: "velocity@localhost",
            When:  time.Now(),
        },
    })

    return &ContentItem{
        Key:         filePath,
        Content:     content,
        ContentType: mimeType,
        VersionID:   commit.String()[:12],
    }, err
}

func (g *GitStorage) ListVersions(ctx context.Context, tenant, contentType, id, ext string) ([]*ContentVersion, error) {
    filePath := g.contentPath(tenant, contentType, id, ext, StateLive)

    // Use git log to get file history
    logIter, err := g.repo.Log(&git.LogOptions{
        PathFilter: func(path string) bool {
            return path == filePath
        },
    })

    var versions []*ContentVersion
    logIter.ForEach(func(c *object.Commit) error {
        versions = append(versions, &ContentVersion{
            VersionID:    c.Hash.String()[:12],
            LastModified: c.Author.When,
        })
        return nil
    })

    return versions, err
}
```

**Challenges & Solutions:**

| Challenge | Solution |
|-----------|----------|
| Concurrent writes | Use mutex or separate branches per write |
| Large binary files | Use Git LFS for files over threshold |
| Metadata storage | Sidecar .meta files or git notes |
| State transitions | Move files between directories, commit |
| Performance with many files | Use sparse checkout, shallow clones |

**Pros:**
- Natural versioning with full history
- Built-in diff capabilities
- Can push to remote (GitHub, GitLab) for backup
- Human-readable file structure

**Cons:**
- Not designed for large binaries (use LFS)
- Concurrent write complexity
- Repository size grows with history
- No native metadata support

---

### 3. SQLite Storage

Single-file database, perfect for development or single-node deployments.

**Schema:** Similar to PostgreSQL but simpler.

```sql
CREATE TABLE content (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant TEXT NOT NULL,
    content_type TEXT NOT NULL,
    content_id TEXT NOT NULL,
    extension TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'live',
    mime_type TEXT,
    content BLOB,
    metadata TEXT DEFAULT '{}',  -- JSON string
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(tenant, content_type, content_id, extension, state)
);

CREATE INDEX idx_content_lookup ON content(tenant, content_type, content_id, state);
```

**Implementation Hints:**

```go
type SQLiteStorage struct {
    db *sql.DB
    mu sync.RWMutex  // SQLite needs write serialization
}

func NewSQLiteStorage(dbPath string) (*SQLiteStorage, error) {
    db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
    if err != nil {
        return nil, err
    }

    // Run migrations
    _, err = db.Exec(schema)
    return &SQLiteStorage{db: db}, err
}
```

**Pros:**
- Zero configuration
- Single file backup
- Good for development/testing
- Embedded, no separate server

**Cons:**
- Single writer limitation
- Not suitable for distributed deployments
- File size limits on some systems

---

### 4. Filesystem Storage

Direct filesystem storage, simplest possible implementation.

**Directory Structure:**
```
/{root}/
├── tenants/
│   └── {tenant}/
│       └── content/
│           └── {type}/
│               ├── {id}.{ext}
│               ├── {id}.{ext}.meta         # JSON metadata
│               ├── _draft/
│               ├── _pending/
│               └── _versions/
│                   └── {id}/
│                       ├── v1.{ext}
│                       ├── v1.{ext}.meta
│                       └── ...
```

**Implementation Hints:**

```go
type FileStorage struct {
    rootPath string
    mu       sync.RWMutex
}

func (f *FileStorage) Put(ctx context.Context, tenant, contentType, id, ext string,
    content []byte, mimeType string, state State) (*ContentItem, error) {

    f.mu.Lock()
    defer f.mu.Unlock()

    filePath := f.contentPath(tenant, contentType, id, ext, state)
    fullPath := filepath.Join(f.rootPath, filePath)

    // Create directory
    os.MkdirAll(filepath.Dir(fullPath), 0755)

    // Write content
    if err := os.WriteFile(fullPath, content, 0644); err != nil {
        return nil, err
    }

    // Write metadata sidecar
    meta := map[string]interface{}{
        "content-type": mimeType,
        "size":         len(content),
        "modified":     time.Now().Format(time.RFC3339),
    }
    metaJSON, _ := json.Marshal(meta)
    os.WriteFile(fullPath+".meta", metaJSON, 0644)

    // Create version copy
    version := f.nextVersion(tenant, contentType, id)
    versionPath := f.versionPath(tenant, contentType, id, version, ext)
    os.MkdirAll(filepath.Dir(versionPath), 0755)
    os.WriteFile(versionPath, content, 0644)
    os.WriteFile(versionPath+".meta", metaJSON, 0644)

    return &ContentItem{
        Key:          filePath,
        Content:      content,
        ContentType:  mimeType,
        VersionID:    version,
        LastModified: time.Now(),
        Size:         int64(len(content)),
    }, nil
}
```

**Challenges & Solutions:**

| Challenge | Solution |
|-----------|----------|
| Versioning | Copy to _versions directory with incrementing names |
| Metadata | Sidecar .meta JSON files |
| Atomic writes | Write to temp file, then rename |
| Concurrency | File locking or mutex |
| Listing | Use filepath.Walk or os.ReadDir |

**Pros:**
- Simplest implementation
- Human-readable structure
- Easy backup (rsync, tar)
- No dependencies

**Cons:**
- No transactions
- Manual versioning
- Doesn't scale across nodes
- Metadata in separate files

---

### 5. Redis Storage

In-memory storage with optional persistence, good for caching layer or ephemeral content.

**Key Structure:**
```
content:{tenant}:{type}:{id}:{state}          -> content bytes
content:{tenant}:{type}:{id}:{state}:meta     -> JSON metadata
content:{tenant}:{type}:{id}:versions         -> sorted set of version IDs
content:{tenant}:{type}:{id}:v:{version}      -> content bytes for version
```

**Implementation Hints:**

```go
type RedisStorage struct {
    client *redis.Client
}

func (r *RedisStorage) Put(ctx context.Context, tenant, contentType, id, ext string,
    content []byte, mimeType string, state State) (*ContentItem, error) {

    key := fmt.Sprintf("content:%s:%s:%s:%s", tenant, contentType, id, state)
    metaKey := key + ":meta"
    versionsKey := fmt.Sprintf("content:%s:%s:%s:versions", tenant, contentType, id)

    // Generate version ID
    versionID := uuid.New().String()[:12]
    versionKey := fmt.Sprintf("content:%s:%s:%s:v:%s", tenant, contentType, id, versionID)

    pipe := r.client.Pipeline()

    // Store current content
    pipe.Set(ctx, key, content, 0)

    // Store metadata
    meta := map[string]interface{}{
        "content-type": mimeType,
        "version":      versionID,
        "modified":     time.Now().Unix(),
    }
    metaJSON, _ := json.Marshal(meta)
    pipe.Set(ctx, metaKey, metaJSON, 0)

    // Store version
    pipe.Set(ctx, versionKey, content, 0)
    pipe.ZAdd(ctx, versionsKey, &redis.Z{
        Score:  float64(time.Now().Unix()),
        Member: versionID,
    })

    _, err := pipe.Exec(ctx)

    return &ContentItem{
        Key:         key,
        Content:     content,
        ContentType: mimeType,
        VersionID:   versionID,
    }, err
}
```

**Pros:**
- Very fast reads/writes
- Built-in expiration
- Pub/sub for real-time updates
- Clustering support

**Cons:**
- Memory-limited
- Persistence complexity
- Not ideal for large files

---

## Implementation Checklist

When implementing a new storage backend:

1. **Create the struct:**
   ```go
   type MyStorage struct {
       // backend-specific fields
   }

   var _ Storage = (*MyStorage)(nil)  // Ensure interface compliance
   ```

2. **Implement constructor:**
   ```go
   func NewMyStorage(config MyConfig) (*MyStorage, error)
   ```

3. **Implement all interface methods** (see `storage.go` for full list)

4. **Handle edge cases:**
   - Content not found → return error
   - State defaults to `StateLive`
   - Empty metadata → return empty map, not nil
   - Streaming → implement both `Get` (buffered) and `GetStream` (streaming)

5. **Test with the API:**
   - Create content
   - Update content
   - Delete content
   - List content
   - State transitions (draft → pending → live)
   - Version history
   - Metadata operations
   - Schema operations
   - Comments
   - Webhooks

6. **Update server initialization:**
   ```go
   // In server/main.go
   var store storage.Storage

   switch config.StorageType {
   case "s3":
       store, err = storage.NewS3Storage(s3Config)
   case "postgres":
       store, err = storage.NewPostgresStorage(pgConfig)
   case "file":
       store, err = storage.NewFileStorage(fileConfig)
   }
   ```

## Performance Considerations

| Backend | Read | Write | List | Large Files | Concurrent |
|---------|------|-------|------|-------------|------------|
| S3 | Good | Good | Fair | Excellent | Excellent |
| PostgreSQL | Excellent | Good | Excellent | Fair | Good |
| SQLite | Excellent | Fair | Good | Fair | Poor |
| Filesystem | Good | Good | Fair | Good | Fair |
| Git | Fair | Fair | Fair | Poor (use LFS) | Poor |
| Redis | Excellent | Excellent | Good | Poor | Excellent |

## Recommended Use Cases

| Backend | Best For |
|---------|----------|
| **S3** | Production, any scale, when you need durability |
| **PostgreSQL** | When you need complex queries, transactions, existing Postgres |
| **SQLite** | Development, testing, single-node deployment |
| **Filesystem** | Development, simple deployments, when you want human-readable |
| **Git** | When you want version control semantics, collaboration |
| **Redis** | Caching layer, ephemeral content, real-time features |
