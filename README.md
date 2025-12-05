# Velocity

A fast, headless CMS backed by S3/Wasabi storage with built-in versioning, multi-tenant support, and a content workflow system.

## Overview

Velocity is a headless content management system designed for modern applications. It provides a REST API for managing content with a powerful workflow system (draft → pending → live), automatic versioning through S3, and multi-tenant isolation. All content is stored in S3-compatible object storage (AWS S3, Wasabi, MinIO, etc.) with streaming support for large files.

## Features

- **S3/Wasabi Storage** - Content stored in S3-compatible object storage with automatic versioning
- **Content Workflow** - Draft → Pending → Live state transitions with approval gates
- **Multi-tenant** - Isolated content per tenant with tenant-specific schema overrides
- **Versioning** - Full version history with restore capability (powered by S3 versioning)
- **Streaming** - Large file support with direct streaming (no memory buffering)
- **HTTP Caching** - ETag and Last-Modified headers with conditional request support (304 Not Modified)
- **Comments** - Review comments on draft/pending content with resolution tracking
- **Schemas** - JSON Schema validation with global and tenant-specific schemas
- **History** - Audit trail with author and message for each publish
- **Diff** - Compare versions with JSON field-level or text line-level diffs

## Quick Start

### Running the Server

```bash
# Build
go build -o velocity-server ./server

# Run with environment variables
export S3_ACCESS_KEY_ID=your-key
export S3_SECRET_ACCESS_KEY=your-secret
export S3_BUCKET=your-bucket

./velocity-server --port 8080 --environment development
```

### Using Docker

```bash
# Build and deploy to DigitalOcean
./deploy.sh

# Or build locally
./build.sh
```

### Basic Usage

```bash
# Check API info
curl http://localhost:8080/api
# {"name":"Velocity","version":"0.1.0"}

# Create content
curl -X POST http://localhost:8080/api/content/articles/hello-world \
  -H "Content-Type: application/json" \
  -d '{"title": "Hello World", "body": "My first article"}'

# Get content
curl http://localhost:8080/api/content/articles/hello-world

# Upload a file
curl -X POST http://localhost:8080/api/content/images/logo \
  -F "file=@logo.png"

# Create a draft
curl -X POST http://localhost:8080/api/content/articles/hello-world/draft \
  -H "Content-Type: application/json" \
  -d '{"title": "Hello World", "body": "Updated draft content"}'

# Publish draft to live
curl -X POST http://localhost:8080/api/content/articles/hello-world/transition \
  -H "Content-Type: application/json" \
  -d '{"from": "draft", "to": "live", "author": "john@example.com", "message": "Initial publish"}'
```

## Configuration

### Server Flags

| Flag | Default | Environment | Description |
|------|---------|-------------|-------------|
| `--port` | `8080` | `PORT` | Server port |
| `--environment` | `development` | `ENVIRONMENT` | Environment: development or production |
| `--s3-endpoint` | `s3.wasabisys.com` | `S3_ENDPOINT` | S3/Wasabi endpoint |
| `--s3-region` | `us-east-1` | `S3_REGION` | S3 region |
| `--s3-bucket` | `velocity` | `S3_BUCKET` | S3 bucket name |
| `--s3-access-key-id` | - | `S3_ACCESS_KEY_ID` | S3 access key |
| `--s3-secret-access-key` | - | `S3_SECRET_ACCESS_KEY` | S3 secret key |
| `--s3-root` | `/{environment}` | `S3_ROOT` | S3 root path prefix |
| `--logging` | `info` | `LOG_LEVEL` | Log level: trace, debug, info, error |
| `--version` | - | - | Show version and exit |

### Environment-Based Isolation

Content is isolated by environment using the S3 root path:
- Development: `/{bucket}/development/...`
- Production: `/{bucket}/production/...`

This allows safe development without affecting production data.

## API Reference

### Info & Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api` | API info (name, version) |
| `GET` | `/api/health` | Health check |
| `GET` | `/api/version` | Server version details |
| `GET` | `/api/types` | List available content types |

### Content Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/content/{type}` | List all live items |
| `GET` | `/api/content/{type}/draft` | List all draft items |
| `GET` | `/api/content/{type}/pending` | List all pending items |
| `POST` | `/api/content/{type}/{id}` | Create new content (live) |
| `POST` | `/api/content/{type}/{id}/draft` | Create new draft |
| `POST` | `/api/content/{type}/{id}/pending` | Create new pending |
| `GET` | `/api/content/{type}/{id}` | Get live content |
| `GET` | `/api/content/{type}/{id}/draft` | Get draft content |
| `GET` | `/api/content/{type}/{id}/pending` | Get pending content |
| `PUT` | `/api/content/{type}/{id}` | Update live content |
| `PUT` | `/api/content/{type}/{id}/{state}` | Update content in state |
| `DELETE` | `/api/content/{type}/{id}` | Delete live content |
| `DELETE` | `/api/content/{type}/{id}/{state}` | Delete content in state |

### State Transitions

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/content/{type}/{id}/transition` | Move content between states |

**Request body:**
```json
{
  "from": "draft",
  "to": "live",
  "author": "user@example.com",
  "message": "Publish reason"
}
```

**Valid transitions:**
- `draft` → `pending` (submit for review)
- `draft` → `live` (direct publish)
- `pending` → `live` (approve and publish)
- `pending` → `draft` (reject back to draft)

### Versioning

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/content/{type}/{id}/versions` | List all versions |
| `GET` | `/api/content/{type}/{id}/versions/{version}` | Get specific version |
| `POST` | `/api/content/{type}/{id}/versions/{version}/restore` | Restore version |

### History

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/content/{type}/{id}/history` | List history records |
| `GET` | `/api/content/{type}/{id}/history/{version}` | Get history record |
| `GET` | `/api/content/{type}/{id}/diff?from={v1}&to={v2}` | Diff between versions |

### Schemas

**Global Schemas** (shared across all tenants):

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/schemas` | List global schemas |
| `GET` | `/api/schemas/{name}` | Get global schema |
| `PUT` | `/api/schemas/{name}` | Create/update global schema |
| `DELETE` | `/api/schemas/{name}` | Delete global schema |

**Tenant Schemas** (tenant-specific overrides):

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/tenant/schemas` | List tenant schemas |
| `GET` | `/api/tenant/schemas/{name}` | Get tenant schema |
| `PUT` | `/api/tenant/schemas/{name}` | Create/update tenant schema |
| `DELETE` | `/api/tenant/schemas/{name}` | Delete tenant schema |

### Comments

Comments are only available on draft and pending content:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/content/{type}/{id}/{state}/comments` | List comments |
| `POST` | `/api/content/{type}/{id}/{state}/comments` | Create comment |
| `GET` | `/api/content/{type}/{id}/{state}/comments/{id}` | Get comment |
| `PUT` | `/api/content/{type}/{id}/{state}/comments/{id}` | Update/resolve comment |
| `DELETE` | `/api/content/{type}/{id}/{state}/comments/{id}` | Delete comment |

## Content Workflow

Velocity implements a three-state workflow for content:

```
                    ┌─────────────────────┐
                    │                     │
                    ▼                     │
┌─────────┐    ┌─────────┐    ┌─────────┐ │
│  DRAFT  │───▶│ PENDING │───▶│  LIVE   │─┘
└─────────┘    └─────────┘    └─────────┘
     │              │              ▲
     │              └──────────────┤ (reject)
     └─────────────────────────────┘ (direct publish)
```

- **Draft**: Work in progress, can be edited freely
- **Pending**: Submitted for review, awaiting approval
- **Live**: Published content, visible to end users

When content transitions to Live, a history record is created with the author and message.

## Storage Structure

```
/{root}/
  schemas/
    {type}.json                           # Global schemas
  tenants/{tenant}/
    schemas/
      {type}.json                         # Tenant schema overrides
    content/{type}/
      {id}.{ext}                          # Live content
      _draft/
        {id}.{ext}                        # Draft content
        _comments/{id}/{comment}.json     # Draft comments
      _pending/
        {id}.{ext}                        # Pending content
        _comments/{id}/{comment}.json     # Pending comments
      _history/{id}/
        {version}.json                    # History metadata
```

## HTTP Caching

Velocity supports HTTP caching to reduce bandwidth and improve performance:

**Response Headers:**
- `ETag` - Content hash for cache validation
- `Last-Modified` - Timestamp of last modification
- `Cache-Control` - Caching directives
- `X-Version-ID` - S3 version identifier
- `X-Content-State` - Content state (draft/pending/live)

**Conditional Requests:**
- `If-None-Match` - Returns 304 if ETag matches
- `If-Modified-Since` - Returns 304 if not modified since

**Cache Duration:**
- Live content: `max-age=60, must-revalidate`
- Specific versions: `max-age=31536000, immutable` (versions never change)

## CLI Tool

```bash
# Build CLI
go build -o velocity-cli ./cli

# Show help
./velocity-cli --help

# List content
./velocity-cli content list articles

# Get content
./velocity-cli content get articles hello-world

# Create content
./velocity-cli content create articles my-article -d '{"title": "New Article"}'
```

## Building

### Build Script

```bash
# Build for all platforms
./build.sh

# Artifacts created:
# - build/velocity-server-{version}-{os}-{arch}
# - build/velocity-cli-{version}-{os}-{arch}
```

### Supported Platforms
- darwin/amd64 (macOS Intel)
- darwin/arm64 (macOS Apple Silicon)
- linux/amd64
- linux/arm64
- windows/amd64

## Deployment

### Deploy to DigitalOcean

```bash
# Set credentials
export DIGITALOCEAN_TOKEN=your-token
export S3_ACCESS_KEY_ID=your-key
export S3_SECRET_ACCESS_KEY=your-secret

# Deploy
./deploy.sh
```

The deploy script will:
1. Calculate version from git tags (using [vermouth](https://github.com/abrayall/vermouth))
2. Build Docker image
3. Push to DigitalOcean Container Registry
4. Create/update DigitalOcean App Platform app

### GitHub Actions

The repository includes a GitHub Actions workflow (`.github/workflows/deploy.yml`) that automatically deploys on push to main.

Required secrets:
- `DIGITALOCEAN_TOKEN` - DigitalOcean API token
- `S3_ACCESS_KEY_ID` - S3/Wasabi access key
- `S3_SECRET_ACCESS_KEY` - S3/Wasabi secret key

## Future Work

### Authentication & Authorization
- [ ] **JWT Authentication** - Token-based authentication with configurable providers
- [ ] **OAuth2/OIDC** - Integration with identity providers (Auth0, Okta, etc.)
- [ ] **API Keys** - Service-to-service authentication
- [ ] **Role-Based Access Control** - Roles: admin, editor, reviewer, viewer
- [ ] **Tenant Extraction** - Extract tenant from JWT claims

### Webhooks & Events
- [ ] **Webhook Configuration** - Register webhook endpoints per event type
- [ ] **Event Types** - content.created, content.updated, content.deleted, content.published
- [ ] **Retry Logic** - Exponential backoff for failed webhook deliveries
- [ ] **Webhook Signatures** - HMAC signatures for webhook verification
- [ ] **Event Log** - Queryable log of all events

### Schema Validation
- [ ] **JSON Schema Validation** - Validate content against schemas on create/update
- [ ] **Schema Versioning** - Track schema changes over time
- [ ] **Migration Support** - Tools to migrate content when schemas change
- [ ] **Schema Inheritance** - Tenant schemas extend global schemas

### Performance & Caching
- [ ] **Server-Side Cache** - In-memory LRU cache to reduce S3 API calls
- [ ] **Redis Cache** - Distributed caching for multi-instance deployments
- [ ] **CDN Integration** - Cache invalidation hooks for CDN (CloudFront, Fastly)
- [ ] **Batch Operations** - Bulk create/update/delete endpoints

### Search & Discovery
- [ ] **Full-Text Search** - Integration with Elasticsearch or Meilisearch
- [ ] **Content Indexing** - Automatic indexing on publish
- [ ] **Faceted Search** - Filter by content type, state, date, author
- [ ] **Related Content** - Suggest related content based on similarity

### Media & Assets
- [ ] **Image Processing** - Resize, crop, format conversion on upload
- [ ] **Image Variants** - Generate multiple sizes automatically
- [ ] **Video Processing** - Transcoding and thumbnail generation
- [ ] **Asset Library** - Centralized media management

### Workflow & Publishing
- [ ] **Scheduled Publishing** - Publish content at a future date/time
- [ ] **Content Expiration** - Automatically unpublish after date
- [ ] **Workflow Customization** - Configurable approval workflows
- [ ] **Content Locking** - Prevent concurrent edits
- [ ] **Collaborative Editing** - Real-time collaboration support

### Observability
- [ ] **Audit Logging** - Track who changed what and when
- [ ] **Prometheus Metrics** - Request latency, error rates, S3 operations
- [ ] **Distributed Tracing** - OpenTelemetry integration
- [ ] **Health Checks** - Deep health checks including S3 connectivity

### API & Integration
- [ ] **GraphQL API** - Alternative to REST API
- [ ] **Batch API** - Bulk operations in single request
- [ ] **Rate Limiting** - Protect API from abuse
- [ ] **API Versioning** - Support multiple API versions

### Administration
- [ ] **Admin UI** - Web-based content management interface
- [ ] **Tenant Management** - Create/manage tenants via API
- [ ] **Usage Analytics** - Content views, API usage statistics
- [ ] **Backup/Restore** - Point-in-time recovery tools

## License

MIT
