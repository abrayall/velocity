# Velocity - Claude Context

## Project Overview

Velocity is a headless CMS backed by S3/Wasabi storage. It provides a REST API for managing content with versioning, multi-tenant support, and a draft/pending/live workflow.

## Architecture

```
velocity/
  server/           # Server entry point (main.go)
  cli/              # CLI client (main.go)
  internal/
    api/            # HTTP handlers and routing (server.go, handlers.go)
    storage/        # S3/Wasabi client (s3.go)
    log/            # Structured logging (log.go)
    models/         # Request/response types (models.go)
    ui/             # Terminal styling (styles.go)
    version/        # Version info (version.go)
```

## Key Design Decisions

### Streaming
All content uploads/downloads stream directly between client and S3 - no buffering in server memory. This allows handling files of any size.

### Content States
- **live** - Published content (S3 versioning enabled)
- **draft** - Work in progress (stored in `_draft/` prefix)
- **pending** - Awaiting approval (stored in `_pending/` prefix)

### Multi-tenancy
Tenant isolation via path prefix: `/{root}/tenants/{tenant}/...`
Currently hardcoded to "velocity" until auth is implemented.

### Schemas
- Global schemas: `/{root}/schemas/{type}.json`
- Tenant overrides: `/{root}/tenants/{tenant}/schemas/{type}.json`
- Tenant schemas take precedence over global

### Versioning
S3 versioning handles live content history automatically. The `_history/` directory stores metadata (author, message, timestamp) for each published version.

## Common Tasks

### Adding a new API endpoint
1. Add handler function in `internal/api/handlers.go`
2. Register route in `internal/api/server.go` setupRoutes()

### Adding a CLI command
1. Add command in `cli/main.go` init() function
2. Implement run function

### Adding a new storage operation
1. Add method to `internal/storage/s3.go` Client struct
2. Follow existing patterns for key construction and error handling

## Build & Run

```bash
# Build all
go build ./...

# Run server
go run ./server --port=8080 --logging=debug

# Run CLI
go run ./cli --help
```

## Testing

```bash
# Create content
curl -X POST http://localhost:8080/api/content/articles/test \
  -H "Content-Type: application/json" \
  -d '{"title": "Test"}'

# Get content
curl http://localhost:8080/api/content/articles/test

# Check health
curl http://localhost:8080/api/health
```

## Environment Variables

- `PORT` - Server port
- `ENVIRONMENT` - development or production
- `S3_ENDPOINT` - S3/Wasabi endpoint
- `S3_REGION` - S3 region
- `S3_BUCKET` - Bucket name
- `S3_ACCESS_KEY_ID` - Access key
- `S3_SECRET_ACCESS_KEY` - Secret key
- `S3_ROOT` - Root path prefix
- `LOG_LEVEL` - trace, debug, info, error
- 3....please regerate the contract again with this info