package models

import (
	"encoding/json"
	"time"
)

// Content represents a generic content item
type Content struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Tenant      string                 `json:"tenant"`
	Environment string                 `json:"environment"`
	Data        map[string]interface{} `json:"data"`
	Metadata    Metadata               `json:"metadata"`
}

// Metadata contains content metadata
type Metadata struct {
	VersionID    string    `json:"version_id,omitempty"`
	CreatedAt    time.Time `json:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
	ContentType  string    `json:"content_type,omitempty"`
	Size         int64     `json:"size,omitempty"`
	ETag         string    `json:"etag,omitempty"`
}

// Product represents a product content type
type Product struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Price       float64   `json:"price,omitempty"`
	SKU         string    `json:"sku,omitempty"`
	Images      []string  `json:"images,omitempty"`
	Categories  []string  `json:"categories,omitempty"`
	Attributes  map[string]interface{} `json:"attributes,omitempty"`
	Status      string    `json:"status,omitempty"` // draft, published, archived
	Metadata    Metadata  `json:"metadata,omitempty"`
}

// Blog represents a blog post content type
type Blog struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Slug        string    `json:"slug,omitempty"`
	Content     string    `json:"content,omitempty"`
	Excerpt     string    `json:"excerpt,omitempty"`
	Author      string    `json:"author,omitempty"`
	Tags        []string  `json:"tags,omitempty"`
	Categories  []string  `json:"categories,omitempty"`
	FeaturedImage string  `json:"featured_image,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Status      string    `json:"status,omitempty"` // draft, published, archived
	Metadata    Metadata  `json:"metadata,omitempty"`
}

// Page represents a page content type
type Page struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Slug        string    `json:"slug,omitempty"`
	Content     string    `json:"content,omitempty"`
	Template    string    `json:"template,omitempty"`
	Parent      string    `json:"parent,omitempty"`
	Order       int       `json:"order,omitempty"`
	SEO         SEO       `json:"seo,omitempty"`
	Status      string    `json:"status,omitempty"` // draft, published, archived
	Metadata    Metadata  `json:"metadata,omitempty"`
}

// Block represents a reusable content block
type Block struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Type        string                 `json:"type,omitempty"` // hero, cta, feature, etc.
	Content     map[string]interface{} `json:"content,omitempty"`
	Settings    map[string]interface{} `json:"settings,omitempty"`
	Status      string                 `json:"status,omitempty"`
	Metadata    Metadata               `json:"metadata,omitempty"`
}

// Section represents a page section
type Section struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Type        string                 `json:"type,omitempty"`
	Blocks      []string               `json:"blocks,omitempty"` // Block IDs
	Settings    map[string]interface{} `json:"settings,omitempty"`
	Order       int                    `json:"order,omitempty"`
	Status      string                 `json:"status,omitempty"`
	Metadata    Metadata               `json:"metadata,omitempty"`
}

// Image represents an image asset
type Image struct {
	ID          string    `json:"id"`
	Filename    string    `json:"filename"`
	AltText     string    `json:"alt_text,omitempty"`
	Caption     string    `json:"caption,omitempty"`
	MimeType    string    `json:"mime_type,omitempty"`
	Width       int       `json:"width,omitempty"`
	Height      int       `json:"height,omitempty"`
	Size        int64     `json:"size,omitempty"`
	URL         string    `json:"url,omitempty"`
	Metadata    Metadata  `json:"metadata,omitempty"`
}

// File represents a generic file asset
type File struct {
	ID          string    `json:"id"`
	Filename    string    `json:"filename"`
	MimeType    string    `json:"mime_type,omitempty"`
	Size        int64     `json:"size,omitempty"`
	URL         string    `json:"url,omitempty"`
	Metadata    Metadata  `json:"metadata,omitempty"`
}

// SEO contains SEO metadata
type SEO struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Keywords    string `json:"keywords,omitempty"`
	OGImage     string `json:"og_image,omitempty"`
	Canonical   string `json:"canonical,omitempty"`
	NoIndex     bool   `json:"no_index,omitempty"`
	NoFollow    bool   `json:"no_follow,omitempty"`
}

// Schema represents a content type schema definition
type Schema struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Fields      map[string]FieldDef    `json:"fields,omitempty"`
	Storage     StorageConfig          `json:"storage,omitempty"`
	Settings    map[string]interface{} `json:"settings,omitempty"`
}

// FieldDef defines a field in a schema
type FieldDef struct {
	Type        string                 `json:"type"`                  // string, number, boolean, array, object, image, file
	Required    bool                   `json:"required,omitempty"`
	Default     interface{}            `json:"default,omitempty"`
	Description string                 `json:"description,omitempty"`
	Items       string                 `json:"items,omitempty"`       // For array type
	Properties  map[string]FieldDef    `json:"properties,omitempty"`  // For object type
	Options     map[string]interface{} `json:"options,omitempty"`     // Additional field options
}

// StorageConfig defines how content of this type is stored
type StorageConfig struct {
	Extension string `json:"extension,omitempty"` // json, html, xml, etc.
	MimeType  string `json:"mime_type,omitempty"` // Default MIME type
}

// Version represents a content version
type Version struct {
	VersionID    string    `json:"version_id"`
	LastModified time.Time `json:"last_modified"`
	Size         int64     `json:"size"`
	IsLatest     bool      `json:"is_latest"`
}

// ListResponse is a generic list response
type ListResponse struct {
	Items      []interface{} `json:"items"`
	Count      int           `json:"count"`
	TotalCount int           `json:"total_count,omitempty"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// ErrorResponse represents an API error
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// ToJSON converts a model to JSON bytes
func ToJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// FromJSON parses JSON bytes into a model
func FromJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
