package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"velocity/internal/ui"
	"velocity/internal/version"
)

var (
	endpoint     string
	apiKey       string
	tenant       string
	outputFmt    string
	dataFlag     string
	fileFlag     string
	metadataFlag string
)

var rootCmd = &cobra.Command{
	Use:   "velocity",
	Short: "Velocity CMS - Fast content management",
}

func init() {
	// Override templates to show header
	cobra.AddTemplateFunc("header", func() string {
		return ui.HeaderString(version.GetVersion())
	})
	rootCmd.SetHelpTemplate(`{{ header }}
Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`)
	rootCmd.SetUsageTemplate(`{{ header }}
Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		ui.PrintError("%v", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", getEnv("VELOCITY_ENDPOINT", "http://localhost:8080"), "API endpoint URL")
	rootCmd.PersistentFlags().StringVar(&apiKey, "api-key", getEnv("VELOCITY_API_KEY", ""), "API key for authentication")
	rootCmd.PersistentFlags().StringVar(&tenant, "tenant", getEnv("VELOCITY_TENANT", "demo"), "Tenant identifier")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "table", "Output format (table, json)")

	// Version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			ui.PrintHeader(version.GetVersion())
		},
	})

	// Types command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "types",
		Short: "List available content types",
		Run:   runTypes,
	})

	// Content command group
	contentCmd := &cobra.Command{
		Use:   "content",
		Short: "Manage content",
	}

	listCmd := &cobra.Command{
		Use:   "list <type>",
		Short: "List content items",
		Args:  cobra.ExactArgs(1),
		Run:   runList,
	}

	getCmd := &cobra.Command{
		Use:   "get <type> <id>",
		Short: "Get a content item",
		Args:  cobra.ExactArgs(2),
		Run:   runGet,
	}

	createCmd := &cobra.Command{
		Use:   "create <type> <id>",
		Short: "Create a content item",
		Args:  cobra.ExactArgs(2),
		Run:   runCreate,
	}
	createCmd.Flags().StringVarP(&dataFlag, "data", "d", "", "JSON data for the content item")
	createCmd.Flags().StringVarP(&fileFlag, "file", "f", "", "File to upload")
	createCmd.Flags().StringVarP(&metadataFlag, "metadata", "m", "", "Metadata as JSON or key:value,key:value format")

	updateCmd := &cobra.Command{
		Use:   "update <type> <id>",
		Short: "Update a content item",
		Args:  cobra.ExactArgs(2),
		Run:   runUpdate,
	}
	updateCmd.Flags().StringVarP(&dataFlag, "data", "d", "", "JSON data for the content item")
	updateCmd.Flags().StringVarP(&fileFlag, "file", "f", "", "File to upload")
	updateCmd.Flags().StringVarP(&metadataFlag, "metadata", "m", "", "Metadata as JSON or key:value,key:value format")

	deleteCmd := &cobra.Command{
		Use:   "delete <type> <id>",
		Short: "Delete a content item",
		Args:  cobra.ExactArgs(2),
		Run:   runDelete,
	}

	// Metadata subcommand group
	metadataCmd := &cobra.Command{
		Use:   "metadata",
		Short: "Manage content metadata",
	}

	metadataGetCmd := &cobra.Command{
		Use:   "get <type> <id>",
		Short: "Get metadata for a content item",
		Args:  cobra.ExactArgs(2),
		Run:   runMetadataGet,
	}

	metadataSetCmd := &cobra.Command{
		Use:   "set <type> <id>",
		Short: "Set metadata on a content item (replaces all)",
		Args:  cobra.ExactArgs(2),
		Run:   runMetadataSet,
	}
	metadataSetCmd.Flags().StringVarP(&metadataFlag, "metadata", "m", "", "Metadata as JSON or key:value,key:value format")

	metadataUpdateCmd := &cobra.Command{
		Use:   "update <type> <id>",
		Short: "Update/merge metadata on a content item",
		Args:  cobra.ExactArgs(2),
		Run:   runMetadataUpdate,
	}
	metadataUpdateCmd.Flags().StringVarP(&metadataFlag, "metadata", "m", "", "Metadata as JSON or key:value,key:value format")

	metadataRemoveCmd := &cobra.Command{
		Use:   "remove <type> <id> <key> [keys...]",
		Short: "Remove metadata keys from a content item",
		Args:  cobra.MinimumNArgs(3),
		Run:   runMetadataRemove,
	}

	metadataCmd.AddCommand(metadataGetCmd, metadataSetCmd, metadataUpdateCmd, metadataRemoveCmd)
	contentCmd.AddCommand(listCmd, getCmd, createCmd, updateCmd, deleteCmd, metadataCmd)
	rootCmd.AddCommand(contentCmd)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Command handlers

func runTypes(cmd *cobra.Command, args []string) {
	client := newClient()
	types, err := client.listTypes()
	if err != nil {
		ui.PrintError("Failed to list types: %v", err)
		os.Exit(1)
	}

	if outputFmt == "json" {
		printJSON(types)
		return
	}

	fmt.Println(ui.Header("Content Types"))
	for _, t := range types {
		fmt.Printf("  %s\n", t)
	}
}

func runList(cmd *cobra.Command, args []string) {
	contentType := args[0]
	client := newClient()

	items, err := client.listContent(contentType)
	if err != nil {
		ui.PrintError("Failed to list content: %v", err)
		os.Exit(1)
	}

	if outputFmt == "json" {
		printJSON(items)
		return
	}

	fmt.Println(ui.Header(fmt.Sprintf("%s Items", strings.Title(contentType))))

	if len(items) == 0 {
		fmt.Println("  No items found")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  ID\tNAME\tSTATUS")
	fmt.Fprintln(w, "  --\t----\t------")

	for _, item := range items {
		id := getField(item, "id")
		name := getField(item, "name", "title")
		status := getField(item, "status")
		fmt.Fprintf(w, "  %s\t%s\t%s\n", id, name, status)
	}
	w.Flush()
}

func runGet(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	client := newClient()

	item, err := client.getContent(contentType, id)
	if err != nil {
		ui.PrintError("Failed to get content: %v", err)
		os.Exit(1)
	}

	if outputFmt == "json" {
		printJSON(item)
		return
	}

	fmt.Println(ui.Header(fmt.Sprintf("%s: %s", strings.Title(contentType), id)))
	printFields(item, "  ")
}

func runCreate(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	client := newClient()

	metadata, err := parseMetadata()
	if err != nil {
		ui.PrintError("Failed to parse metadata: %v", err)
		os.Exit(1)
	}

	var result map[string]interface{}

	if fileFlag != "" {
		result, err = client.uploadFile(contentType, id, fileFlag, metadata)
	} else {
		var data map[string]interface{}
		data, err = parseData()
		if err != nil {
			ui.PrintError("Failed to parse data: %v", err)
			os.Exit(1)
		}
		result, err = client.createContentWithMetadata(contentType, id, data, metadata)
	}

	if err != nil {
		ui.PrintError("Failed to create content: %v", err)
		os.Exit(1)
	}

	if outputFmt == "json" {
		printJSON(result)
		return
	}

	ui.PrintSuccess("Created %s: %s", contentType, id)
}

func runUpdate(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	client := newClient()

	metadata, err := parseMetadata()
	if err != nil {
		ui.PrintError("Failed to parse metadata: %v", err)
		os.Exit(1)
	}

	if fileFlag != "" {
		_, err = client.uploadFile(contentType, id, fileFlag, metadata)
	} else {
		var data map[string]interface{}
		data, err = parseData()
		if err != nil {
			ui.PrintError("Failed to parse data: %v", err)
			os.Exit(1)
		}
		_, err = client.updateContentWithMetadata(contentType, id, data, metadata)
	}

	if err != nil {
		ui.PrintError("Failed to update content: %v", err)
		os.Exit(1)
	}

	ui.PrintSuccess("Updated %s: %s", contentType, id)
}

func runDelete(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	client := newClient()

	if err := client.deleteContent(contentType, id); err != nil {
		ui.PrintError("Failed to delete content: %v", err)
		os.Exit(1)
	}

	ui.PrintSuccess("Deleted %s: %s", contentType, id)
}

func runMetadataGet(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	client := newClient()

	metadata, err := client.getMetadata(contentType, id)
	if err != nil {
		ui.PrintError("Failed to get metadata: %v", err)
		os.Exit(1)
	}

	if outputFmt == "json" {
		printJSON(metadata)
		return
	}

	fmt.Println(ui.Header(fmt.Sprintf("Metadata: %s/%s", contentType, id)))
	if len(metadata) == 0 {
		fmt.Println("  No metadata")
		return
	}
	for key, value := range metadata {
		fmt.Printf("  %s: %s\n", key, value)
	}
}

func runMetadataSet(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	client := newClient()

	metadata, err := parseMetadata()
	if err != nil {
		ui.PrintError("Failed to parse metadata: %v", err)
		os.Exit(1)
	}

	if metadata == nil {
		ui.PrintError("No metadata provided, use --metadata flag")
		os.Exit(1)
	}

	if err := client.setMetadata(contentType, id, metadata); err != nil {
		ui.PrintError("Failed to set metadata: %v", err)
		os.Exit(1)
	}

	ui.PrintSuccess("Set metadata on %s/%s", contentType, id)
}

func runMetadataUpdate(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	client := newClient()

	metadata, err := parseMetadata()
	if err != nil {
		ui.PrintError("Failed to parse metadata: %v", err)
		os.Exit(1)
	}

	if metadata == nil {
		ui.PrintError("No metadata provided, use --metadata flag")
		os.Exit(1)
	}

	if err := client.updateMetadata(contentType, id, metadata); err != nil {
		ui.PrintError("Failed to update metadata: %v", err)
		os.Exit(1)
	}

	ui.PrintSuccess("Updated metadata on %s/%s", contentType, id)
}

func runMetadataRemove(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	keys := args[2:]
	client := newClient()

	if err := client.removeMetadata(contentType, id, keys); err != nil {
		ui.PrintError("Failed to remove metadata: %v", err)
		os.Exit(1)
	}

	ui.PrintSuccess("Removed metadata keys from %s/%s", contentType, id)
}

// Helpers

func parseData() (map[string]interface{}, error) {
	var jsonData string

	if dataFlag != "" {
		jsonData = dataFlag
	} else {
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			data, err := io.ReadAll(os.Stdin)
			if err != nil {
				return nil, err
			}
			jsonData = string(data)
		}
	}

	if jsonData == "" {
		return nil, fmt.Errorf("no data provided, use --data or pipe JSON via stdin")
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(jsonData), &result); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	return result, nil
}

// parseMetadata parses metadata from --metadata flag
// Supports JSON format: {"key":"value"} or key:value format: key1:value1,key2:value2
func parseMetadata() (map[string]string, error) {
	if metadataFlag == "" {
		return nil, nil
	}

	// Try JSON first
	if strings.HasPrefix(strings.TrimSpace(metadataFlag), "{") {
		var result map[string]string
		if err := json.Unmarshal([]byte(metadataFlag), &result); err != nil {
			return nil, fmt.Errorf("invalid JSON metadata: %w", err)
		}
		return result, nil
	}

	// Parse key:value,key:value format
	result := make(map[string]string)
	pairs := strings.Split(metadataFlag, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid metadata format '%s', expected key:value", pair)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return nil, fmt.Errorf("metadata key cannot be empty")
		}
		result[key] = value
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func printJSON(v interface{}) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}

func printFields(item map[string]interface{}, prefix string) {
	for key, value := range item {
		switch v := value.(type) {
		case map[string]interface{}:
			fmt.Printf("%s%s:\n", prefix, key)
			printFields(v, prefix+"  ")
		default:
			fmt.Printf("%s%s: %v\n", prefix, key, v)
		}
	}
}

func getField(item map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := item[key]; ok {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

// HTTP Client

type client struct {
	baseURL    string
	apiKey     string
	tenant     string
	httpClient *http.Client
}

func newClient() *client {
	return &client{
		baseURL: endpoint,
		apiKey:  apiKey,
		tenant:  tenant,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *client) request(method, path string, body interface{}) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequest(method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	if c.tenant != "" {
		req.Header.Set("X-Tenant", c.tenant)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, string(data))
	}

	return data, nil
}

func (c *client) listTypes() ([]string, error) {
	data, err := c.request("GET", "/api/types", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Types []string `json:"types"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result.Types, nil
}

func (c *client) listContent(contentType string) ([]map[string]interface{}, error) {
	data, err := c.request("GET", "/api/content/"+contentType, nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Items []map[string]interface{} `json:"items"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result.Items, nil
}

func (c *client) getContent(contentType, id string) (map[string]interface{}, error) {
	data, err := c.request("GET", "/api/content/"+contentType+"/"+id, nil)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *client) createContent(contentType, id string, body map[string]interface{}) (map[string]interface{}, error) {
	return c.createContentWithMetadata(contentType, id, body, nil)
}

func (c *client) createContentWithMetadata(contentType, id string, body map[string]interface{}, metadata map[string]string) (map[string]interface{}, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal body: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/api/content/"+contentType+"/"+id, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	if c.tenant != "" {
		req.Header.Set("X-Tenant", c.tenant)
	}

	// Add metadata headers
	for key, value := range metadata {
		req.Header.Set("X-Meta-"+key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, string(data))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *client) uploadFile(contentType, id, filePath string, metadata map[string]string) (map[string]interface{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Detect content type from file extension
	ext := filepath.Ext(filePath)
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	req, err := http.NewRequest("POST", c.baseURL+"/api/content/"+contentType+"/"+id, file)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", mimeType)
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	if c.tenant != "" {
		req.Header.Set("X-Tenant", c.tenant)
	}

	// Add metadata headers
	for key, value := range metadata {
		req.Header.Set("X-Meta-"+key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, string(data))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *client) updateContent(contentType, id string, body map[string]interface{}) (map[string]interface{}, error) {
	return c.updateContentWithMetadata(contentType, id, body, nil)
}

func (c *client) updateContentWithMetadata(contentType, id string, body map[string]interface{}, metadata map[string]string) (map[string]interface{}, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal body: %w", err)
	}

	req, err := http.NewRequest("PUT", c.baseURL+"/api/content/"+contentType+"/"+id, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	if c.tenant != "" {
		req.Header.Set("X-Tenant", c.tenant)
	}

	// Add metadata headers
	for key, value := range metadata {
		req.Header.Set("X-Meta-"+key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, string(data))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *client) deleteContent(contentType, id string) error {
	_, err := c.request("DELETE", "/api/content/"+contentType+"/"+id, nil)
	return err
}

func (c *client) getMetadata(contentType, id string) (map[string]string, error) {
	data, err := c.request("GET", "/api/content/"+contentType+"/"+id+"/metadata", nil)
	if err != nil {
		return nil, err
	}

	var result map[string]string
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *client) setMetadata(contentType, id string, metadata map[string]string) error {
	_, err := c.request("PUT", "/api/content/"+contentType+"/"+id+"/metadata", metadata)
	return err
}

func (c *client) updateMetadata(contentType, id string, metadata map[string]string) error {
	_, err := c.request("PATCH", "/api/content/"+contentType+"/"+id+"/metadata", metadata)
	return err
}

func (c *client) removeMetadata(contentType, id string, keys []string) error {
	body := map[string][]string{"keys": keys}
	_, err := c.request("DELETE", "/api/content/"+contentType+"/"+id+"/metadata", body)
	return err
}
