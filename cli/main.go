package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"velocity/internal/ui"
	"velocity/internal/version"
)

var (
	endpoint  string
	apiKey    string
	outputFmt string
	dataFlag  string
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
		Use:   "create <type>",
		Short: "Create a content item",
		Args:  cobra.ExactArgs(1),
		Run:   runCreate,
	}
	createCmd.Flags().StringVarP(&dataFlag, "data", "d", "", "JSON data for the content item")

	updateCmd := &cobra.Command{
		Use:   "update <type> <id>",
		Short: "Update a content item",
		Args:  cobra.ExactArgs(2),
		Run:   runUpdate,
	}
	updateCmd.Flags().StringVarP(&dataFlag, "data", "d", "", "JSON data for the content item")

	deleteCmd := &cobra.Command{
		Use:   "delete <type> <id>",
		Short: "Delete a content item",
		Args:  cobra.ExactArgs(2),
		Run:   runDelete,
	}

	contentCmd.AddCommand(listCmd, getCmd, createCmd, updateCmd, deleteCmd)
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
	client := newClient()

	data, err := parseData()
	if err != nil {
		ui.PrintError("Failed to parse data: %v", err)
		os.Exit(1)
	}

	result, err := client.createContent(contentType, data)
	if err != nil {
		ui.PrintError("Failed to create content: %v", err)
		os.Exit(1)
	}

	if outputFmt == "json" {
		printJSON(result)
		return
	}

	ui.PrintSuccess("Created %s: %s", contentType, getField(result, "id"))
}

func runUpdate(cmd *cobra.Command, args []string) {
	contentType := args[0]
	id := args[1]
	client := newClient()

	data, err := parseData()
	if err != nil {
		ui.PrintError("Failed to parse data: %v", err)
		os.Exit(1)
	}

	_, err = client.updateContent(contentType, id, data)
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
	httpClient *http.Client
}

func newClient() *client {
	return &client{
		baseURL: endpoint,
		apiKey:  apiKey,
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

func (c *client) createContent(contentType string, body map[string]interface{}) (map[string]interface{}, error) {
	data, err := c.request("POST", "/api/content/"+contentType, body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *client) updateContent(contentType, id string, body map[string]interface{}) (map[string]interface{}, error) {
	data, err := c.request("PUT", "/api/content/"+contentType+"/"+id, body)
	if err != nil {
		return nil, err
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
