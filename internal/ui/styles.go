package ui

import (
	"fmt"

	"github.com/charmbracelet/lipgloss"
)

// Color palette
var (
	Primary    = lipgloss.Color("#FF5F1F") // Electric Orange - fast & energetic
	Secondary  = lipgloss.Color("#3B82F6") // Blue
	Success    = lipgloss.Color("#27C93F") // Green
	Warning    = lipgloss.Color("#F59E0B") // Amber
	Error      = lipgloss.Color("#EF4444") // Red
	MutedColor = lipgloss.Color("#888888") // Gray
	White      = lipgloss.Color("#FFFFFF") // White
)

// Styles
var (
	TitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(Primary).
			MarginBottom(1)

	SuccessStyle = lipgloss.NewStyle().
			Foreground(Success).
			Bold(true)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(Error).
			Bold(true)

	WarningStyle = lipgloss.NewStyle().
			Foreground(Warning)

	InfoStyle = lipgloss.NewStyle().
			Foreground(Secondary)

	MutedStyle = lipgloss.NewStyle().
			Foreground(MutedColor)

	BoldStyle = lipgloss.NewStyle().
			Bold(true)

	KeyStyle = lipgloss.NewStyle().
			Foreground(MutedColor)

	ValueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#E5E7EB"))

	HighlightStyle = lipgloss.NewStyle().
			Foreground(Primary).
			Bold(true)
)

// Banner returns the ASCII art banner for Velocity
func Banner() string {
	banner := `
 █   █ █▀▀ █   █▀▀█ █▀▀ ▀█▀ ▀▀█▀▀ █  █
 ▀▄ ▄▀ █▀▀ █   █  █ █    █    █   █▄▄█
   ▀   ▀▀▀ ▀▀▀ ▀▀▀▀ ▀▀▀ ▀▀▀   ▀   ▄▄▄█`
	return TitleStyle.Render(banner)
}

// Divider returns a styled divider line
func Divider() string {
	return MutedStyle.Render("──────────────────────────────────────────────")
}

// VersionLine returns the formatted version string
func VersionLine(version string) string {
	return ValueStyle.Render(" v" + version)
}

// PrintVersion prints the version
func PrintVersion(version string) {
	fmt.Println(VersionLine(version))
}

// HeaderString returns the full header as a string
func HeaderString(version string) string {
	return "\n" + Divider() + "\n" + Banner() + "\n" + VersionLine(version) + "\n\n" + Divider() + "\n"
}

// PrintHeader prints the full header with banner, dividers, and version
func PrintHeader(version string) {
	fmt.Println(HeaderString(version))
}

// Header returns a styled section header
func Header(text string) string {
	return BoldStyle.Render("▸ " + text)
}

// PrintSuccess prints a success message with checkmark
func PrintSuccess(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Println(SuccessStyle.Render("✓ " + msg))
}

// PrintError prints an error message with X mark
func PrintError(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Println(ErrorStyle.Render("✗ " + msg))
}

// PrintWarning prints a warning message
func PrintWarning(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Println(WarningStyle.Render("⚠ " + msg))
}

// PrintInfo prints an info message
func PrintInfo(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Println(InfoStyle.Render("• " + msg))
}

// PrintKeyValue prints a formatted key-value pair
func PrintKeyValue(key, value string) {
	fmt.Printf("%s: %s\n", KeyStyle.Render(key), ValueStyle.Render(value))
}

// Highlight returns highlighted text
func Highlight(s string) string {
	return HighlightStyle.Render(s)
}

// Muted returns muted/gray text
func Muted(s string) string {
	return MutedStyle.Render(s)
}

// Bold returns bold text
func Bold(s string) string {
	return BoldStyle.Render(s)
}
