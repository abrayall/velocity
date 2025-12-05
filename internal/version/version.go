package version

// Version is set at build time via ldflags
var Version = "0.1.0-dev"

// GetVersion returns the current version
func GetVersion() string {
	return Version
}
