package version

import (
	"fmt"
	"strconv"
	"strings"
)

// Version is the full version string, set at build time via ldflags.
var Version = "dev"

// APIVersion derives the API compatibility version from the full version.
// Pre-1.0: "0.{minor}" (e.g., "0.8.1" -> "0.8")
// Post-1.0: "{major}" (e.g., "1.2.3" -> "1")
// Returns "dev" if version is "dev" or unparseable.
func APIVersion() string {
	if Version == "dev" || Version == "" {
		return "dev"
	}
	parts := strings.SplitN(Version, ".", 3)
	if len(parts) < 2 {
		return Version
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return Version
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return Version
	}
	if major == 0 {
		return fmt.Sprintf("0.%d", minor)
	}
	return strconv.Itoa(major)
}

// VersionMismatchError is returned when the server rejects a request
// due to an API version mismatch.
type VersionMismatchError struct {
	ServerVersion string
	ClientVersion string
}

func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf(
		"version mismatch: this CLI expects API version %s, but the server is version %s — upgrade the server or use a compatible CLI version",
		e.ClientVersion, e.ServerVersion,
	)
}
