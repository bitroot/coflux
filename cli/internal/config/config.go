package config

import "strings"

// Config represents the coflux.toml configuration file.
// Defaults are set via viper.SetDefault() in cmd/coflux/main.go.
type Config struct {
	Host      string       `mapstructure:"host"`
	Token     string       `mapstructure:"token"`
	Secure    *bool        `mapstructure:"secure"`
	Workspace string       `mapstructure:"workspace"`
	Team      string       `mapstructure:"team"`
	Server    ServerConfig `mapstructure:"server"`
	Worker    WorkerConfig `mapstructure:"worker"`
	Blobs     BlobsConfig   `mapstructure:"blobs"`
	Logs      LogsConfig    `mapstructure:"logs"`
	Metrics   MetricsConfig `mapstructure:"metrics"`
	LogLevel  string       `mapstructure:"log_level"`
	Output    string       `mapstructure:"output"`
}

// ServerConfig holds settings for running a local server via `coflux server`.
type ServerConfig struct {
	Port           int      `mapstructure:"port"`
	DataDir        string   `mapstructure:"data_dir"`
	Image          string   `mapstructure:"image"`
	Project        string   `mapstructure:"project"`
	PublicHost     string   `mapstructure:"public_host"`
	Auth           *bool    `mapstructure:"auth"`
	SuperToken     string   `mapstructure:"super_token"`
	SuperTokenHash string   `mapstructure:"super_token_hash"`
	Secret         string   `mapstructure:"secret"`
	StudioTeams    []string `mapstructure:"studio_teams"`
	StudioURL      string   `mapstructure:"studio_url"`
	AllowOrigins   []string `mapstructure:"allow_origins"`
	LauncherTypes  []string `mapstructure:"launcher_types"`
}

// WorkerConfig holds worker-specific settings
type WorkerConfig struct {
	Adapter     []string `mapstructure:"adapter"`
	Concurrency int      `mapstructure:"concurrency"`
	Provides    []string `mapstructure:"provides"`
}

// ParseProvides converts a flat provides list (e.g., ["gpu:A100", "gpu:H100", "region:eu"])
// into a grouped map (e.g., {"gpu": ["A100", "H100"], "region": ["eu"]}).
// Strings without ":" are treated as boolean tags with value "true".
func ParseProvides(provides []string) map[string][]string {
	result := make(map[string][]string)
	for _, p := range provides {
		if key, value, ok := strings.Cut(p, ":"); ok {
			result[key] = append(result[key], value)
		} else {
			result[p] = append(result[p], "true")
		}
	}
	return result
}

// BlobsConfig holds blob storage configuration
type BlobsConfig struct {
	Threshold int               `mapstructure:"threshold"`
	Stores    []BlobStoreConfig `mapstructure:"stores"`
}

// BlobStoreConfig represents a blob store configuration
type BlobStoreConfig struct {
	Type string `mapstructure:"type"`
	// HTTP store fields
	URL   string  `mapstructure:"url"`
	Token *string `mapstructure:"token"`
	// S3 store fields
	Bucket string `mapstructure:"bucket"`
	Prefix string `mapstructure:"prefix"`
	Region string `mapstructure:"region"`
}

// LogsConfig holds log storage configuration
type LogsConfig struct {
	Type          string  `mapstructure:"type"`
	Token         *string `mapstructure:"token"`
	URL           string  `mapstructure:"url"`
	BatchSize     int     `mapstructure:"batch_size"`
	FlushInterval float64 `mapstructure:"flush_interval"`
}

// MetricsConfig holds metric storage configuration
type MetricsConfig struct {
	Type          string  `mapstructure:"type"`
	Token         *string `mapstructure:"token"`
	URL           string  `mapstructure:"url"`
	BatchSize     int     `mapstructure:"batch_size"`
	FlushInterval float64 `mapstructure:"flush_interval"`
	ThrottleRate  float64 `mapstructure:"throttle_rate"` // max points per key per second
}

// IsSecure determines if the connection should use TLS
func (c *Config) IsSecure() bool {
	if c.Secure != nil {
		return *c.Secure
	}
	// Default: localhost uses HTTP, others use HTTPS
	return !IsLocalhost(c.Host)
}

// IsLocalhost checks if the host is localhost-like.
// Handles localhost, *.localhost, 127.0.0.1, [::1], all with optional port.
func IsLocalhost(host string) bool {
	// Handle IPv6 addresses in brackets (e.g., [::1]:7777)
	if len(host) > 0 && host[0] == '[' {
		bracketEnd := -1
		for i, c := range host {
			if c == ']' {
				bracketEnd = i
				break
			}
		}
		if bracketEnd != -1 {
			return host[1:bracketEnd] == "::1"
		}
		return false
	}

	// Remove port suffix for regular hosts
	hostname := host
	for i := len(host) - 1; i >= 0; i-- {
		if host[i] == ':' {
			hostname = host[:i]
			break
		}
	}

	// Check for localhost, subdomains of localhost, or IPv4 loopback
	return hostname == "localhost" ||
		(len(hostname) > 10 && hostname[len(hostname)-10:] == ".localhost") ||
		hostname == "127.0.0.1"
}

// HTTPURL returns the HTTP base URL for the server
func (c *Config) HTTPURL() string {
	scheme := "http"
	if c.IsSecure() {
		scheme = "https"
	}
	return scheme + "://" + c.Host
}

// WebSocketURL returns the WebSocket URL for the server
func (c *Config) WebSocketURL() string {
	scheme := "ws"
	if c.IsSecure() {
		scheme = "wss"
	}
	return scheme + "://" + c.Host
}
