package config

// Config represents the coflux.toml configuration file.
// Defaults are set via viper.SetDefault() in cmd/coflux/main.go.
type Config struct {
	Workspace   string              `mapstructure:"workspace"`
	Team        string              `mapstructure:"team"`
	Modules     []string            `mapstructure:"modules"`
	Adapter     []string            `mapstructure:"adapter"`
	Concurrency int                 `mapstructure:"concurrency"`
	Session     string              `mapstructure:"session"` // Pre-existing session ID (for pool-launched workers)
	Server      ServerConfig        `mapstructure:"server"`
	Provides    map[string][]string `mapstructure:"provides"`
	Serializers []SerializerConfig  `mapstructure:"serializers"`
	Blobs       BlobsConfig         `mapstructure:"blobs"`
	Logs        LogsConfig          `mapstructure:"logs"`
}

// ServerConfig holds server connection settings
type ServerConfig struct {
	Host   string `mapstructure:"host"`
	Token  string `mapstructure:"token"`
	Secure *bool  `mapstructure:"secure"`
}

// SerializerConfig holds serializer configuration
type SerializerConfig struct {
	Type   string `mapstructure:"type"`
	Module string `mapstructure:"module"`
	Class  string `mapstructure:"class"`
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
	URL string `mapstructure:"url"`
	// S3 store fields
	Bucket string `mapstructure:"bucket"`
	Prefix string `mapstructure:"prefix"`
	Region string `mapstructure:"region"`
}

// LogsConfig holds log storage configuration
type LogsConfig struct {
	Store LogStoreConfig `mapstructure:"store"`
}

// LogStoreConfig represents a log store configuration
type LogStoreConfig struct {
	Type          string  `mapstructure:"type"`
	URL           string  `mapstructure:"url"`
	BatchSize     int     `mapstructure:"batch_size"`
	FlushInterval float64 `mapstructure:"flush_interval"`
}

// IsSecure determines if the connection should use TLS
func (c *Config) IsSecure() bool {
	if c.Server.Secure != nil {
		return *c.Server.Secure
	}
	// Default: localhost uses HTTP, others use HTTPS
	return !IsLocalhost(c.Server.Host)
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
	return scheme + "://" + c.Server.Host
}

// WebSocketURL returns the WebSocket URL for the server
func (c *Config) WebSocketURL() string {
	scheme := "ws"
	if c.IsSecure() {
		scheme = "wss"
	}
	return scheme + "://" + c.Server.Host
}
