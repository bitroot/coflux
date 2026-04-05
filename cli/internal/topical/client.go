package topical

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"

	"github.com/bitroot/coflux/cli/internal/version"
	"github.com/coder/websocket"
	topical "github.com/joefreeman/topical/client_go"
)

// Connect establishes a Topical WebSocket connection to the Coflux server.
// Authentication uses the bearer token subprotocol convention.
func Connect(ctx context.Context, host string, secure bool, token string, project string) (*topical.Client, error) {
	scheme := "ws"
	if secure {
		scheme = "wss"
	}
	wsURL := fmt.Sprintf("%s://%s/topics", scheme, host)
	if apiVersion := version.APIVersion(); apiVersion != "dev" {
		params := url.Values{}
		params.Set("version", apiVersion)
		wsURL += "?" + params.Encode()
	}

	opts := []topical.Option{
		topical.WithReconnect(true),
	}

	dialOpts := &websocket.DialOptions{}

	if token != "" {
		encoded := base64.RawURLEncoding.EncodeToString([]byte(token))
		dialOpts.Subprotocols = []string{fmt.Sprintf("bearer.%s", encoded), "v1"}
	}

	if project != "" {
		dialOpts.HTTPHeader = http.Header{"X-Project": {project}}
	}

	if len(dialOpts.Subprotocols) > 0 || dialOpts.HTTPHeader != nil {
		opts = append(opts, topical.WithDialOptions(dialOpts))
	}

	return topical.Connect(ctx, wsURL, opts...)
}
