package topical

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"

	"github.com/bitroot/coflux/cli/internal/version"
	"github.com/coder/websocket"
	topical "github.com/joefreeman/topical/client_go"
)

// Connect establishes a Topical WebSocket connection to the Coflux server.
// Authentication uses the bearer token subprotocol convention.
func Connect(ctx context.Context, host string, secure bool, token string) (*topical.Client, error) {
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

	if token != "" {
		encoded := base64.RawURLEncoding.EncodeToString([]byte(token))
		opts = append(opts, topical.WithDialOptions(&websocket.DialOptions{
			Subprotocols: []string{fmt.Sprintf("bearer.%s", encoded), "v1"},
		}))
	}

	return topical.Connect(ctx, wsURL, opts...)
}
