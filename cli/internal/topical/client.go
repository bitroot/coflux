package topical

import (
	"context"
	"encoding/base64"
	"fmt"

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
	url := fmt.Sprintf("%s://%s/topics", scheme, host)

	opts := []topical.Option{
		topical.WithReconnect(true),
	}

	if token != "" {
		encoded := base64.RawURLEncoding.EncodeToString([]byte(token))
		opts = append(opts, topical.WithDialOptions(&websocket.DialOptions{
			Subprotocols: []string{fmt.Sprintf("bearer.%s", encoded), "v1"},
		}))
	}

	return topical.Connect(ctx, url, opts...)
}
