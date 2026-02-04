package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var (
	ErrDeviceFlowExpired = errors.New("device code has expired")
	ErrNoToken           = errors.New("no Studio session token available")
	ErrTokenInvalid      = errors.New("studio session token is invalid or expired")
)

// DeviceFlowStart is the response from starting device flow
type DeviceFlowStart struct {
	DeviceCode      string `json:"device_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

// DeviceFlowResult is the successful result of device flow
type DeviceFlowResult struct {
	AccessToken string `json:"access_token"`
	UserEmail   string `json:"user_email"`
	UserID      string `json:"user_id"`
}

// Credentials stored in config file
type Credentials struct {
	StudioToken string `json:"studio_token"`
	UserEmail   string `json:"user_email,omitempty"`
	UserID      string `json:"user_id,omitempty"`
}

// TokenCacheEntry cached project token
type TokenCacheEntry struct {
	Token     string  `json:"token"`
	ExpiresAt float64 `json:"expires_at"`
}

// GetCredentialsPath returns the path to the credentials file
func GetCredentialsPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	configDir := filepath.Join(home, ".config", "coflux")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", err
	}
	return filepath.Join(configDir, "credentials.json"), nil
}

// LoadCredentials loads stored credentials
func LoadCredentials() (*Credentials, error) {
	path, err := GetCredentialsPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var creds Credentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, nil
	}
	return &creds, nil
}

// SaveCredentials saves credentials to disk
func SaveCredentials(creds *Credentials) error {
	path, err := GetCredentialsPath()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	return nil
}

// ClearCredentials removes stored credentials
func ClearCredentials() error {
	path, err := GetCredentialsPath()
	if err != nil {
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return ClearTokenCache()
}

// GetStudioToken returns the stored Studio session token
func GetStudioToken() (string, error) {
	creds, err := LoadCredentials()
	if err != nil {
		return "", err
	}
	if creds == nil || creds.StudioToken == "" {
		return "", ErrNoToken
	}
	return creds.StudioToken, nil
}

// StartDeviceFlow initiates the device flow authentication
func StartDeviceFlow(studioURL string) (*DeviceFlowStart, error) {
	url := studioURL + "/api/auth/device"

	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to start device flow: %s", string(body))
	}

	var result DeviceFlowStart
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// PollForToken polls for the device flow token until approved or expired
func PollForToken(deviceCode, studioURL string, interval, timeout int) (*DeviceFlowResult, error) {
	url := studioURL + "/api/auth/device/token"
	startTime := time.Now()
	client := &http.Client{}

	for time.Since(startTime).Seconds() < float64(timeout) {
		body := map[string]string{"device_code": deviceCode}
		jsonBody, _ := json.Marshal(body)

		req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusOK {
			var result DeviceFlowResult
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				_ = resp.Body.Close()
				return nil, err
			}
			_ = resp.Body.Close()
			return &result, nil
		}

		if resp.StatusCode == http.StatusBadRequest {
			var errResp struct {
				Error            string `json:"error"`
				ErrorDescription string `json:"error_description"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
				_ = resp.Body.Close()
				switch errResp.Error {
				case "authorization_pending":
					time.Sleep(time.Duration(interval) * time.Second)
					continue
				case "expired_token":
					return nil, ErrDeviceFlowExpired
				default:
					return nil, fmt.Errorf("%s: %s", errResp.Error, errResp.ErrorDescription)
				}
			}
		}

		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected response: %d", resp.StatusCode)
	}

	return nil, ErrDeviceFlowExpired
}

// Token cache functions

func getTokenCachePath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	configDir := filepath.Join(home, ".config", "coflux")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", err
	}
	return filepath.Join(configDir, "token_cache.json"), nil
}

func loadTokenCache() (map[string]TokenCacheEntry, error) {
	path, err := getTokenCachePath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return make(map[string]TokenCacheEntry), nil
	}
	if err != nil {
		return nil, err
	}

	var cache map[string]TokenCacheEntry
	if err := json.Unmarshal(data, &cache); err != nil {
		return make(map[string]TokenCacheEntry), nil
	}
	return cache, nil
}

func saveTokenCache(cache map[string]TokenCacheEntry) error {
	path, err := getTokenCachePath()
	if err != nil {
		return err
	}

	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0600)
}

// ClearTokenCache removes the token cache file
func ClearTokenCache() error {
	path, err := getTokenCachePath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ExchangeForProjectToken exchanges a Studio token for a project JWT
func ExchangeForProjectToken(team, host, studioURL, studioToken string) (string, int, error) {
	if studioToken == "" {
		var err error
		studioToken, err = GetStudioToken()
		if err != nil {
			return "", 0, err
		}
	}

	url := studioURL + "/api/auth/project-token"
	body := map[string]string{"team": team, "host": host}
	jsonBody, _ := json.Marshal(body)

	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+studioToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusUnauthorized {
		return "", 0, ErrTokenInvalid
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", 0, fmt.Errorf("failed to exchange token: %s", string(body))
	}

	var result struct {
		Token     string `json:"token"`
		ExpiresIn int    `json:"expiresIn"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, err
	}

	return result.Token, result.ExpiresIn, nil
}

// GetProjectToken gets a project token, using cache if available
func GetProjectToken(team, host, studioURL string, refreshBuffer int) (string, error) {
	cache, err := loadTokenCache()
	if err != nil {
		return "", err
	}

	key := team + ":" + host
	if entry, ok := cache[key]; ok {
		if float64(time.Now().Unix()) < entry.ExpiresAt-float64(refreshBuffer) {
			return entry.Token, nil
		}
	}

	// Exchange for new token
	token, expiresIn, err := ExchangeForProjectToken(team, host, studioURL, "")
	if err != nil {
		return "", err
	}

	// Cache the result
	cache[key] = TokenCacheEntry{
		Token:     token,
		ExpiresAt: float64(time.Now().Unix()) + float64(expiresIn),
	}
	if err := saveTokenCache(cache); err != nil {
		// Log warning but don't fail
	}

	return token, nil
}
