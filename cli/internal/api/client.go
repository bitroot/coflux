package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"

	"github.com/bitroot/coflux/cli/internal/version"
)

// Client provides HTTP API access to the Coflux server
type Client struct {
	baseURL string
	token   string
	project string
	client  *http.Client
}

// NewClient creates a new API client
func NewClient(host string, secure bool, token string, project string) *Client {
	scheme := "http"
	if secure {
		scheme = "https"
	}
	return &Client{
		baseURL: fmt.Sprintf("%s://%s", scheme, host),
		token:   token,
		project: project,
		client:  &http.Client{},
	}
}

// CreateSession creates a new worker session
func (c *Client) CreateSession(ctx context.Context, workspaceID string, provides map[string][]string, accepts map[string][]string) (string, error) {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	if provides != nil {
		body["provides"] = provides
	}
	if accepts != nil {
		body["accepts"] = accepts
	}

	var result struct {
		SessionID string `json:"sessionId"`
	}

	if _, err := c.post(ctx, "/api/create_session", body, &result); err != nil {
		return "", err
	}

	return result.SessionID, nil
}

// RegisterManifests registers workflow manifests
func (c *Client) RegisterManifests(ctx context.Context, workspaceID string, manifests map[string]map[string]any) error {
	body := map[string]any{
		"workspaceId": workspaceID,
		"manifests":   manifests,
	}

	_, err := c.post(ctx, "/api/register_manifests", body, nil)
	return err
}

// SubmitResult contains the IDs returned from submitting a workflow
type SubmitResult struct {
	RunID       string `json:"runId"`
	StepID      string `json:"stepId"`
	ExecutionID string `json:"executionId"`
}

// SubmitWorkflow submits a workflow for execution
func (c *Client) SubmitWorkflow(ctx context.Context, workspaceID, module, target string, arguments [][]any, options map[string]any) (*SubmitResult, error) {
	body := map[string]any{
		"workspaceId": workspaceID,
		"module":      module,
		"target":      target,
		"arguments":   arguments,
	}
	maps.Copy(body, options)

	var result SubmitResult
	if _, err := c.post(ctx, "/api/submit_workflow", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetManifests retrieves the latest manifests for a workspace
func (c *Client) GetManifests(ctx context.Context, workspaceID string) (map[string]any, error) {
	return c.CaptureTopic(ctx, "workspaces/"+workspaceID+"/manifests")
}

// GetWorkflow retrieves a workflow definition
func (c *Client) GetWorkflow(ctx context.Context, workspaceID, module, target string) (map[string]any, error) {
	result, err := c.CaptureTopic(ctx, "workspaces/"+workspaceID+"/workflows/"+module+"/"+target)
	if err != nil {
		return nil, err
	}
	// Flatten configuration into top level for callers that expect it there
	if config, ok := result["configuration"].(map[string]any); ok {
		for k, v := range config {
			result[k] = v
		}
		delete(result, "configuration")
	}
	return result, nil
}

// GetAssetByID retrieves asset information by ID
func (c *Client) GetAssetByID(ctx context.Context, assetID string) (map[string]any, error) {
	return c.CaptureTopic(ctx, "asset/"+assetID)
}

// Workspaces API

// GetWorkspaces lists all workspaces
func (c *Client) GetWorkspaces(ctx context.Context) (map[string]map[string]any, error) {
	var result map[string]map[string]any
	if err := c.get(ctx, "/topics/workspaces", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// CreateWorkspace creates a new workspace, returning its external ID
func (c *Client) CreateWorkspace(ctx context.Context, name string, baseID *string) (string, error) {
	body := map[string]any{
		"name": name,
	}
	if baseID != nil {
		body["baseId"] = *baseID
	}
	var result struct {
		ID string `json:"id"`
	}
	if _, err := c.post(ctx, "/api/create_workspace", body, &result); err != nil {
		return "", err
	}
	return result.ID, nil
}

// UpdateWorkspace updates a workspace
func (c *Client) UpdateWorkspace(ctx context.Context, workspaceID string, updates map[string]any) error {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	maps.Copy(body, updates)
	_, err := c.post(ctx, "/api/update_workspace", body, nil)
	return err
}

// ArchiveWorkspace archives a workspace
func (c *Client) ArchiveWorkspace(ctx context.Context, workspaceID string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	_, err := c.post(ctx, "/api/archive_workspace", body, nil)
	return err
}

// PauseWorkspace pauses a workspace (stops dispatching new executions)
func (c *Client) PauseWorkspace(ctx context.Context, workspaceID string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	_, err := c.post(ctx, "/api/pause_workspace", body, nil)
	return err
}

// ResumeWorkspace resumes a paused workspace
func (c *Client) ResumeWorkspace(ctx context.Context, workspaceID string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	_, err := c.post(ctx, "/api/resume_workspace", body, nil)
	return err
}

// ArchiveModule archives a module in a workspace (hides its targets)
func (c *Client) ArchiveModule(ctx context.Context, workspaceID, moduleName string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
		"moduleName":  moduleName,
	}
	_, err := c.post(ctx, "/api/archive_module", body, nil)
	return err
}

// Pools API

// GetPools lists all pools in a workspace
func (c *Client) GetPools(ctx context.Context, workspaceID string) (map[string]map[string]any, error) {
	var result map[string]map[string]any
	if err := c.get(ctx, "/topics/workspaces/"+workspaceID+"/pools", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetPool gets a single pool
func (c *Client) GetPool(ctx context.Context, workspaceID, pool string) (map[string]any, error) {
	result, err := c.CaptureTopic(ctx, "workspaces/"+workspaceID+"/pools/"+pool)
	if err != nil {
		return nil, err
	}
	// Extract the pool sub-object (topic returns {pool: {...}, workers: {...}})
	if p, ok := result["pool"].(map[string]any); ok {
		return p, nil
	}
	return result, nil
}

// CreatePool creates a new pool (fails if it already exists)
func (c *Client) CreatePool(ctx context.Context, workspaceID, poolName string, pool any) error {
	body := map[string]any{
		"workspaceId": workspaceID,
		"poolName":    poolName,
		"pool":        pool,
	}
	_, err := c.post(ctx, "/api/create_pool", body, nil)
	return err
}

// UpdatePool updates or creates a pool
func (c *Client) UpdatePool(ctx context.Context, workspaceID, poolName string, pool any) error {
	body := map[string]any{
		"workspaceId": workspaceID,
		"poolName":    poolName,
		"pool":        pool,
	}
	_, err := c.post(ctx, "/api/update_pool", body, nil)
	return err
}

func (c *Client) DisablePool(ctx context.Context, workspaceID, poolName string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
		"poolName":    poolName,
	}
	_, err := c.post(ctx, "/api/disable_pool", body, nil)
	return err
}

// ErrConflict indicates the pool configuration has changed since the ETag was fetched.
var ErrConflict = errors.New("conflict: pool configuration has changed, re-run to see updated plan")

// GetPoolConfigsResult holds the response from GetPoolConfigs.
type GetPoolConfigsResult struct {
	Pools map[string]map[string]any
	ETag  string
}

// GetPoolConfigs retrieves all pool configs for a workspace along with an ETag.
func (c *Client) GetPoolConfigs(ctx context.Context, workspaceID string) (*GetPoolConfigsResult, error) {
	body := map[string]any{"workspaceId": workspaceID}
	var pools map[string]map[string]any
	headers, err := c.post(ctx, "/api/get_pools", body, &pools)
	if err != nil {
		return nil, err
	}
	return &GetPoolConfigsResult{Pools: pools, ETag: headers.Get("ETag")}, nil
}

// UpdatePools atomically replaces all pool configs for a workspace.
// The ifMatch parameter should be the ETag from a previous GetPoolConfigs call.
func (c *Client) UpdatePools(ctx context.Context, workspaceID string, pools map[string]map[string]any, ifMatch string) error {
	jsonBody, err := json.Marshal(map[string]any{
		"workspaceId": workspaceID,
		"pools":       pools,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/update_pools", bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", ifMatch)
	c.setCommonHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode == 412 {
		return ErrConflict
	}

	if resp.StatusCode >= 400 {
		if err := checkVersionMismatch(resp.StatusCode, respBody); err != nil {
			return err
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (c *Client) EnablePool(ctx context.Context, workspaceID, poolName string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
		"poolName":    poolName,
	}
	_, err := c.post(ctx, "/api/enable_pool", body, nil)
	return err
}

// Tokens API

// ListTokens lists all service tokens
func (c *Client) ListTokens(ctx context.Context) ([]map[string]any, error) {
	var result map[string]map[string]any
	if err := c.get(ctx, "/topics/tokens", nil, &result); err != nil {
		return nil, err
	}
	tokens := make([]map[string]any, 0, len(result))
	for _, token := range result {
		tokens = append(tokens, token)
	}
	return tokens, nil
}

// CreateToken creates a new service token
func (c *Client) CreateToken(ctx context.Context, name string, workspaces []string) (string, error) {
	body := map[string]any{}
	if name != "" {
		body["name"] = name
	}
	if workspaces != nil {
		body["workspaces"] = workspaces
	}

	var result struct {
		Token string `json:"token"`
	}
	if _, err := c.post(ctx, "/api/create_token", body, &result); err != nil {
		return "", err
	}
	return result.Token, nil
}

// RevokeToken revokes a service token
func (c *Client) RevokeToken(ctx context.Context, externalID string) error {
	body := map[string]any{
		"externalId": externalID,
	}
	_, err := c.post(ctx, "/api/revoke_token", body, nil)
	return err
}

// RerunStepResult contains the IDs returned from re-running a step
type RerunStepResult struct {
	ExecutionID string `json:"executionId"`
	Attempt     int    `json:"attempt"`
}

// RerunStep triggers a re-run of an existing step
func (c *Client) RerunStep(ctx context.Context, workspaceID, stepID string) (*RerunStepResult, error) {
	body := map[string]any{
		"workspaceId": workspaceID,
		"stepId":      stepID,
	}
	var result RerunStepResult
	if _, err := c.post(ctx, "/api/rerun_step", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CancelExecution cancels a running or pending execution
func (c *Client) CancelExecution(ctx context.Context, workspaceID, executionID string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
		"executionId": executionID,
	}
	_, err := c.post(ctx, "/api/cancel_execution", body, nil)
	return err
}

// CaptureTopic captures a topic snapshot via the REST endpoint
func (c *Client) CaptureTopic(ctx context.Context, path string) (map[string]any, error) {
	var result map[string]any
	if err := c.get(ctx, "/topics/"+path, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// HTTP helpers

func (c *Client) setCommonHeaders(req *http.Request) {
	if apiVersion := version.APIVersion(); apiVersion != "dev" {
		req.Header.Set("X-API-Version", apiVersion)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.project != "" {
		req.Header.Set("X-Project", c.project)
	}
}

func (c *Client) get(ctx context.Context, path string, params url.Values, result any) error {
	reqURL := c.baseURL + path
	if params != nil {
		reqURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setCommonHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		if err := checkVersionMismatch(resp.StatusCode, respBody); err != nil {
			return err
		}
		var errResp struct {
			Error   string `json:"error"`
			Message string `json:"message"`
			Details any    `json:"details"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			if errResp.Details != nil {
				detailsJSON, _ := json.Marshal(errResp.Details)
				return fmt.Errorf("%s: %s (details: %s)", errResp.Error, errResp.Message, string(detailsJSON))
			}
			return fmt.Errorf("%s: %s", errResp.Error, errResp.Message)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}
	}

	return nil
}

func (c *Client) post(ctx context.Context, path string, body any, result any) (http.Header, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.setCommonHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		if err := checkVersionMismatch(resp.StatusCode, respBody); err != nil {
			return nil, err
		}
		var errResp struct {
			Error   string `json:"error"`
			Message string `json:"message"`
			Details any    `json:"details"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			if errResp.Details != nil {
				detailsJSON, _ := json.Marshal(errResp.Details)
				return nil, fmt.Errorf("%s: %s (details: %s)", errResp.Error, errResp.Message, string(detailsJSON))
			}
			return nil, fmt.Errorf("%s: %s", errResp.Error, errResp.Message)
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}
	}

	return resp.Header, nil
}

// checkVersionMismatch checks if an HTTP error response is a version mismatch
// and returns a typed VersionMismatchError if so.
func checkVersionMismatch(statusCode int, body []byte) error {
	if statusCode != 409 {
		return nil
	}
	var errResp struct {
		Error   string `json:"error"`
		Details struct {
			Server   string `json:"server"`
			Expected string `json:"expected"`
		} `json:"details"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error == "version_mismatch" {
		return &version.VersionMismatchError{
			ServerVersion: errResp.Details.Server,
			ClientVersion: version.APIVersion(),
		}
	}
	return nil
}
