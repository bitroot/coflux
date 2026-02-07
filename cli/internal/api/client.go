package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
)

// Client provides HTTP API access to the Coflux server
type Client struct {
	baseURL string
	token   string
	client  *http.Client
}

// NewClient creates a new API client
func NewClient(host string, secure bool, token string) *Client {
	scheme := "http"
	if secure {
		scheme = "https"
	}
	return &Client{
		baseURL: fmt.Sprintf("%s://%s", scheme, host),
		token:   token,
		client:  &http.Client{},
	}
}

// CreateSession creates a new worker session
func (c *Client) CreateSession(ctx context.Context, workspace string, provides map[string][]string, concurrency int) (string, error) {
	body := map[string]any{
		"workspaceName": workspace,
	}
	if provides != nil {
		body["provides"] = provides
	}
	if concurrency > 0 {
		body["concurrency"] = concurrency
	}

	var result struct {
		SessionID string `json:"sessionId"`
	}

	if err := c.post(ctx, "/api/create_session", body, &result); err != nil {
		return "", err
	}

	return result.SessionID, nil
}

// RegisterManifests registers workflow manifests
func (c *Client) RegisterManifests(ctx context.Context, workspace string, manifests map[string]map[string]any) error {
	body := map[string]any{
		"workspaceName": workspace,
		"manifests":     manifests,
	}

	return c.post(ctx, "/api/register_manifests", body, nil)
}

// SubmitResult contains the IDs returned from submitting a workflow
type SubmitResult struct {
	RunID       string      `json:"runId"`
	StepID      string      `json:"stepId"`
	ExecutionID json.Number `json:"executionId"`
}

// SubmitWorkflow submits a workflow for execution
func (c *Client) SubmitWorkflow(ctx context.Context, workspace, module, target string, arguments [][]any, options map[string]any) (*SubmitResult, error) {
	body := map[string]any{
		"workspaceName": workspace,
		"module":        module,
		"target":        target,
		"arguments":     arguments,
	}
	maps.Copy(body, options)

	var result SubmitResult
	if err := c.post(ctx, "/api/submit_workflow", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetManifests retrieves the latest manifests for a workspace
func (c *Client) GetManifests(ctx context.Context, workspace string) (map[string]any, error) {
	var result map[string]any
	params := url.Values{
		"workspace": {workspace},
	}
	if err := c.get(ctx, "/api/get_manifests", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetWorkflow retrieves a workflow definition
func (c *Client) GetWorkflow(ctx context.Context, workspace, module, target string) (map[string]any, error) {
	var result map[string]any
	params := url.Values{
		"workspace": {workspace},
		"module":    {module},
		"target":    {target},
	}
	if err := c.get(ctx, "/api/get_workflow", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetAssetByID retrieves asset information by ID
func (c *Client) GetAssetByID(ctx context.Context, assetID string) (map[string]any, error) {
	var result map[string]any
	params := url.Values{"asset": {assetID}}
	if err := c.get(ctx, "/api/get_asset", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Workspaces API

// GetWorkspaces lists all workspaces
func (c *Client) GetWorkspaces(ctx context.Context) (map[string]map[string]any, error) {
	var result map[string]map[string]any
	if err := c.get(ctx, "/api/get_workspaces", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// CreateWorkspace creates a new workspace
func (c *Client) CreateWorkspace(ctx context.Context, name string, baseID *string) error {
	body := map[string]any{
		"name": name,
	}
	if baseID != nil {
		body["baseId"] = *baseID
	}
	return c.post(ctx, "/api/create_workspace", body, nil)
}

// UpdateWorkspace updates a workspace
func (c *Client) UpdateWorkspace(ctx context.Context, workspaceID string, updates map[string]any) error {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	maps.Copy(body, updates)
	return c.post(ctx, "/api/update_workspace", body, nil)
}

// ArchiveWorkspace archives a workspace
func (c *Client) ArchiveWorkspace(ctx context.Context, workspaceID string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	return c.post(ctx, "/api/archive_workspace", body, nil)
}

// PauseWorkspace pauses a workspace (stops dispatching new executions)
func (c *Client) PauseWorkspace(ctx context.Context, workspaceID string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	return c.post(ctx, "/api/pause_workspace", body, nil)
}

// ResumeWorkspace resumes a paused workspace
func (c *Client) ResumeWorkspace(ctx context.Context, workspaceID string) error {
	body := map[string]any{
		"workspaceId": workspaceID,
	}
	return c.post(ctx, "/api/resume_workspace", body, nil)
}

// ArchiveModule archives a module in a workspace (hides its targets)
func (c *Client) ArchiveModule(ctx context.Context, workspace, moduleName string) error {
	body := map[string]any{
		"workspaceName": workspace,
		"moduleName":    moduleName,
	}
	return c.post(ctx, "/api/archive_module", body, nil)
}

// Pools API

// GetPools lists all pools in a workspace
func (c *Client) GetPools(ctx context.Context, workspace string) (map[string]map[string]any, error) {
	var result map[string]map[string]any
	params := url.Values{"workspace": {workspace}}
	if err := c.get(ctx, "/api/get_pools", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetPool gets a single pool
func (c *Client) GetPool(ctx context.Context, workspace, pool string) (map[string]any, error) {
	var result map[string]any
	params := url.Values{"workspace": {workspace}, "pool": {pool}}
	if err := c.get(ctx, "/api/get_pool", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// UpdatePool updates or creates a pool
func (c *Client) UpdatePool(ctx context.Context, workspace, poolName string, pool any) error {
	body := map[string]any{
		"workspaceName": workspace,
		"poolName":      poolName,
		"pool":          pool,
	}
	return c.post(ctx, "/api/update_pool", body, nil)
}

// Tokens API

// ListTokens lists all API tokens
func (c *Client) ListTokens(ctx context.Context) ([]map[string]any, error) {
	var result struct {
		Tokens []map[string]any `json:"tokens"`
	}
	if err := c.get(ctx, "/api/list_tokens", nil, &result); err != nil {
		return nil, err
	}
	return result.Tokens, nil
}

// CreateToken creates a new API token
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
	if err := c.post(ctx, "/api/create_token", body, &result); err != nil {
		return "", err
	}
	return result.Token, nil
}

// RevokeToken revokes an API token
func (c *Client) RevokeToken(ctx context.Context, externalID string) error {
	body := map[string]any{
		"externalId": externalID,
	}
	return c.post(ctx, "/api/revoke_token", body, nil)
}

// RerunStepResult contains the IDs returned from re-running a step
type RerunStepResult struct {
	ExecutionID json.Number `json:"executionId"`
	Attempt     int         `json:"attempt"`
}

// RerunStep triggers a re-run of an existing step
func (c *Client) RerunStep(ctx context.Context, workspace, stepID string) (*RerunStepResult, error) {
	body := map[string]any{
		"workspaceName": workspace,
		"stepId":        stepID,
	}
	var result RerunStepResult
	if err := c.post(ctx, "/api/rerun_step", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CancelExecution cancels a running or pending execution
func (c *Client) CancelExecution(ctx context.Context, workspace, executionID string) error {
	body := map[string]any{
		"workspaceName": workspace,
		"executionId":   executionID,
	}
	return c.post(ctx, "/api/cancel_execution", body, nil)
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

func (c *Client) get(ctx context.Context, path string, params url.Values, result any) error {
	reqURL := c.baseURL + path
	if params != nil {
		reqURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-API-Version", APIVersion)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

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

func (c *Client) post(ctx context.Context, path string, body any, result any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Version", APIVersion)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

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
