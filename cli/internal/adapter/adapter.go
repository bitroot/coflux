package adapter

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
)

// Adapter is the interface for language adapters.
// Adapters communicate with language-specific executors via the standard
// JSON Lines protocol over stdio.
type Adapter interface {
	// Discover finds targets in the specified modules
	Discover(ctx context.Context, modules []string) (*DiscoveryManifest, error)

	// SpawnExecutor starts a new executor process
	SpawnExecutor(ctx context.Context) (*Executor, error)

	// Command returns the base command used by the adapter
	Command() []string
}

// Executor represents a running executor process
type Executor struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	stderr io.ReadCloser

	mu        sync.Mutex
	ready     bool
	closed    bool
	requestID int

	// Channels for communication
	messages chan []byte
	errors   chan error
}

// CommandAdapter implements Adapter using a configurable command.
// The command should support "discover" and "execute" subcommands.
type CommandAdapter struct {
	command []string // Base command (e.g., ["python", "-m", "coflux"])
}

// NewCommandAdapter creates a new adapter with the given base command.
// The command will be invoked with "discover <modules...>" or "execute" appended.
func NewCommandAdapter(command []string) *CommandAdapter {
	return &CommandAdapter{command: command}
}

// Command returns the base command
func (a *CommandAdapter) Command() []string {
	return a.command
}

// Discover runs discovery on the specified modules
func (a *CommandAdapter) Discover(ctx context.Context, modules []string) (*DiscoveryManifest, error) {
	// Build command: base... discover modules...
	args := append([]string{}, a.command[1:]...)
	args = append(args, "discover")
	args = append(args, modules...)
	cmd := exec.CommandContext(ctx, a.command[0], args...)

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("discovery failed: %s", string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("failed to run discovery: %w", err)
	}

	var manifest DiscoveryManifest
	if err := json.Unmarshal(output, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse discovery output: %w", err)
	}

	return &manifest, nil
}

// SpawnExecutor starts a new executor process
func (a *CommandAdapter) SpawnExecutor(ctx context.Context) (*Executor, error) {
	// Build command: base... execute
	args := append([]string{}, a.command[1:]...)
	args = append(args, "execute")
	cmd := exec.CommandContext(ctx, a.command[0], args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start executor: %w", err)
	}

	exec := &Executor{
		cmd:      cmd,
		stdin:    stdin,
		stdout:   bufio.NewReader(stdout),
		stderr:   stderr,
		messages: make(chan []byte, 100),
		errors:   make(chan error, 1),
	}

	// Start reading messages in background
	go exec.readLoop()

	return exec, nil
}

// readLoop continuously reads messages from stdout
func (e *Executor) readLoop() {
	for {
		line, err := e.stdout.ReadBytes('\n')
		if err != nil {
			e.mu.Lock()
			e.closed = true
			e.mu.Unlock()
			if err != io.EOF {
				e.errors <- err
			}
			close(e.messages)
			return
		}
		e.messages <- line
	}
}

// WaitReady waits for the executor to send the ready message
func (e *Executor) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case msg, ok := <-e.messages:
		if !ok {
			return fmt.Errorf("executor closed before ready")
		}
		method, _, _, err := ParseMessage(msg)
		if err != nil {
			return fmt.Errorf("failed to parse ready message: %w", err)
		}
		if method != "ready" {
			return fmt.Errorf("expected ready message, got %s", method)
		}
		e.mu.Lock()
		e.ready = true
		e.mu.Unlock()
		return nil
	case err := <-e.errors:
		return fmt.Errorf("executor error: %w", err)
	}
}

// Send sends a message to the executor
func (e *Executor) Send(msg any) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return fmt.Errorf("executor is closed")
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	data = append(data, '\n')
	if _, err := e.stdin.Write(data); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

// SendExecute sends an execute command to the executor
func (e *Executor) SendExecute(executionID, target string, arguments []Argument, workingDir string) error {
	req := ExecuteRequest{
		Method: "execute",
		Params: ExecuteRequestParams{
			ExecutionID: executionID,
			Target:      target,
			Arguments:   arguments,
			WorkingDir:  workingDir,
		},
	}
	return e.Send(req)
}

// SendResponse sends a response to an executor request
func (e *Executor) SendResponse(id int, result any, err *ErrorInfo) error {
	resp := Response{
		ID:     id,
		Result: result,
		Error:  err,
	}
	return e.Send(resp)
}

// Receive receives the next message from the executor
func (e *Executor) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-e.messages:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case err := <-e.errors:
		return nil, err
	}
}

// IsReady returns whether the executor is ready
func (e *Executor) IsReady() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ready
}

// IsClosed returns whether the executor is closed
func (e *Executor) IsClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

// Close terminates the executor process
func (e *Executor) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	_ = e.stdin.Close()

	// Kill the process
	if e.cmd.Process != nil {
		_ = e.cmd.Process.Kill()
	}

	return e.cmd.Wait()
}

// NextRequestID returns the next request ID for tracking requests
func (e *Executor) NextRequestID() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.requestID++
	return e.requestID
}
