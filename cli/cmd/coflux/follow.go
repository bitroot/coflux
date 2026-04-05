package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	topicalclient "github.com/bitroot/coflux/cli/internal/topical"
	"golang.org/x/term"
)

// stepState holds the display state for a single step (latest execution).
type stepState struct {
	Module   string
	Target   string
	ParentID string // execution ID of parent (empty for root)
	StepNum  string // step number within the run (e.g., "3")
	Attempt  string // latest execution attempt (e.g., "1")
	Status   string
	Detail   string
}

// groupMember records a child step's membership in a group, keyed by the
// parent execution that defined the group.
type groupMember struct {
	parentExecID string // execution ID of the parent
	groupID      string // group ID within that execution
	groupName    string // human-readable group name (may be empty)
}

// groupSummary is a rendered summary line for a collapsed group.
type groupSummary struct {
	text string // pre-formatted summary text
}

// watchRun subscribes to the run topic and renders a live-updating tree of
// step statuses (TTY only). Waits for all steps to complete. Returns exit code.
func watchRun(ctx context.Context, host string, secure bool, token string, project string, runID string, workspaceID string) (int, error) {
	client, err := topicalclient.Connect(ctx, host, secure, token, project)
	if err != nil {
		return 1, fmt.Errorf("failed to connect to topics: %w", err)
	}
	defer client.Close()

	sub := client.Subscribe("workspaces/"+workspaceID+"/runs/"+runID, nil)
	defer sub.Unsubscribe()

	// Ctrl+C detaches (stops watching) but leaves the workflow running
	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lastRendered := map[string]stepState{}
	linesDrawn := 0

	// Get terminal height for capping live output
	maxLines := 0
	if _, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil && h > 0 {
		maxLines = h - 1 // leave room for the cursor line
	}

	var lastData map[string]any

	for {
		select {
		case value, ok := <-sub.Values():
			if !ok {
				return 1, fmt.Errorf("subscription closed unexpectedly")
			}

			data, ok := value.(map[string]any)
			if !ok {
				continue
			}
			lastData = data

			linesDrawn = renderStepTree(data, lastRendered, linesDrawn, maxLines)

			exitCode, done := checkAllComplete(data)
			if done {
				// Final uncapped render to show the full tree
				if maxLines > 0 {
					// Force redraw by clearing tracked state
					for k := range lastRendered {
						delete(lastRendered, k)
					}
					renderStepTree(lastData, lastRendered, linesDrawn, 0)
				}
				return exitCode, nil
			}

		case err, ok := <-sub.Err():
			if !ok {
				return 1, fmt.Errorf("subscription closed unexpectedly")
			}
			return 1, fmt.Errorf("subscription error: %w", err)

		case <-sigCtx.Done():
			fmt.Fprintf(os.Stderr, "\nDetached. Workflow still running.\n")
			return 0, nil
		}
	}
}

// waitForRootResult subscribes to the run topic and waits for the root step's
// latest execution to have a result. Returns the full run topic data, exit
// code, and error.
func waitForRootResult(ctx context.Context, host string, secure bool, token string, project string, runID string, workspaceID string) (map[string]any, int, error) {
	client, err := topicalclient.Connect(ctx, host, secure, token, project)
	if err != nil {
		return nil, 1, fmt.Errorf("failed to connect to topics: %w", err)
	}
	defer client.Close()

	sub := client.Subscribe("workspaces/"+workspaceID+"/runs/"+runID, nil)
	defer sub.Unsubscribe()

	for {
		select {
		case value, ok := <-sub.Values():
			if !ok {
				return nil, 1, fmt.Errorf("subscription closed unexpectedly")
			}

			data, ok := value.(map[string]any)
			if !ok {
				continue
			}

			exitCode, done := checkRootComplete(data)
			if done {
				return data, exitCode, nil
			}

		case err, ok := <-sub.Err():
			if !ok {
				return nil, 1, fmt.Errorf("subscription closed unexpectedly")
			}
			return nil, 1, fmt.Errorf("subscription error: %w", err)

		case <-ctx.Done():
			return nil, 1, ctx.Err()
		}
	}
}

// renderStepTree redraws the step tree in place using ANSI escape codes.
// maxLines caps the output height (0 = unlimited). Returns the number of lines
// currently drawn.
func renderStepTree(data map[string]any, lastRendered map[string]stepState, linesDrawn int, maxLines int) int {
	steps, _ := data["steps"].(map[string]any)
	if steps == nil {
		return linesDrawn
	}

	// Build current state for every step, and an executionID -> stepID index
	currentStates := make(map[string]stepState, len(steps))
	execToStep := map[string]string{} // executionID -> stepID
	// groupMembers maps child stepID -> groupMember info
	groupMembers := map[string]groupMember{}
	for stepID, raw := range steps {
		stepData, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		module, _ := stepData["module"].(string)
		target, _ := stepData["target"].(string)
		parentID, _ := stepData["parentId"].(string)
		executions, _ := stepData["executions"].(map[string]any)

		// Index all execution IDs for this step, and parse group info
		for _, execRaw := range executions {
			e, ok := execRaw.(map[string]any)
			if !ok {
				continue
			}
			eid, _ := e["executionId"].(string)
			if eid != "" {
				execToStep[eid] = stepID
			}

			// Parse groups map (groupID -> name) and children array
			groups, _ := e["groups"].(map[string]any)
			childrenArr, _ := e["children"].([]any)
			for _, childRaw := range childrenArr {
				child, ok := childRaw.(map[string]any)
				if !ok {
					continue
				}
				childStepID, _ := child["stepId"].(string)
				if childStepID == "" {
					continue
				}
				// groupId may be a float64 from JSON or nil
				var gidStr string
				switch gid := child["groupId"].(type) {
				case float64:
					gidStr = fmt.Sprintf("%d", int(gid))
				case string:
					gidStr = gid
				default:
					continue // no group
				}
				groupName := ""
				if groups != nil {
					if name, ok := groups[gidStr].(string); ok {
						groupName = name
					}
				}
				groupMembers[childStepID] = groupMember{
					parentExecID: eid,
					groupID:      gidStr,
					groupName:    groupName,
				}
			}
		}

		// Extract step number from stepID (format: "runID:stepNumber")
		stepNum := stepID
		if idx := strings.Index(stepID, ":"); idx >= 0 {
			stepNum = stepID[idx+1:]
		}

		status, detail, latestAttempt := latestExecutionStatus(executions)
		currentStates[stepID] = stepState{
			Module:   module,
			Target:   target,
			ParentID: parentID,
			StepNum:  stepNum,
			Attempt:  latestAttempt,
			Status:   status,
			Detail:   detail,
		}
	}

	// Skip redraw if nothing changed
	if statesEqual(currentStates, lastRendered) {
		return linesDrawn
	}

	// Build parent-step -> children map using executionID -> stepID resolution
	children := map[string][]string{} // parentStepID -> []childStepID
	var roots []string
	for stepID, st := range currentStates {
		if st.ParentID == "" {
			roots = append(roots, stepID)
		} else if parentStep, ok := execToStep[st.ParentID]; ok {
			children[parentStep] = append(children[parentStep], stepID)
		} else {
			// Parent execution not found (yet); treat as root
			roots = append(roots, stepID)
		}
	}

	// Sort children and roots by step ID for deterministic order
	sort.Strings(roots)
	for k := range children {
		sort.Strings(children[k])
	}

	// Walk tree depth-first to build output lines, collapsing groups
	type line struct {
		prefix  string
		stepID  string
		summary *groupSummary // non-nil for group summary lines
	}
	var lines []line
	var walkChildren func(stepIDs []string, prefix string)
	walkChildren = func(stepIDs []string, prefix string) {
		// Partition children into groups and ungrouped, preserving order.
		// groupKey = parentExecID + ":" + groupID
		type groupEntry struct {
			key     string
			name    string
			stepIDs []string
		}
		groupMap := map[string]*groupEntry{}
		var orderedItems []any // either string (stepID) or *groupEntry (first occurrence)
		for _, id := range stepIDs {
			gm, inGroup := groupMembers[id]
			if !inGroup {
				orderedItems = append(orderedItems, id)
				continue
			}
			key := gm.parentExecID + ":" + gm.groupID
			if ge, ok := groupMap[key]; ok {
				ge.stepIDs = append(ge.stepIDs, id)
			} else {
				ge = &groupEntry{key: key, name: gm.groupName, stepIDs: []string{id}}
				groupMap[key] = ge
				orderedItems = append(orderedItems, ge)
			}
		}

		// Flatten to a list of items to render
		type renderItem struct {
			stepID string
			group  *groupEntry // non-nil means this is a group (render first child + summary)
		}
		var items []renderItem
		for _, item := range orderedItems {
			switch v := item.(type) {
			case string:
				items = append(items, renderItem{stepID: v})
			case *groupEntry:
				if len(v.stepIDs) == 1 {
					// Single-member group: render normally
					items = append(items, renderItem{stepID: v.stepIDs[0]})
				} else {
					items = append(items, renderItem{group: v})
				}
			}
		}

		for i, item := range items {
			isLast := i == len(items)-1
			var connector, childPrefix string
			if isLast {
				connector = prefix + "└─ "
				childPrefix = prefix + "   "
			} else {
				connector = prefix + "├─ "
				childPrefix = prefix + "│  "
			}

			if item.group != nil {
				ge := item.group
				firstID := ge.stepIDs[0]
				// Render the group header as a tree layer
				summaryText := buildGroupSummary(ge.name, ge.stepIDs, currentStates, children)
				lines = append(lines, line{
					prefix:  connector,
					summary: &groupSummary{text: summaryText},
				})
				// Render the first child nested under the group
				lines = append(lines, line{prefix: childPrefix + "└─ ", stepID: firstID})
				if kids, ok := children[firstID]; ok {
					walkChildren(kids, childPrefix+"   ")
				}
			} else {
				lines = append(lines, line{prefix: connector, stepID: item.stepID})
				if kids, ok := children[item.stepID]; ok {
					walkChildren(kids, childPrefix)
				}
			}
		}
	}
	for _, id := range roots {
		lines = append(lines, line{prefix: "", stepID: id})
		if kids, ok := children[id]; ok {
			walkChildren(kids, "")
		}
	}

	// Truncate if exceeding maxLines (reserve 1 line for the "more" footer)
	totalSteps := len(lines)
	truncated := 0
	if maxLines > 0 && len(lines) > maxLines {
		truncated = len(lines) - (maxLines - 1)
		lines = lines[:maxLines-1]
	}

	// Move cursor up to overwrite previous output
	if linesDrawn > 0 {
		fmt.Printf("\033[%dA", linesDrawn)
	}

	// Render
	for _, l := range lines {
		if l.summary != nil {
			fmt.Printf("\r%s%s\033[K\n", l.prefix, l.summary.text)
			continue
		}
		st := currentStates[l.stepID]
		label := ""
		if st.StepNum != "" && st.Attempt != "" {
			label = colorDim + st.StepNum + ":" + st.Attempt + colorReset + " "
		}
		label += st.Module + "/" + st.Target
		// Show run ID on root steps
		if st.ParentID == "" {
			if idx := strings.Index(l.stepID, ":"); idx > 0 {
				label += " [" + l.stepID[:idx] + "]"
			}
		}
		indicator := statusIndicator(st.Status)
		annotation := statusAnnotation(st.Status, st.Detail)
		if annotation != "" {
			fmt.Printf("\r%s%s %s%s\033[K\n", l.prefix, indicator, label, annotation)
		} else {
			fmt.Printf("\r%s%s %s\033[K\n", l.prefix, indicator, label)
		}
	}
	if truncated > 0 {
		fmt.Printf("\r%s… %d more steps (%d total)%s\033[K\n", colorDim, truncated, totalSteps, colorReset)
	}

	// Clear any leftover lines from previous render
	outputLines := len(lines)
	if truncated > 0 {
		outputLines++
	}
	for i := outputLines; i < linesDrawn; i++ {
		fmt.Print("\r\033[K\n")
	}

	drawn := max(outputLines, linesDrawn)

	// Update tracking
	for k := range lastRendered {
		delete(lastRendered, k)
	}
	for k, v := range currentStates {
		lastRendered[k] = v
	}

	return drawn
}

// buildGroupSummary builds a summary string like:
// "group_name: 1 of 22 (5 pending, 2 running, 1 error)"
// The group name is shown in normal text, the counter is dim, and status
// counts are colored to match their status indicators. Each child's status
// is its "branch status" — if any descendant is still running/pending, the
// branch counts as running, regardless of the child's own terminal state.
func buildGroupSummary(groupName string, stepIDs []string, states map[string]stepState, childrenMap map[string][]string) string {
	total := len(stepIDs)

	// Count branch statuses
	counts := map[string]int{}
	for _, id := range stepIDs {
		counts[branchStatus(id, states, childrenMap)]++
	}

	// Build colored status parts (non-completed/non-cached only)
	statusOrder := []string{"pending", "running", "error", "cancelled", "abandoned", "suspended", "deferred"}
	var parts []string
	for _, s := range statusOrder {
		if c, ok := counts[s]; ok && c > 0 {
			color := statusColor(s)
			parts = append(parts, fmt.Sprintf("%s%d %s%s", color, c, s, colorReset))
		}
	}

	var summary string
	if groupName != "" {
		summary = groupName + " "
	}
	summary += colorDim + fmt.Sprintf("1 of %d", total) + colorReset
	if len(parts) > 0 {
		summary += " (" + strings.Join(parts, ", ") + ")"
	}

	return summary
}

// branchStatus returns the effective status for a step considering all its
// descendants. Active states (running, pending) take precedence over terminal
// states so that a branch with a running grandchild under an errored child
// shows as "running" until all descendants settle.
func branchStatus(stepID string, states map[string]stepState, childrenMap map[string][]string) string {
	st, ok := states[stepID]
	if !ok {
		return "pending"
	}

	// Collect this step's status plus all descendant statuses
	statuses := collectBranchStatuses(stepID, states, childrenMap)

	// Priority: active states first, then terminal failures
	for _, s := range []string{"running", "pending", "error", "cancelled", "abandoned", "suspended", "deferred"} {
		for _, status := range statuses {
			if status == s {
				return s
			}
		}
	}

	// All completed/cached
	return st.Status
}

// collectBranchStatuses recursively collects the status of a step and all its
// descendants.
func collectBranchStatuses(stepID string, states map[string]stepState, childrenMap map[string][]string) []string {
	st, ok := states[stepID]
	if !ok {
		return []string{"pending"}
	}
	result := []string{st.Status}
	for _, childID := range childrenMap[stepID] {
		result = append(result, collectBranchStatuses(childID, states, childrenMap)...)
	}
	return result
}

// statusColor returns the ANSI color code for a given status string.
func statusColor(status string) string {
	switch status {
	case "completed":
		return colorGreen
	case "cached":
		return colorDimGreen
	case "running":
		return colorBlue
	case "error", "abandoned":
		return colorRed
	case "cancelled":
		return colorYellow
	default:
		return colorDim
	}
}

// statesEqual returns true if two state maps are identical.
func statesEqual(a, b map[string]stepState) bool {
	if len(a) != len(b) {
		return false
	}
	for id, as := range a {
		if bs, ok := b[id]; !ok || as != bs {
			return false
		}
	}
	return true
}

// latestExecutionStatus returns the status, detail string, and attempt key
// for the most recent execution attempt of a step.
func latestExecutionStatus(executions map[string]any) (string, string, string) {
	if len(executions) == 0 {
		return "pending", "", ""
	}

	var latestAttempt string
	for attempt := range executions {
		if latestAttempt == "" || attempt > latestAttempt {
			latestAttempt = attempt
		}
	}

	exec, ok := executions[latestAttempt].(map[string]any)
	if !ok {
		return "pending", "", latestAttempt
	}

	status, detail, _ := computeExecutionStatus(exec)
	return status, detail, latestAttempt
}

// computeExecutionStatus determines the status string, detail, and timestamp
// for a single execution from its topic data. "Spawned" results are unwrapped
// to show the status of the inner result (matching Studio behaviour).
func computeExecutionStatus(exec map[string]any) (status, detail string, ts int64) {
	result, _ := exec["result"].(map[string]any)

	// Unwrap spawned: look through to the inner result. If the spawn is
	// async and hasn't resolved yet, inner result will be absent — treat
	// as running (result = nil).
	if result != nil {
		if rt, _ := result["type"].(string); rt == "spawned" {
			inner, ok := result["result"].(map[string]any)
			if ok {
				result = inner
			} else {
				result = nil
			}
		}
	}

	if result == nil {
		if exec["assignedAt"] != nil {
			if assignedAt, ok := exec["assignedAt"].(float64); ok {
				ts = int64(assignedAt)
			}
			return "running", "", ts
		}
		if createdAt, ok := exec["createdAt"].(float64); ok {
			ts = int64(createdAt)
		}
		return "pending", "", ts
	}

	if completedAt, ok := result["completedAt"].(float64); ok {
		ts = int64(completedAt)
	}

	resultType, _ := result["type"].(string)
	switch resultType {
	case "value":
		return "completed", "", ts
	case "error":
		errData, _ := result["error"].(map[string]any)
		if errData != nil {
			errType, _ := errData["type"].(string)
			errMsg, _ := errData["message"].(string)
			if errType != "" {
				detail = errType + ": " + errMsg
			} else {
				detail = errMsg
			}
		}
		return "error", detail, ts
	case "cancelled":
		return "cancelled", "", ts
	case "abandoned":
		return "abandoned", "", ts
	case "suspended":
		return "suspended", "", ts
	case "cached":
		return "cached", "", ts
	case "deferred":
		return "deferred", "", ts
	default:
		return resultType, "", ts
	}
}

// checkAllComplete checks if every execution across all steps has a terminal
// result. Returns done=true when all are finished, with exit code 0 if all
// succeeded or 1 if any failed.
func checkAllComplete(data map[string]any) (exitCode int, done bool) {
	steps, _ := data["steps"].(map[string]any)
	if len(steps) == 0 {
		return 0, false
	}

	hasFailure := false
	for _, stepData := range steps {
		s, ok := stepData.(map[string]any)
		if !ok {
			continue
		}

		executions, _ := s["executions"].(map[string]any)
		if len(executions) == 0 {
			return 0, false
		}

		// Check latest attempt for this step
		var latestAttempt string
		for attempt := range executions {
			if latestAttempt == "" || attempt > latestAttempt {
				latestAttempt = attempt
			}
		}

		exec, ok := executions[latestAttempt].(map[string]any)
		if !ok {
			return 0, false
		}

		result, _ := exec["result"].(map[string]any)
		if result == nil {
			return 0, false
		}

		resultType, _ := result["type"].(string)
		switch resultType {
		case "value", "cached", "spawned":
			// ok
		default:
			hasFailure = true
		}
	}

	if hasFailure {
		return 1, true
	}
	return 0, true
}

// checkRootComplete checks if the root step (no parentId) has a terminal result.
func checkRootComplete(data map[string]any) (exitCode int, done bool) {
	steps, _ := data["steps"].(map[string]any)
	if len(steps) == 0 {
		return 0, false
	}

	for _, stepData := range steps {
		s, ok := stepData.(map[string]any)
		if !ok || s["parentId"] != nil {
			continue
		}

		executions, _ := s["executions"].(map[string]any)
		if len(executions) == 0 {
			return 0, false
		}

		var latestAttempt string
		for attempt := range executions {
			if latestAttempt == "" || attempt > latestAttempt {
				latestAttempt = attempt
			}
		}
		if latestAttempt == "" {
			return 0, false
		}

		exec, ok := executions[latestAttempt].(map[string]any)
		if !ok {
			return 0, false
		}

		result, _ := exec["result"].(map[string]any)
		if result == nil {
			return 0, false
		}

		resultType, _ := result["type"].(string)
		switch resultType {
		case "value", "cached", "spawned":
			return 0, true
		default:
			return 1, true
		}
	}

	return 0, false
}

// ANSI color codes
const (
	colorReset     = "\033[0m"
	colorRed       = "\033[31m"
	colorGreen     = "\033[32m"
	colorYellow    = "\033[33m"
	colorBlue      = "\033[34m"
	colorMagenta   = "\033[35m"
	colorCyan      = "\033[36m"
	colorBold      = "\033[1m"
	colorDim       = "\033[90m"
	colorDimGreen  = "\033[2;32m"
	colorBrightRed = "\033[91m"
)

// statusIndicator returns a colored ● for the given status.
func statusIndicator(status string) string {
	switch status {
	case "completed":
		return colorGreen + "●" + colorReset
	case "cached":
		return colorDimGreen + "●" + colorReset
	case "running":
		return colorBlue + "●" + colorReset
	case "error", "abandoned":
		return colorRed + "●" + colorReset
	case "cancelled":
		return colorYellow + "●" + colorReset
	default:
		return colorDim + "●" + colorReset
	}
}

// statusAnnotation returns the text shown to the right of the target name.
// Completed steps show nothing; other terminal states and running get annotations.
func statusAnnotation(status, detail string) string {
	switch status {
	case "completed":
		return ""
	case "cached":
		return "  " + colorDim + "(cached)" + colorReset
	case "running":
		return "  " + colorDim + "Running..." + colorReset
	case "cancelled":
		return "  " + colorDim + "(cancelled)" + colorReset
	case "abandoned":
		return "  " + colorDim + "(abandoned)" + colorReset
	case "error":
		if detail != "" {
			return "  " + colorRed + detail + colorReset
		}
		return "  " + colorRed + "error" + colorReset
	case "spawned":
		if detail != "" {
			return " → " + detail + "  " + colorDim + "(spawned)" + colorReset
		}
		return "  " + colorDim + "(spawned)" + colorReset
	case "pending":
		return ""
	default:
		return "  " + colorDim + status + colorReset
	}
}
