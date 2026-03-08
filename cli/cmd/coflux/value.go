package main

import (
	"fmt"
	"strings"
)

func formatDuration(seconds float64) string {
	days := int(seconds) / 86400
	hours := (int(seconds) % 86400) / 3600
	minutes := (int(seconds) % 3600) / 60
	secs := seconds - float64(int(seconds)-int(seconds)%60)
	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%dm", minutes))
	}
	if secs > 0 || len(parts) == 0 {
		if secs == float64(int(secs)) {
			parts = append(parts, fmt.Sprintf("%ds", int(secs)))
		} else {
			parts = append(parts, fmt.Sprintf("%gs", secs))
		}
	}
	return strings.Join(parts, "")
}

type refFormatter func(ref any) string

// formatDataIndent recursively formats a Data value as human-readable text with indentation.
func formatDataIndent(data any, references []any, fmtRef refFormatter, depth int) string {
	switch v := data.(type) {
	case string:
		return fmt.Sprintf(`"%s"`, v)
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "True"
		}
		return "False"
	case nil:
		return "None"
	case []any:
		items := make([]string, len(v))
		for i, item := range v {
			items[i] = formatDataIndent(item, references, fmtRef, depth)
		}
		return "[" + strings.Join(items, ", ") + "]"
	case map[string]any:
		typ, _ := v["type"].(string)
		switch typ {
		case "dict":
			items, _ := v["items"].([]any)
			if len(items) == 0 {
				return "{}"
			}
			indent := strings.Repeat("  ", depth+1)
			outdent := strings.Repeat("  ", depth)
			var lines []string
			for i := 0; i+1 < len(items); i += 2 {
				key := formatDataIndent(items[i], references, fmtRef, depth+1)
				val := formatDataIndent(items[i+1], references, fmtRef, depth+1)
				lines = append(lines, indent+key+" ↦ "+val)
			}
			return "{\n" + strings.Join(lines, "\n") + "\n" + outdent + "}"
		case "set":
			items, _ := v["items"].([]any)
			if len(items) == 0 {
				return "∅"
			}
			parts := make([]string, len(items))
			for i, item := range items {
				parts[i] = formatDataIndent(item, references, fmtRef, depth)
			}
			return "{" + strings.Join(parts, ", ") + "}"
		case "tuple":
			items, _ := v["items"].([]any)
			parts := make([]string, len(items))
			for i, item := range items {
				parts[i] = formatDataIndent(item, references, fmtRef, depth)
			}
			return "(" + strings.Join(parts, ", ") + ")"
		case "datetime", "date", "time":
			val, _ := v["value"].(string)
			return val
		case "duration":
			seconds, _ := v["value"].(float64)
			return formatDuration(seconds)
		case "decimal":
			val, _ := v["value"].(string)
			return val
		case "uuid":
			val, _ := v["value"].(string)
			return val
		case "ref":
			index, _ := v["index"].(float64)
			idx := int(index)
			if idx >= 0 && idx < len(references) {
				return fmtRef(references[idx])
			}
			return "<ref ?>"
		}
	}
	return fmt.Sprintf("%v", data)
}

// formatData recursively formats a Data value as human-readable text.
func formatData(data any, references []any) string {
	return formatDataIndent(data, references, formatReference, 0)
}

func formatReference(ref any) string {
	r, ok := ref.(map[string]any)
	if !ok {
		return "<ref ?>"
	}

	typ, _ := r["type"].(string)
	switch typ {
	case "execution":
		module, _ := r["module"].(string)
		target, _ := r["target"].(string)
		if module != "" || target != "" {
			return fmt.Sprintf("<execution %s/%s>", module, target)
		}
		return "<execution>"
	case "asset":
		asset, _ := r["asset"].(map[string]any)
		if asset != nil {
			name, _ := asset["name"].(string)
			totalCount, _ := asset["totalCount"].(float64)
			totalSize, _ := asset["totalSize"].(float64)
			if name != "" {
				return fmt.Sprintf("<asset %q (%d files, %s)>", name, int(totalCount), humanSize(int64(totalSize)))
			}
			return fmt.Sprintf("<asset (%d files, %s)>", int(totalCount), humanSize(int64(totalSize)))
		}
		return "<asset>"
	case "fragment":
		format, _ := r["format"].(string)
		size, _ := r["size"].(float64)
		return fmt.Sprintf("<fragment %s (%s)>", format, humanSize(int64(size)))
	}
	return "<ref ?>"
}

// formatLogReference formats a reference in the flat log format.
func formatLogReference(ref any) string {
	r, ok := ref.(map[string]any)
	if !ok {
		return "<ref ?>"
	}

	typ, _ := r["type"].(string)
	switch typ {
	case "execution":
		module, _ := r["module"].(string)
		target, _ := r["target"].(string)
		if module != "" || target != "" {
			return fmt.Sprintf("<execution %s/%s>", module, target)
		}
		return "<execution>"
	case "asset":
		name, _ := r["name"].(string)
		totalCount, _ := r["totalCount"].(float64)
		totalSize, _ := r["totalSize"].(float64)
		if name != "" {
			return fmt.Sprintf("<asset %q (%d files, %s)>", name, int(totalCount), humanSize(int64(totalSize)))
		}
		return fmt.Sprintf("<asset (%d files, %s)>", int(totalCount), humanSize(int64(totalSize)))
	case "fragment":
		format, _ := r["format"].(string)
		size, _ := r["size"].(float64)
		return fmt.Sprintf("<fragment %s (%s)>", format, humanSize(int64(size)))
	}
	return "<ref ?>"
}

// formatLogData formats a log value's data using the flat log reference format.
func formatLogData(data any, references []any) string {
	return formatDataIndent(data, references, formatLogReference, 0)
}
