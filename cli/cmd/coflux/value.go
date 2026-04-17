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

type valueFormatter struct {
	fmtRef  refFormatter
	colored bool
}

func (f *valueFormatter) color(color, s string) string {
	if !f.colored {
		return s
	}
	return color + s + colorReset
}

func (f *valueFormatter) format(data any, references []any, depth int) string {
	switch v := data.(type) {
	case string:
		return f.color(colorGreen, fmt.Sprintf(`"%s"`, v))
	case float64:
		var s string
		if v == float64(int64(v)) {
			s = fmt.Sprintf("%d", int64(v))
		} else {
			s = fmt.Sprintf("%g", v)
		}
		return f.color(colorMagenta, s)
	case bool:
		if v {
			return f.color(colorRed, "True")
		}
		return f.color(colorRed, "False")
	case nil:
		return f.color(colorRed, "None")
	case []any:
		items := make([]string, len(v))
		for i, item := range v {
			items[i] = f.format(item, references, depth)
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
			arrow := f.color(colorDim, " ↦ ")
			var lines []string
			for i := 0; i+1 < len(items); i += 2 {
				key := f.format(items[i], references, depth+1)
				val := f.format(items[i+1], references, depth+1)
				lines = append(lines, indent+key+arrow+val)
			}
			return "{\n" + strings.Join(lines, "\n") + "\n" + outdent + "}"
		case "set":
			items, _ := v["items"].([]any)
			if len(items) == 0 {
				return "∅"
			}
			parts := make([]string, len(items))
			for i, item := range items {
				parts[i] = f.format(item, references, depth)
			}
			return "{" + strings.Join(parts, ", ") + "}"
		case "tuple":
			items, _ := v["items"].([]any)
			parts := make([]string, len(items))
			for i, item := range items {
				parts[i] = f.format(item, references, depth)
			}
			return "(" + strings.Join(parts, ", ") + ")"
		case "datetime", "date", "time":
			val, _ := v["value"].(string)
			return f.color(colorYellow, val)
		case "duration":
			seconds, _ := v["value"].(float64)
			return f.color(colorYellow, formatDuration(seconds))
		case "decimal":
			val, _ := v["value"].(string)
			return f.color(colorCyan, val)
		case "uuid":
			val, _ := v["value"].(string)
			return val
		case "ref":
			index, _ := v["index"].(float64)
			idx := int(index)
			if idx >= 0 && idx < len(references) {
				return f.color(colorDim, f.fmtRef(references[idx]))
			}
			return f.color(colorDim, "<ref ?>")
		}
	}
	return fmt.Sprintf("%v", data)
}

// formatData formats a Data value as human-readable text with colors.
func formatData(data any, references []any) string {
	f := &valueFormatter{fmtRef: formatReference, colored: true}
	return f.format(data, references, 0)
}

// formatDataPlain formats a Data value as plain text (no colors).
func formatDataPlain(data any, references []any) string {
	f := &valueFormatter{fmtRef: formatReference, colored: false}
	return f.format(data, references, 0)
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
	case "input":
		inputId, _ := r["inputId"].(string)
		if inputId != "" {
			return fmt.Sprintf("<input %s>", inputId)
		}
		return "<input>"
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
	case "input":
		inputId, _ := r["inputId"].(string)
		if inputId != "" {
			return fmt.Sprintf("<input %s>", inputId)
		}
		return "<input>"
	}
	return "<ref ?>"
}

// formatLogData formats a log value's data using the flat log reference format with colors.
func formatLogData(data any, references []any) string {
	f := &valueFormatter{fmtRef: formatLogReference, colored: true}
	return f.format(data, references, 0)
}

// formatLogDataPlain formats a log value's data as plain text.
func formatLogDataPlain(data any, references []any) string {
	f := &valueFormatter{fmtRef: formatLogReference, colored: false}
	return f.format(data, references, 0)
}
