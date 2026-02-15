package main

import (
	"fmt"
	"strings"
)

type refFormatter func(ref any) string

// formatDataWith recursively formats a Data value as human-readable text,
// using the provided reference formatter to resolve {"type": "ref", "index": N} entries.
func formatDataWith(data any, references []any, fmtRef refFormatter) string {
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
			items[i] = formatDataWith(item, references, fmtRef)
		}
		return "[" + strings.Join(items, ", ") + "]"
	case map[string]any:
		typ, _ := v["type"].(string)
		switch typ {
		case "dict":
			items, _ := v["items"].([]any)
			pairs := make([]string, 0, len(items)/2)
			for i := 0; i+1 < len(items); i += 2 {
				key := formatDataWith(items[i], references, fmtRef)
				val := formatDataWith(items[i+1], references, fmtRef)
				pairs = append(pairs, key+": "+val)
			}
			return "{" + strings.Join(pairs, ", ") + "}"
		case "set":
			items, _ := v["items"].([]any)
			parts := make([]string, len(items))
			for i, item := range items {
				parts[i] = formatDataWith(item, references, fmtRef)
			}
			return "{" + strings.Join(parts, ", ") + "}"
		case "tuple":
			items, _ := v["items"].([]any)
			parts := make([]string, len(items))
			for i, item := range items {
				parts[i] = formatDataWith(item, references, fmtRef)
			}
			return "(" + strings.Join(parts, ", ") + ")"
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
// The references slice corresponds to the value's references array,
// used to resolve {"type": "ref", "index": N} entries.
func formatData(data any, references []any) string {
	return formatDataWith(data, references, formatReference)
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
			return fmt.Sprintf("<step %s.%s>", module, target)
		}
		return "<step>"
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
// Log references have fields at the top level (e.g. r["module"], r["target"])
// rather than nested under r["execution"] or r["asset"].
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
			return fmt.Sprintf("<step %s.%s>", module, target)
		}
		return "<step>"
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
	return formatDataWith(data, references, formatLogReference)
}
