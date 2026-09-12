package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bitroot/coflux/cli/internal/api"
	"github.com/bitroot/coflux/cli/internal/blob"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var catalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "Manage the catalog",
}

func init() {
	catalogCmd.AddCommand(catalogListCmd)
	catalogCmd.AddCommand(catalogInspectCmd)
	catalogCmd.AddCommand(catalogGetCmd)
	catalogCmd.AddCommand(catalogPublishCmd)
	catalogCmd.AddCommand(catalogDownloadCmd)
}

// catalog list

var catalogListCmd = &cobra.Command{
	Use:   "list [prefix]",
	Short: "List catalog paths and their latest versions",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runCatalogList,
}

func runCatalogList(cmd *cobra.Command, args []string) error {
	prefix := ""
	if len(args) == 1 {
		prefix = args[0]
	}

	client, wsID, err := catalogClient(cmd)
	if err != nil {
		return err
	}

	entries, err := client.GetCatalog(cmd.Context(), wsID, prefix)
	if err != nil {
		return err
	}

	if isOutput("json") {
		return outputJSON(entries)
	}

	if len(entries) == 0 {
		fmt.Println("No catalog entries.")
		return nil
	}

	var rows [][]string
	for _, entry := range entries {
		rows = append(rows, []string{
			getString(entry, "path"),
			fmt.Sprintf("%d", getInt64(entry, "number")),
			catalogValueSummary(entry),
			formatTimestamp(getInt64(entry, "createdAt") / 1000),
			getString(entry, "workspaceId"),
		})
	}
	printTable([]string{"Path", "Version", "Value", "Published", "Workspace"}, rows)
	return nil
}

// catalog inspect

var catalogInspectLimit int

var catalogInspectCmd = &cobra.Command{
	Use:   "inspect <path>",
	Short: "List the versions at a catalog path",
	Args:  cobra.ExactArgs(1),
	RunE:  runCatalogInspect,
}

func init() {
	catalogInspectCmd.Flags().IntVar(&catalogInspectLimit, "limit", 20, "Maximum number of versions to show")
}

func runCatalogInspect(cmd *cobra.Command, args []string) error {
	client, wsID, err := catalogClient(cmd)
	if err != nil {
		return err
	}

	versions, err := client.GetCatalogVersions(cmd.Context(), wsID, args[0], catalogInspectLimit)
	if err != nil {
		return err
	}

	if isOutput("json") {
		return outputJSON(versions)
	}

	if len(versions) == 0 {
		fmt.Println("No versions.")
		return nil
	}

	var rows [][]string
	for _, version := range versions {
		rows = append(rows, []string{
			fmt.Sprintf("%d", getInt64(version, "number")),
			catalogValueSummary(version),
			formatTimestamp(getInt64(version, "createdAt") / 1000),
			getString(version, "workspaceId"),
			getString(version, "publishedBy"),
		})
	}
	printTable([]string{"Version", "Value", "Published", "Workspace", "Published by"}, rows)
	return nil
}

// catalog get

var catalogGetCmd = &cobra.Command{
	Use:   "get <path>[@<version>]",
	Short: "Print the value at a catalog path",
	Long: `Print the value at a catalog path.

Shows the latest version's value, or a specific version's with <path>@<number>.
To fetch the files of any assets the value holds, use 'catalog download'.`,
	Args: cobra.ExactArgs(1),
	RunE: runCatalogGet,
}

func runCatalogGet(cmd *cobra.Command, args []string) error {
	client, wsID, err := catalogClient(cmd)
	if err != nil {
		return err
	}

	version, err := findCatalogVersion(cmd.Context(), client, wsID, args[0])
	if err != nil {
		return err
	}

	if isOutput("json") {
		return outputJSON(version)
	}

	value, _ := version["value"].(map[string]any)
	if value == nil {
		return fmt.Errorf("version has no value")
	}
	return printValue(value)
}

// catalog publish

var catalogPublishAsset string

var catalogPublishCmd = &cobra.Command{
	Use:   "publish <path> [<value>]",
	Short: "Publish a value at a catalog path",
	Long: `Publish a value at a catalog path.

The value is a JSON document, or an existing asset with --asset <asset-id>.
Publishing what is already the latest version writes nothing.

Examples:
  coflux catalog publish configs/training '{"threshold": 0.7}'
  coflux catalog publish models/churn --asset Abc123`,
	Args: cobra.RangeArgs(1, 2),
	RunE: runCatalogPublish,
}

func init() {
	catalogPublishCmd.Flags().StringVar(&catalogPublishAsset, "asset", "", "Publish an existing asset by id, instead of a JSON value")
}

func runCatalogPublish(cmd *cobra.Command, args []string) error {
	var value any
	switch {
	case len(args) == 2 && catalogPublishAsset != "":
		return fmt.Errorf("specify either a value or --asset, not both")
	case len(args) == 2:
		if err := json.Unmarshal([]byte(args[1]), &value); err != nil {
			return fmt.Errorf("invalid value: expected JSON: %w", err)
		}
		if value == nil {
			return fmt.Errorf("invalid value: null cannot be published from the CLI")
		}
	case catalogPublishAsset == "":
		return fmt.Errorf("specify a value (JSON) or --asset <asset-id>")
	}

	client, wsID, err := catalogClient(cmd)
	if err != nil {
		return err
	}

	result, err := client.PublishCatalog(cmd.Context(), wsID, args[0], value, catalogPublishAsset)
	if err != nil {
		return err
	}

	if isOutput("json") {
		return outputJSON(result)
	}

	if result.Created {
		fmt.Printf("Published %s@%d\n", getString(result.Version, "path"), getInt64(result.Version, "number"))
	} else {
		fmt.Printf("Already the head: %s@%d\n", getString(result.Version, "path"), getInt64(result.Version, "number"))
	}
	return nil
}

// catalog download

var (
	catalogDownloadTo    string
	catalogDownloadForce bool
	catalogDownloadMatch string
)

var catalogDownloadCmd = &cobra.Command{
	Use:   "download <path>[@<version>]",
	Short: "Download the assets held by the value at a catalog path",
	Long: `Download the assets held by the value at a catalog path.

A value that is a single asset restores flat into --to. A value holding
several assets restores each into a subdirectory named by the keys (or
indices) leading to it. A value holding no assets is an error; use
'catalog get' to see it.`,
	Args: cobra.ExactArgs(1),
	RunE: runCatalogDownload,
}

func init() {
	catalogDownloadCmd.Flags().StringVar(&catalogDownloadTo, "to", ".", "The local path to download the contents to")
	catalogDownloadCmd.Flags().BoolVar(&catalogDownloadForce, "force", false, "Overwrite any existing files if present")
	catalogDownloadCmd.Flags().StringVar(&catalogDownloadMatch, "match", "", "Glob-style pattern to filter files (within each asset)")
}

func runCatalogDownload(cmd *cobra.Command, args []string) error {
	client, wsID, err := catalogClient(cmd)
	if err != nil {
		return err
	}

	version, err := findCatalogVersion(cmd.Context(), client, wsID, args[0])
	if err != nil {
		return err
	}

	value, _ := version["value"].(map[string]any)
	if value == nil {
		return fmt.Errorf("version has no value")
	}
	data, references, err := loadValueData(value)
	if err != nil {
		return err
	}

	var assets []assetLocation
	collectAssets(data, references, nil, &assets)
	if len(assets) == 0 {
		return fmt.Errorf("nothing to download: %s@%d holds no assets (see 'coflux catalog get %s')",
			getString(version, "path"), getInt64(version, "number"), args[0])
	}

	// Resolve every asset before writing anything, so a missing one doesn't
	// leave a half-restored tree behind.
	type plannedAsset struct {
		location assetLocation
		entries  map[string]map[string]any
		dest     string
	}
	var planned []plannedAsset
	for _, location := range assets {
		asset, err := client.GetAssetByID(cmd.Context(), location.assetID)
		if err != nil {
			return fmt.Errorf("asset '%s' not found", location.assetID)
		}
		entries := filterAssetEntries(asset, catalogDownloadMatch)
		dest := catalogDownloadTo
		if len(assets) > 1 {
			dest = filepath.Join(append([]string{catalogDownloadTo}, location.trail...)...)
		}
		for key := range entries {
			destPath := filepath.Join(dest, key)
			if info, statErr := os.Stat(destPath); statErr == nil {
				if !catalogDownloadForce {
					return fmt.Errorf("file already exists at path: %s (use --force to overwrite)", destPath)
				}
				if !info.Mode().IsRegular() {
					return fmt.Errorf("cannot overwrite non-file: %s", destPath)
				}
			}
		}
		planned = append(planned, plannedAsset{location, entries, dest})
	}

	total := 0
	for _, p := range planned {
		total += len(p.entries)
	}
	if total == 0 {
		fmt.Println("Nothing to download")
		return nil
	}

	token, err := resolveToken()
	if err != nil {
		return err
	}
	stores, err := createBlobStoresFromViper(token)
	if err != nil {
		return fmt.Errorf("failed to create blob stores: %w", err)
	}
	if len(stores) == 0 {
		return fmt.Errorf("blob store not configured")
	}
	blobManager := blob.NewManager(stores, filepath.Join(os.TempDir(), "coflux-cache", "blobs"), viper.GetInt("blobs.threshold"))
	if err := blobManager.EnsureCacheDir(); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	fmt.Printf("Downloading %s@%d (%d files)...\n", getString(version, "path"), getInt64(version, "number"), total)
	for _, p := range planned {
		for key, entry := range p.entries {
			destPath := filepath.Join(p.dest, key)
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return fmt.Errorf("failed to create directory for %s: %w", key, err)
			}
			cachePath, err := blobManager.Download(getString(entry, "blobKey"))
			if err != nil {
				return fmt.Errorf("failed to download %s: %w", key, err)
			}
			if err := copyFile(cachePath, destPath); err != nil {
				return fmt.Errorf("failed to copy %s: %w", key, err)
			}
			shown, _ := filepath.Rel(catalogDownloadTo, destPath)
			fmt.Printf("  %s\n", shown)
		}
	}
	fmt.Println("Download complete.")
	return nil
}

// Helpers

// catalogClient resolves the API client and the workspace every catalog
// command works against.
func catalogClient(cmd *cobra.Command) (*api.Client, string, error) {
	client, err := newClient()
	if err != nil {
		return nil, "", err
	}
	wsName, err := requireWorkspace()
	if err != nil {
		return nil, "", err
	}
	wsID, err := resolveWorkspaceID(cmd.Context(), client, wsName)
	if err != nil {
		return nil, "", err
	}
	return client, wsID, nil
}

// parseCatalogRef splits "path" or "path@6" into the path and the version
// number (0 when unspecified).
func parseCatalogRef(ref string) (string, int64, error) {
	path, numberText, found := strings.Cut(ref, "@")
	if !found {
		return path, 0, nil
	}
	number, err := strconv.ParseInt(numberText, 10, 64)
	if err != nil || number <= 0 {
		return "", 0, fmt.Errorf("invalid version in %q: expected <path>@<number>", ref)
	}
	return path, number, nil
}

// findCatalogVersion resolves "path" to the head visible from the
// workspace, or "path@n" to that version.
func findCatalogVersion(ctx context.Context, client *api.Client, wsID, ref string) (map[string]any, error) {
	path, number, err := parseCatalogRef(ref)
	if err != nil {
		return nil, err
	}
	if number == 0 {
		entries, err := client.GetCatalog(ctx, wsID, path)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if getString(entry, "path") == path {
				return entry, nil
			}
		}
	} else {
		// Versions are listed newest first; walk until the one asked for.
		versions, err := client.GetCatalogVersions(ctx, wsID, path, 500)
		if err != nil {
			return nil, err
		}
		for _, candidate := range versions {
			if getInt64(candidate, "number") == number {
				return candidate, nil
			}
		}
	}
	return nil, fmt.Errorf("no such catalog version: %s", ref)
}

// catalogValueSummary renders a version's value on one line, for a table
// cell.
func catalogValueSummary(version map[string]any) string {
	value, _ := version["value"].(map[string]any)
	if value == nil {
		return ""
	}
	references, _ := value["references"].([]any)
	var text string
	switch value["type"] {
	case "raw":
		text = formatDataCompact(value["data"], references)
	case "blob":
		size, _ := value["size"].(float64)
		text = fmt.Sprintf("<blob (%s)>", humanSize(int64(size)))
	}
	const limit = 60
	if runes := []rune(text); len(runes) > limit {
		text = string(runes[:limit-1]) + "…"
	}
	return text
}

// loadValueData returns a value's encoded data and references, fetching
// the blob behind a blob-backed value.
func loadValueData(value map[string]any) (any, []any, error) {
	references, _ := value["references"].([]any)
	switch value["type"] {
	case "raw":
		return value["data"], references, nil
	case "blob":
		data, err := loadBlobData(getString(value, "key"))
		if err != nil {
			return nil, nil, err
		}
		return data, references, nil
	}
	return nil, nil, fmt.Errorf("unknown value type: %v", value["type"])
}

// assetLocation is an asset reference found inside a value, with the keys
// and indices leading to it.
type assetLocation struct {
	trail   []string
	assetID string
}

// collectAssets walks encoded data (the JSON value format: lists, typed
// dicts/sets/tuples, refs) and appends every asset reference it holds.
func collectAssets(data any, references []any, trail []string, out *[]assetLocation) {
	switch v := data.(type) {
	case []any:
		for i, item := range v {
			collectAssets(item, references, append(trail, strconv.Itoa(i)), out)
		}
	case map[string]any:
		switch v["type"] {
		case "dict":
			items, _ := v["items"].([]any)
			for i := 0; i+1 < len(items); i += 2 {
				collectAssets(items[i+1], references, append(trail, pathSegment(items[i])), out)
			}
		case "set", "tuple":
			items, _ := v["items"].([]any)
			for i, item := range items {
				collectAssets(item, references, append(trail, strconv.Itoa(i)), out)
			}
		case "ref":
			index, _ := v["index"].(float64)
			idx := int(index)
			if idx < 0 || idx >= len(references) {
				return
			}
			ref, _ := references[idx].(map[string]any)
			if ref != nil && ref["type"] == "asset" {
				*out = append(*out, assetLocation{
					trail:   append([]string(nil), trail...),
					assetID: getString(ref, "assetId"),
				})
			}
		}
	}
}

// pathSegment turns a dict key into a directory name: its string form,
// with anything that could escape the directory replaced.
func pathSegment(key any) string {
	var text string
	switch k := key.(type) {
	case string:
		text = k
	case float64:
		if k == float64(int64(k)) {
			text = strconv.FormatInt(int64(k), 10)
		} else {
			text = strconv.FormatFloat(k, 'g', -1, 64)
		}
	default:
		text = fmt.Sprint(k)
	}
	text = strings.NewReplacer("/", "_", "\\", "_").Replace(text)
	if text == "" || text == "." || text == ".." {
		text = "_"
	}
	return text
}

// filterAssetEntries returns an asset's entries as a map of path to entry,
// keeping only those matching the glob when one is given.
func filterAssetEntries(asset map[string]any, match string) map[string]map[string]any {
	entries, _ := asset["entries"].(map[string]any)
	filtered := make(map[string]map[string]any)
	for key, value := range entries {
		if entry, ok := value.(map[string]any); ok {
			if match == "" || matchGlob(match, key) {
				filtered[key] = entry
			}
		}
	}
	return filtered
}
