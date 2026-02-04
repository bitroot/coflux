package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bitroot/coflux/cli/internal/blob"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var assetsCmd = &cobra.Command{
	Use:   "assets",
	Short: "Manage assets",
}

func init() {
	assetsCmd.AddCommand(assetsInspectCmd)
	assetsCmd.AddCommand(assetsDownloadCmd)
}

// assets inspect
var assetsInspectMatch string

var assetsInspectCmd = &cobra.Command{
	Use:   "inspect <asset-id>",
	Short: "Inspect an asset",
	Args:  cobra.ExactArgs(1),
	RunE:  runAssetsInspect,
}

func init() {
	assetsInspectCmd.Flags().StringVar(&assetsInspectMatch, "match", "", "Glob-style pattern to filter files")
}

func runAssetsInspect(cmd *cobra.Command, args []string) error {
	assetID := args[0]

	client, err := newClient()
	if err != nil {
		return err
	}

	asset, err := client.GetAssetByID(cmd.Context(), assetID)
	if err != nil {
		return fmt.Errorf("asset '%s' not found", assetID)
	}

	name := getString(asset, "name")
	if name == "" {
		name = "(untitled)"
	}
	fmt.Printf("Name: %s\n", name)

	entries, ok := asset["entries"].(map[string]any)
	if !ok {
		fmt.Println("No entries found.")
		return nil
	}

	// Filter entries if match pattern provided
	filteredEntries := make(map[string]map[string]any)
	for key, value := range entries {
		if entry, ok := value.(map[string]any); ok {
			if assetsInspectMatch == "" || matchGlob(assetsInspectMatch, key) {
				filteredEntries[key] = entry
			}
		}
	}

	if assetsInspectMatch != "" {
		fmt.Printf("Matched %d of %d entries.\n", len(filteredEntries), len(entries))
	}

	if len(filteredEntries) == 0 {
		fmt.Println("No entries to display.")
		return nil
	}

	var rows [][]string
	for path, entry := range filteredEntries {
		size := getInt64(entry, "size")
		blobKey := getString(entry, "blobKey")
		entryType := "(unknown)"
		if metadata, ok := entry["metadata"].(map[string]any); ok {
			if t := getString(metadata, "type"); t != "" {
				entryType = t
			}
		}
		rows = append(rows, []string{path, humanSize(size), entryType, blobKey})
	}

	printTable([]string{"Path", "Size", "Type", "Blob key"}, rows)
	return nil
}

// assets download
var (
	assetsDownloadTo    string
	assetsDownloadForce bool
	assetsDownloadMatch string
)

var assetsDownloadCmd = &cobra.Command{
	Use:   "download <asset-id>",
	Short: "Download the contents of an asset",
	Args:  cobra.ExactArgs(1),
	RunE:  runAssetsDownload,
}

func init() {
	assetsDownloadCmd.Flags().StringVar(&assetsDownloadTo, "to", ".", "The local path to download the contents to")
	assetsDownloadCmd.Flags().BoolVar(&assetsDownloadForce, "force", false, "Overwrite any existing files if present")
	assetsDownloadCmd.Flags().StringVar(&assetsDownloadMatch, "match", "", "Glob-style pattern to filter files")
}

func runAssetsDownload(cmd *cobra.Command, args []string) error {
	assetID := args[0]

	client, err := newClient()
	if err != nil {
		return err
	}

	asset, err := client.GetAssetByID(cmd.Context(), assetID)
	if err != nil {
		return fmt.Errorf("asset '%s' not found", assetID)
	}

	entries, ok := asset["entries"].(map[string]any)
	if !ok || len(entries) == 0 {
		fmt.Println("Nothing to download")
		return nil
	}

	// Filter entries if match pattern provided
	filteredEntries := make(map[string]map[string]any)
	for key, value := range entries {
		if entry, ok := value.(map[string]any); ok {
			if assetsDownloadMatch == "" || matchGlob(assetsDownloadMatch, key) {
				filteredEntries[key] = entry
			}
		}
	}

	if assetsDownloadMatch != "" {
		fmt.Printf("Matched %d of %d entries.\n", len(filteredEntries), len(entries))
	}

	if len(filteredEntries) == 0 {
		fmt.Println("Nothing to download")
		return nil
	}

	// Check that we won't overwrite existing files
	for key := range filteredEntries {
		destPath := filepath.Join(assetsDownloadTo, key)
		if info, statErr := os.Stat(destPath); statErr == nil {
			if !assetsDownloadForce {
				return fmt.Errorf("file already exists at path: %s (use --force to overwrite)", destPath)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("cannot overwrite non-file: %s", destPath)
			}
		}
	}

	// Create blob stores from config
	stores, err := createBlobStoresFromViper()
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

	// Calculate total size
	var totalSize int64
	for _, entry := range filteredEntries {
		totalSize += getInt64(entry, "size")
	}

	fmt.Printf("Downloading %d files (%s)...\n", len(filteredEntries), humanSize(totalSize))

	// Download each file
	for key, entry := range filteredEntries {
		blobKey := getString(entry, "blobKey")
		destPath := filepath.Join(assetsDownloadTo, key)

		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory for %s: %w", key, err)
		}

		// Download blob to cache
		cachePath, err := blobManager.Download(blobKey)
		if err != nil {
			return fmt.Errorf("failed to download %s: %w", key, err)
		}

		// Copy from cache to destination
		if err := copyFile(cachePath, destPath); err != nil {
			return fmt.Errorf("failed to copy %s: %w", key, err)
		}

		fmt.Printf("  %s\n", key)
	}

	fmt.Println("Download complete.")
	return nil
}

// Helper functions

func matchGlob(pattern, name string) bool {
	matched, _ := filepath.Match(pattern, name)
	if matched {
		return true
	}
	// Handle ** for recursive matching
	if strings.Contains(pattern, "**") {
		parts := strings.Split(pattern, "**")
		if len(parts) == 2 {
			prefix := parts[0]
			suffix := strings.TrimPrefix(parts[1], "/")
			if remainder, found := strings.CutPrefix(name, prefix); found {
				if suffix == "" {
					return true
				}
				matched, _ := filepath.Match(suffix, filepath.Base(remainder))
				return matched
			}
		}
	}
	return false
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		_ = dstFile.Close()
		return err
	}
	return dstFile.Close()
}

func createBlobStoresFromViper() ([]blob.Store, error) {
	var stores []blob.Store

	// Check for stores in config
	storeConfigs := viper.Get("blobs.stores")
	if storeConfigs == nil {
		// Default to HTTP store at server
		baseURL := fmt.Sprintf("%s://%s/blobs", map[bool]string{true: "https", false: "http"}[isSecure()], getHost())
		stores = append(stores, blob.NewHTTPStore(baseURL))
		return stores, nil
	}

	// Parse store configs
	if storeList, ok := storeConfigs.([]any); ok {
		for _, s := range storeList {
			if storeCfg, ok := s.(map[string]any); ok {
				storeType, _ := storeCfg["type"].(string)
				switch storeType {
				case "http":
					url, _ := storeCfg["url"].(string)
					stores = append(stores, blob.NewHTTPStore(url))
				case "s3":
					bucket, _ := storeCfg["bucket"].(string)
					prefix, _ := storeCfg["prefix"].(string)
					region, _ := storeCfg["region"].(string)
					store, err := blob.NewS3Store(context.Background(), bucket, prefix, region)
					if err != nil {
						return nil, err
					}
					stores = append(stores, store)
				}
			}
		}
	}

	if len(stores) == 0 {
		// Default to HTTP store at server
		baseURL := fmt.Sprintf("%s://%s/blobs", map[bool]string{true: "https", false: "http"}[isSecure()], getHost())
		stores = append(stores, blob.NewHTTPStore(baseURL))
	}

	return stores, nil
}
