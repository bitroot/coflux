package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/bitroot/coflux/cli/internal/blob"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var blobsCmd = &cobra.Command{
	Use:   "blobs",
	Short: "Manage blobs",
}

func init() {
	blobsCmd.AddCommand(blobsGetCmd)
}

// blobs get
var blobsGetOutput string

var blobsGetCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Gets a blob by key and writes the content to stdout or a file",
	Args:  cobra.ExactArgs(1),
	RunE:  runBlobsGet,
}

func init() {
	blobsGetCmd.Flags().StringVarP(&blobsGetOutput, "output", "o", "", "Output file path (default: stdout)")
}

func runBlobsGet(cmd *cobra.Command, args []string) error {
	key := args[0]

	// Create blob stores from config
	stores, err := createBlobStoresFromViper()
	if err != nil {
		return fmt.Errorf("failed to create blob stores: %w", err)
	}
	if len(stores) == 0 {
		return fmt.Errorf("blob store not configured")
	}

	blobManager := blob.NewManager(stores, filepath.Join(os.TempDir(), "coflux-cache", "blobs"), viper.GetInt("blobs.threshold"))

	// Get the blob
	reader, err := blobManager.Get(key)
	if err != nil {
		return fmt.Errorf("blob not found: %s", key)
	}
	defer func() { _ = reader.Close() }()

	// Write to output
	var file *os.File
	var out io.Writer
	if blobsGetOutput != "" {
		file, err = os.Create(blobsGetOutput)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		out = file
	} else {
		out = os.Stdout
	}

	if _, err := io.Copy(out, reader); err != nil {
		if file != nil {
			_ = file.Close()
		}
		return fmt.Errorf("failed to write blob: %w", err)
	}
	if file != nil {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close output file: %w", err)
		}
	}

	return nil
}
