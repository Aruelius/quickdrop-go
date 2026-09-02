package transfer

import (
	"os"
	"path/filepath"
	"testing"

	transferprotocol "github.com/Aruelius/quickdrop-go/protocol"
)

func TestMatchingPartialRequiresAlignedIdenticalMetadata(t *testing.T) {
	directory := t.TempDir()
	partPath := filepath.Join(directory, "archive.bin.quickdrop.part")
	metaPath := filepath.Join(directory, "archive.bin.quickdrop.json")
	metadata := transferprotocol.FileMetadata{Name: "archive.bin", Size: 4096, MIME: "application/octet-stream", ChunkSize: 1024, TotalChunks: 4, LastModified: 1234, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := os.WriteFile(partPath, make([]byte, 2048), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writePartialMetadata(metaPath, partialMetadata{File: metadata, ReceivedBytes: 2048}); err != nil {
		t.Fatal(err)
	}
	if offset := matchingPartial(metaPath, partPath, metadata); offset != 2048 {
		t.Fatalf("offset=%d", offset)
	}
	changed := metadata
	changed.SHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if offset := matchingPartial(metaPath, partPath, changed); offset != 0 {
		t.Fatalf("changed file resumed at %d", offset)
	}
	if err := os.Truncate(partPath, 2047); err != nil {
		t.Fatal(err)
	}
	if offset := matchingPartial(metaPath, partPath, metadata); offset != 0 {
		t.Fatalf("misaligned partial resumed at %d", offset)
	}
}

func TestAvailableDiskBytes(t *testing.T) {
	available, err := availableDiskBytes(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if available == 0 {
		t.Fatal("available disk space is zero")
	}
}
