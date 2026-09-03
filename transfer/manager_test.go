package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	transferprotocol "github.com/Aruelius/quickdrop-go/protocol"
	"github.com/pion/webrtc/v4"
)

type fakeMessageChannel struct {
	mu         sync.Mutex
	onMessage  func(webrtc.DataChannelMessage)
	onClose    func()
	onError    func(error)
	controls   chan transferprotocol.Control
	state      webrtc.DataChannelState
	sendErrors int
	attempts   [][]byte
}

func newFakeMessageChannel() *fakeMessageChannel {
	return &fakeMessageChannel{controls: make(chan transferprotocol.Control, 64), state: webrtc.DataChannelStateOpen}
}

func (f *fakeMessageChannel) Send(value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = append(f.attempts, append([]byte(nil), value...))
	if f.sendErrors > 0 {
		f.sendErrors--
		return errors.New("send queue full")
	}
	return nil
}
func (f *fakeMessageChannel) SendText(value string) error {
	var control transferprotocol.Control
	if json.Unmarshal([]byte(value), &control) == nil {
		f.controls <- control
	}
	return nil
}
func (f *fakeMessageChannel) BufferedAmount() uint64               { return 0 }
func (f *fakeMessageChannel) SetBufferedAmountLowThreshold(uint64) {}
func (f *fakeMessageChannel) ReadyState() webrtc.DataChannelState  { return f.state }
func (f *fakeMessageChannel) OnMessage(handler func(webrtc.DataChannelMessage)) {
	f.onMessage = handler
}
func (f *fakeMessageChannel) OnClose(handler func())      { f.onClose = handler }
func (f *fakeMessageChannel) OnError(handler func(error)) { f.onError = handler }
func (f *fakeMessageChannel) OnBufferedAmountLow(func())  {}
func (f *fakeMessageChannel) Close() error {
	f.state = webrtc.DataChannelStateClosed
	if f.onClose != nil {
		f.onClose()
	}
	return nil
}
func (f *fakeMessageChannel) receiveBinary(data []byte) {
	f.onMessage(webrtc.DataChannelMessage{Data: data})
}
func (f *fakeMessageChannel) receiveControl(control transferprotocol.Control) {
	encoded, _ := json.Marshal(control)
	f.onMessage(webrtc.DataChannelMessage{Data: encoded, IsString: true})
}

func waitForControl(t *testing.T, channel *fakeMessageChannel, kind string) transferprotocol.Control {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case control := <-channel.controls:
			if control.Type == kind {
				return control
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}

func waitForEvent(t *testing.T, events <-chan TransferEvent, kind string) TransferEvent {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Type == kind {
				return event
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}

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

func TestReceiveCompletesWhenParallelChunksAndEndArriveOutOfOrder(t *testing.T) {
	directory := t.TempDir()
	content := []byte("abcdefghijklmnopqrstuvwx")
	digest := sha256.Sum256(content)
	metadata := transferprotocol.FileMetadata{
		Name: "parallel.bin", Size: int64(len(content)), MIME: "application/octet-stream", ChunkSize: 8,
		TotalChunks: 3, SHA256: hex.EncodeToString(digest[:]), CommitAck: true,
	}
	channel := newFakeMessageChannel()
	events := make(chan TransferEvent, 128)
	manager := NewTransferManager(channel, 64*1024, TransferOptions{
		ReceiveDir: directory, AutoAccept: true, DurableAckInterval: 16, OnEvent: func(event TransferEvent) { events <- event },
	})
	transferID := "parallel-transfer"
	channel.receiveControl(transferprotocol.Control{Version: 1, Type: "file_start", TransferID: transferID, File: &metadata})
	waitForControl(t, channel, "file_accept")

	sendChunk := func(index int64) {
		offset := index * 8
		packet, err := transferprotocol.EncodeChunk(transferprotocol.ChunkHeader{TransferID: transferID, Index: index, Offset: offset, Length: 8}, content[offset:offset+8])
		if err != nil {
			t.Fatal(err)
		}
		channel.receiveBinary(packet)
	}
	sendChunk(2)
	channel.receiveControl(transferprotocol.Control{Version: 1, Type: "file_end", TransferID: transferID, SHA256: metadata.SHA256})
	sendChunk(0)
	sendChunk(1)
	waitForEvent(t, events, "completed")
	received, err := os.ReadFile(filepath.Join(directory, metadata.Name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, content) {
		t.Fatalf("received=%q", received)
	}
	for {
		ack := waitForControl(t, channel, "file_ack")
		if ack.ReceivedBytes == metadata.Size {
			break
		}
	}
	_ = manager
}

func TestReceiveRejectsDuplicatePendingChunk(t *testing.T) {
	metadata := transferprotocol.FileMetadata{Name: "duplicate.bin", Size: 24, MIME: "application/octet-stream", ChunkSize: 8, TotalChunks: 3, CommitAck: true}
	channel := newFakeMessageChannel()
	events := make(chan TransferEvent, 64)
	NewTransferManager(channel, 64*1024, TransferOptions{ReceiveDir: t.TempDir(), AutoAccept: true, OnEvent: func(event TransferEvent) { events <- event }})
	transferID := "duplicate-transfer"
	channel.receiveControl(transferprotocol.Control{Version: 1, Type: "file_start", TransferID: transferID, File: &metadata})
	waitForControl(t, channel, "file_accept")
	packet, _ := transferprotocol.EncodeChunk(transferprotocol.ChunkHeader{TransferID: transferID, Index: 2, Offset: 16, Length: 8}, make([]byte, 8))
	channel.receiveBinary(packet)
	channel.receiveBinary(packet)
	failed := waitForEvent(t, events, "failed")
	if failed.Message != "duplicate pending file chunk" {
		t.Fatalf("message=%q", failed.Message)
	}
}

func TestReceiveTimesOutAfterEndWithMissingChunk(t *testing.T) {
	metadata := transferprotocol.FileMetadata{Name: "missing.bin", Size: 16, MIME: "application/octet-stream", ChunkSize: 8, TotalChunks: 2, CommitAck: true}
	channel := newFakeMessageChannel()
	events := make(chan TransferEvent, 64)
	NewTransferManager(channel, 64*1024, TransferOptions{
		ReceiveDir: t.TempDir(), AutoAccept: true, ReceiveCompletionTimeout: 25 * time.Millisecond,
		OnEvent: func(event TransferEvent) { events <- event },
	})
	transferID := "missing-transfer"
	channel.receiveControl(transferprotocol.Control{Version: 1, Type: "file_start", TransferID: transferID, File: &metadata})
	waitForControl(t, channel, "file_accept")
	packet, _ := transferprotocol.EncodeChunk(transferprotocol.ChunkHeader{TransferID: transferID, Index: 0, Offset: 0, Length: 8}, make([]byte, 8))
	channel.receiveBinary(packet)
	channel.receiveControl(transferprotocol.Control{Version: 1, Type: "file_end", TransferID: transferID})
	failed := waitForEvent(t, events, "failed")
	if failed.Message != "timed out waiting for missing file chunks" {
		t.Fatalf("message=%q", failed.Message)
	}
}

func TestReceiveRejectsReorderBufferOverflow(t *testing.T) {
	metadata := transferprotocol.FileMetadata{Name: "overflow.bin", Size: 16, MIME: "application/octet-stream", ChunkSize: 8, TotalChunks: 2, CommitAck: true}
	channel := newFakeMessageChannel()
	events := make(chan TransferEvent, 64)
	NewTransferManager(channel, 64*1024, TransferOptions{
		ReceiveDir: t.TempDir(), AutoAccept: true, MaxReorderBufferBytes: 7,
		OnEvent: func(event TransferEvent) { events <- event },
	})
	transferID := "overflow-transfer"
	channel.receiveControl(transferprotocol.Control{Version: 1, Type: "file_start", TransferID: transferID, File: &metadata})
	waitForControl(t, channel, "file_accept")
	packet, _ := transferprotocol.EncodeChunk(transferprotocol.ChunkHeader{TransferID: transferID, Index: 1, Offset: 8, Length: 8}, make([]byte, 8))
	channel.receiveBinary(packet)
	failed := waitForEvent(t, events, "failed")
	if failed.Message != "file chunk reorder buffer exceeded" {
		t.Fatalf("message=%q", failed.Message)
	}
}

func TestReceiveNegotiatesNonDurableProgressAcknowledgements(t *testing.T) {
	for _, test := range []struct {
		name        string
		progressAck bool
		wantType    string
	}{
		{name: "new sender", progressAck: true, wantType: "file_progress"},
		{name: "legacy sender", progressAck: false, wantType: "file_ack"},
	} {
		t.Run(test.name, func(t *testing.T) {
			const chunkSize = 512 * 1024
			metadata := transferprotocol.FileMetadata{
				Name: "progress.bin", Size: 2 * chunkSize, MIME: "application/octet-stream",
				ChunkSize: chunkSize, TotalChunks: 2, CommitAck: true, ProgressAck: test.progressAck,
			}
			channel := newFakeMessageChannel()
			events := make(chan TransferEvent, 64)
			NewTransferManager(channel, chunkSize+4096, TransferOptions{
				ReceiveDir: t.TempDir(), AutoAccept: true,
				OnEvent: func(event TransferEvent) { events <- event },
			})
			transferID := "progress-transfer"
			channel.receiveControl(transferprotocol.Control{Version: 1, Type: "file_start", TransferID: transferID, File: &metadata})
			waitForControl(t, channel, "file_accept")
			packet, err := transferprotocol.EncodeChunk(
				transferprotocol.ChunkHeader{TransferID: transferID, Index: 0, Offset: 0, Length: chunkSize},
				make([]byte, chunkSize),
			)
			if err != nil {
				t.Fatal(err)
			}
			channel.receiveBinary(packet)
			progress := waitForControl(t, channel, test.wantType)
			if progress.ReceivedBytes != chunkSize {
				t.Fatalf("receivedBytes=%d", progress.ReceivedBytes)
			}
			channel.receiveControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: transferID, Reason: "test complete"})
			waitForEvent(t, events, "cancelled")
		})
	}
}

func TestSendBinaryRetriesTheSamePacketWhenQueueIsTemporarilyFull(t *testing.T) {
	channel := newFakeMessageChannel()
	channel.sendErrors = 1
	manager := NewTransferManager(channel, 64*1024, TransferOptions{})
	packet := []byte("same-packet")
	if err := manager.sendBinary(context.Background(), packet); err != nil {
		t.Fatal(err)
	}
	channel.mu.Lock()
	defer channel.mu.Unlock()
	if len(channel.attempts) != 2 || !bytes.Equal(channel.attempts[0], packet) || !bytes.Equal(channel.attempts[1], packet) {
		t.Fatalf("attempts=%q", channel.attempts)
	}
}
