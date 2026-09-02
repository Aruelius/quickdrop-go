// Package transfer implements bounded, resumable and integrity-checked
// QuickDrop transfers over a WebRTC DataChannel.
package transfer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	transferprotocol "github.com/Aruelius/quickdrop-go/protocol"
	"github.com/pion/webrtc/v4"
)

const (
	maxBufferedAmount  = 2 * 1024 * 1024
	maxUnacknowledged  = 8 * 1024 * 1024
	durableAckInterval = 8 * 1024 * 1024
)

type TransferOptions struct {
	ReceiveDir        string
	AutoAccept        bool
	AcceptFile        func(transferprotocol.FileMetadata) bool
	Overwrite         bool
	MaxFileSize       int64
	BandwidthLimitBPS uint64
	OnEvent           func(TransferEvent)
}

type TransferEvent struct {
	Type        string `json:"type"`
	TransferID  string `json:"transferId,omitempty"`
	Name        string `json:"name,omitempty"`
	Direction   string `json:"direction,omitempty"`
	Transferred int64  `json:"transferred,omitempty"`
	Size        int64  `json:"size,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	Path        string `json:"path,omitempty"`
	Text        string `json:"text,omitempty"`
	Message     string `json:"message,omitempty"`
}

type TransferManager struct {
	channel        *webrtc.DataChannel
	options        TransferOptions
	chunkSize      int
	mu             sync.Mutex
	receiveStartMu sync.Mutex
	senders        map[string]*sendState
	receivers      map[string]*receiveState
	closed         chan struct{}
	closeOnce      sync.Once
}

type sendState struct {
	accept chan int64
	acks   chan struct{}
	cancel chan error
	mu     sync.Mutex
	acked  int64
	once   sync.Once
}

type receiveEvent struct {
	header *transferprotocol.ChunkHeader
	data   []byte
	end    *transferprotocol.Control
	cancel error
}

type receiveState struct {
	metadata transferprotocol.FileMetadata
	path     string
	partPath string
	metaPath string
	file     *os.File
	events   chan receiveEvent
}

type partialMetadata struct {
	File          transferprotocol.FileMetadata `json:"file"`
	ReceivedBytes int64                         `json:"receivedBytes"`
}

func NewTransferManager(channel *webrtc.DataChannel, maxMessageSize uint32, options TransferOptions) *TransferManager {
	chunkSize := transferprotocol.DefaultChunkSize
	if maxMessageSize > 4096 {
		chunkSize = min(transferprotocol.TargetChunkSize, int(maxMessageSize)-4096)
	}
	manager := &TransferManager{channel: channel, options: options, chunkSize: chunkSize, senders: make(map[string]*sendState), receivers: make(map[string]*receiveState), closed: make(chan struct{})}
	channel.OnMessage(manager.handleMessage)
	channel.OnClose(func() { manager.closeOnce.Do(func() { close(manager.closed) }) })
	channel.OnError(func(err error) { manager.emit(TransferEvent{Type: "error", Message: err.Error()}) })
	return manager
}

func (m *TransferManager) Closed() <-chan struct{} { return m.closed }

func (m *TransferManager) SendText(text string) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return errors.New("text is empty")
	}
	payload, _ := json.Marshal(map[string]string{"text": text})
	message := transferprotocol.Control{Version: 1, Type: "text", ID: randomID(), Timestamp: time.Now().UnixMilli(), Payload: payload}
	if err := m.sendControl(message); err != nil {
		return err
	}
	m.emit(TransferEvent{Type: "text", Direction: "send", Text: text})
	return nil
}

func (m *TransferManager) SendFile(ctx context.Context, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("only regular files are supported")
	}
	if m.options.MaxFileSize > 0 && info.Size() > m.options.MaxFileSize {
		return fmt.Errorf("file exceeds configured maximum of %d bytes", m.options.MaxFileSize)
	}

	m.emit(TransferEvent{Type: "hashing", Direction: "send", Name: info.Name(), Size: info.Size()})
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("hash file: %w", err)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	transferID := randomID()
	metadata := transferprotocol.FileMetadata{Name: info.Name(), Size: info.Size(), MIME: mime.TypeByExtension(filepath.Ext(info.Name())), ChunkSize: m.chunkSize, TotalChunks: transferprotocol.TotalChunks(info.Size(), m.chunkSize), LastModified: info.ModTime().UnixMilli(), SHA256: digest, CommitAck: true, Resume: true}
	if metadata.MIME == "" {
		metadata.MIME = "application/octet-stream"
	}
	state := &sendState{accept: make(chan int64, 1), acks: make(chan struct{}, 1), cancel: make(chan error, 1)}
	m.mu.Lock()
	m.senders[transferID] = state
	m.mu.Unlock()
	defer func() { m.mu.Lock(); delete(m.senders, transferID); m.mu.Unlock() }()
	if err := m.sendControl(transferprotocol.Control{Version: 1, Type: "file_start", TransferID: transferID, File: &metadata}); err != nil {
		return err
	}
	m.emit(TransferEvent{Type: "waiting", TransferID: transferID, Direction: "send", Name: metadata.Name, Size: metadata.Size, SHA256: digest})

	var offset int64
	select {
	case offset = <-state.accept:
	case err := <-state.cancel:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Minute):
		return errors.New("receiver did not accept the file")
	}
	if offset < 0 || offset > info.Size() || offset%int64(m.chunkSize) != 0 {
		offset = 0
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	index := offset / int64(m.chunkSize)
	buffer := make([]byte, m.chunkSize)
	started := time.Now()
	sentThisRun := int64(0)
	for offset < info.Size() {
		if err := waitForBuffer(ctx, m.channel); err != nil {
			return err
		}
		state.mu.Lock()
		acked := state.acked
		state.mu.Unlock()
		if offset-acked >= maxUnacknowledged {
			select {
			case <-state.acks:
				continue
			case err := <-state.cancel:
				return err
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second):
				return errors.New("receiver acknowledgement timed out")
			}
		}
		want := min(int64(len(buffer)), info.Size()-offset)
		n, readErr := io.ReadFull(file, buffer[:want])
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return readErr
		}
		if n == 0 {
			break
		}
		packet, err := transferprotocol.EncodeChunk(transferprotocol.ChunkHeader{TransferID: transferID, Index: index, Offset: offset, Length: n}, buffer[:n])
		if err != nil {
			return err
		}
		if err := m.pace(ctx, sentThisRun+int64(len(packet)), started); err != nil {
			return err
		}
		if err := m.channel.Send(packet); err != nil {
			return err
		}
		offset += int64(n)
		index++
		sentThisRun += int64(len(packet))
		m.emit(TransferEvent{Type: "progress", TransferID: transferID, Direction: "send", Name: metadata.Name, Transferred: offset, Size: metadata.Size})
	}
	if err := m.sendControl(transferprotocol.Control{Version: 1, Type: "file_end", TransferID: transferID, SHA256: digest}); err != nil {
		return err
	}
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		state.mu.Lock()
		acked := state.acked
		state.mu.Unlock()
		if acked >= info.Size() {
			break
		}
		select {
		case <-state.acks:
		case err := <-state.cancel:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("final receiver acknowledgement timed out")
		}
	}
	m.emit(TransferEvent{Type: "completed", TransferID: transferID, Direction: "send", Name: metadata.Name, Transferred: metadata.Size, Size: metadata.Size, SHA256: digest})
	return nil
}

func (m *TransferManager) handleMessage(message webrtc.DataChannelMessage) {
	if message.IsString {
		control, err := transferprotocol.DecodeControl(message.Data)
		if err == nil {
			m.handleControl(control)
		}
		return
	}
	header, payload, err := transferprotocol.DecodeChunk(message.Data)
	if err != nil {
		m.emit(TransferEvent{Type: "error", Message: err.Error()})
		return
	}
	m.mu.Lock()
	receiver := m.receivers[header.TransferID]
	m.mu.Unlock()
	if receiver == nil {
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: header.TransferID, Reason: "unknown transfer"})
		return
	}
	data := append([]byte(nil), payload...)
	select {
	case receiver.events <- receiveEvent{header: &header, data: data}:
	default:
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: header.TransferID, Reason: "receiver is overloaded"})
	}
}

func (m *TransferManager) handleControl(message transferprotocol.Control) {
	switch message.Type {
	case "text":
		var payload struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(message.Payload, &payload) == nil && payload.Text != "" {
			m.emit(TransferEvent{Type: "text", Direction: "receive", Text: payload.Text})
		}
	case "file_start", "resume_query":
		if message.File != nil {
			go m.startReceive(message.TransferID, *message.File)
		}
	case "file_accept":
		m.acceptSender(message.TransferID, 0)
	case "resume_state":
		m.acceptSender(message.TransferID, message.ReceivedBytes)
	case "file_ack":
		m.mu.Lock()
		sender := m.senders[message.TransferID]
		m.mu.Unlock()
		if sender != nil {
			sender.mu.Lock()
			if message.ReceivedBytes > sender.acked {
				sender.acked = message.ReceivedBytes
			}
			sender.mu.Unlock()
			select {
			case sender.acks <- struct{}{}:
			default:
			}
		}
	case "file_end":
		m.mu.Lock()
		receiver := m.receivers[message.TransferID]
		m.mu.Unlock()
		if receiver != nil {
			receiver.events <- receiveEvent{end: &message}
		}
	case "file_cancel":
		m.mu.Lock()
		receiver := m.receivers[message.TransferID]
		sender := m.senders[message.TransferID]
		m.mu.Unlock()
		if receiver != nil {
			reason := strings.TrimSpace(message.Reason)
			if reason == "" {
				reason = "sender cancelled the transfer"
			}
			select {
			case receiver.events <- receiveEvent{cancel: errors.New(reason)}:
			default:
				m.receiveFailed(message.TransferID, receiver.metadata.Name, errors.New(reason))
			}
		}
		if sender != nil {
			reason := strings.TrimSpace(message.Reason)
			if reason == "" {
				reason = "receiver cancelled the transfer"
			}
			sender.once.Do(func() { sender.cancel <- errors.New(reason) })
		}
	}
}

func (m *TransferManager) startReceive(transferID string, metadata transferprotocol.FileMetadata) {
	m.receiveStartMu.Lock()
	defer m.receiveStartMu.Unlock()
	if transferID == "" || !validFileMetadata(metadata) {
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: transferID, Reason: "invalid file metadata"})
		return
	}
	if m.options.MaxFileSize > 0 && metadata.Size > m.options.MaxFileSize {
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: transferID, Reason: "file exceeds receiver limit"})
		return
	}
	if !m.options.AutoAccept && (m.options.AcceptFile == nil || !m.options.AcceptFile(metadata)) {
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: transferID, Reason: "receiver rejected the file"})
		return
	}
	m.mu.Lock()
	_, duplicate := m.receivers[transferID]
	m.mu.Unlock()
	if duplicate {
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: transferID, Reason: "duplicate transfer"})
		return
	}
	directory := m.options.ReceiveDir
	if directory == "" {
		directory = "."
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		m.receiveFailed(transferID, metadata.Name, err)
		return
	}
	name := filepath.Base(strings.ReplaceAll(metadata.Name, "\\", "/"))
	if name == "." || name == "" {
		m.receiveFailed(transferID, metadata.Name, errors.New("invalid file name"))
		return
	}
	target := filepath.Join(directory, name)
	partPath, metaPath := target+".quickdrop.part", target+".quickdrop.json"
	m.mu.Lock()
	for _, active := range m.receivers {
		if active.path == target {
			m.mu.Unlock()
			m.receiveFailed(transferID, name, errors.New("another transfer is already writing this destination"))
			return
		}
	}
	m.mu.Unlock()
	if !m.options.Overwrite {
		if _, err := os.Lstat(target); err == nil {
			m.receiveFailed(transferID, name, errors.New("destination already exists; use --overwrite"))
			return
		}
	}
	for _, candidate := range []string{partPath, metaPath, metaPath + ".new"} {
		if info, err := os.Lstat(candidate); err == nil && info.Mode()&os.ModeSymlink != 0 {
			m.receiveFailed(transferID, name, fmt.Errorf("refusing symbolic link at %s", candidate))
			return
		}
	}
	resumeOffset := matchingPartial(metaPath, partPath, metadata)
	if !metadata.Resume {
		resumeOffset = 0
	}
	available, err := availableDiskBytes(directory)
	if err != nil {
		m.receiveFailed(transferID, name, fmt.Errorf("check free disk space: %w", err))
		return
	}
	remaining := uint64(metadata.Size - resumeOffset)
	if resumeOffset == 0 {
		if info, statErr := os.Stat(partPath); statErr == nil && info.Mode().IsRegular() {
			if reclaim := uint64(info.Size()); ^uint64(0)-available > reclaim {
				available += reclaim
			}
		}
	}
	if available < remaining {
		m.receiveFailed(transferID, name, fmt.Errorf("insufficient disk space: need %d bytes, have %d", remaining, available))
		return
	}
	flags := os.O_CREATE | os.O_RDWR
	if resumeOffset == 0 {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(partPath, flags, 0o640)
	if err != nil {
		m.receiveFailed(transferID, name, err)
		return
	}
	if _, err := file.Seek(resumeOffset, io.SeekStart); err != nil {
		_ = file.Close()
		m.receiveFailed(transferID, name, err)
		return
	}
	state := &receiveState{metadata: metadata, path: target, partPath: partPath, metaPath: metaPath, file: file, events: make(chan receiveEvent, 64)}
	m.mu.Lock()
	if _, exists := m.receivers[transferID]; exists {
		m.mu.Unlock()
		_ = file.Close()
		return
	}
	m.receivers[transferID] = state
	m.mu.Unlock()
	if err := writePartialMetadata(metaPath, partialMetadata{File: metadata, ReceivedBytes: resumeOffset}); err != nil {
		m.finishReceiver(transferID)
		m.receiveFailed(transferID, name, err)
		return
	}
	go m.receiveLoop(transferID, state, resumeOffset)
	if resumeOffset > 0 {
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "resume_state", TransferID: transferID, ReceivedBytes: resumeOffset})
	} else {
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_accept", TransferID: transferID, Acknowledgements: true})
	}
	m.emit(TransferEvent{Type: "accepted", TransferID: transferID, Direction: "receive", Name: name, Transferred: resumeOffset, Size: metadata.Size, Path: target})
}

func (m *TransferManager) receiveLoop(transferID string, state *receiveState, offset int64) {
	expectedOffset := offset
	expectedIndex := offset / int64(state.metadata.ChunkSize)
	hash := sha256.New()
	if offset > 0 {
		if _, err := state.file.Seek(0, io.SeekStart); err != nil {
			m.receiveFailed(transferID, state.metadata.Name, err)
			return
		}
		if _, err := io.CopyN(hash, state.file, offset); err != nil {
			m.receiveFailed(transferID, state.metadata.Name, err)
			return
		}
		if _, err := state.file.Seek(offset, io.SeekStart); err != nil {
			m.receiveFailed(transferID, state.metadata.Name, err)
			return
		}
	}
	lastDurable := offset
	for event := range state.events {
		if event.cancel != nil {
			m.finishReceiver(transferID)
			m.emit(TransferEvent{Type: "cancelled", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Message: event.cancel.Error()})
			return
		}
		if event.header != nil {
			header := event.header
			if header.TransferID != transferID || header.Offset != expectedOffset || header.Index != expectedIndex || header.Length != len(event.data) || expectedOffset+int64(len(event.data)) > state.metadata.Size {
				m.receiveFailed(transferID, state.metadata.Name, errors.New("file chunk order or length mismatch"))
				return
			}
			if _, err := state.file.Write(event.data); err != nil {
				m.receiveFailed(transferID, state.metadata.Name, err)
				return
			}
			_, _ = hash.Write(event.data)
			expectedOffset += int64(len(event.data))
			expectedIndex++
			if expectedOffset-lastDurable >= durableAckInterval || expectedOffset == state.metadata.Size {
				if err := state.file.Sync(); err != nil {
					m.receiveFailed(transferID, state.metadata.Name, err)
					return
				}
				lastDurable = expectedOffset
				_ = writePartialMetadata(state.metaPath, partialMetadata{File: state.metadata, ReceivedBytes: lastDurable})
				// The final acknowledgement is sent only after SHA-256 verification and
				// the partial file has been atomically promoted to its destination.
				if expectedOffset < state.metadata.Size || !state.metadata.CommitAck {
					_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_ack", TransferID: transferID, ReceivedBytes: lastDurable})
				}
			}
			m.emit(TransferEvent{Type: "progress", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Transferred: expectedOffset, Size: state.metadata.Size, Path: state.path})
			continue
		}
		if event.end != nil {
			if expectedOffset != state.metadata.Size {
				m.receiveFailed(transferID, state.metadata.Name, errors.New("file ended before announced size"))
				return
			}
			digest := hex.EncodeToString(hash.Sum(nil))
			expectedHash := state.metadata.SHA256
			if expectedHash == "" {
				expectedHash = event.end.SHA256
			}
			if expectedHash != "" && !strings.EqualFold(expectedHash, digest) {
				m.receiveIntegrityFailed(transferID, state, errors.New("SHA-256 verification failed"))
				return
			}
			if err := state.file.Sync(); err != nil {
				m.receiveFailed(transferID, state.metadata.Name, err)
				return
			}
			if err := state.file.Close(); err != nil {
				m.receiveFailed(transferID, state.metadata.Name, err)
				return
			}
			if m.options.Overwrite {
				_ = os.Remove(state.path)
			}
			if err := os.Rename(state.partPath, state.path); err != nil {
				m.receiveFailed(transferID, state.metadata.Name, err)
				return
			}
			_ = os.Remove(state.metaPath)
			_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_ack", TransferID: transferID, ReceivedBytes: state.metadata.Size})
			m.finishReceiver(transferID)
			m.emit(TransferEvent{Type: "completed", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Transferred: state.metadata.Size, Size: state.metadata.Size, SHA256: digest, Path: state.path})
			return
		}
	}
}

func (m *TransferManager) acceptSender(transferID string, offset int64) {
	m.mu.Lock()
	sender := m.senders[transferID]
	m.mu.Unlock()
	if sender == nil {
		return
	}
	select {
	case sender.accept <- offset:
	default:
	}
}

func (m *TransferManager) receiveFailed(transferID, name string, err error) {
	_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: transferID, Reason: err.Error()})
	m.finishReceiver(transferID)
	m.emit(TransferEvent{Type: "failed", TransferID: transferID, Direction: "receive", Name: name, Message: err.Error()})
}

func (m *TransferManager) receiveIntegrityFailed(transferID string, state *receiveState, err error) {
	_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_cancel", TransferID: transferID, Reason: err.Error()})
	m.finishReceiver(transferID)
	_ = os.Remove(state.partPath)
	_ = os.Remove(state.metaPath)
	m.emit(TransferEvent{Type: "failed", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Message: err.Error()})
}

func (m *TransferManager) finishReceiver(transferID string) {
	m.mu.Lock()
	state := m.receivers[transferID]
	delete(m.receivers, transferID)
	m.mu.Unlock()
	if state != nil {
		_ = state.file.Close()
	}
}

func (m *TransferManager) sendControl(value transferprotocol.Control) error {
	encoded, err := transferprotocol.EncodeControl(value)
	if err != nil {
		return err
	}
	return m.channel.SendText(string(encoded))
}

func (m *TransferManager) pace(ctx context.Context, bytes int64, started time.Time) error {
	if m.options.BandwidthLimitBPS == 0 {
		return nil
	}
	want := time.Duration(float64(bytes) / float64(m.options.BandwidthLimitBPS) * float64(time.Second))
	wait := want - time.Since(started)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *TransferManager) emit(event TransferEvent) {
	if m.options.OnEvent != nil {
		m.options.OnEvent(event)
	}
}

func waitForBuffer(ctx context.Context, channel *webrtc.DataChannel) error {
	for channel.BufferedAmount() >= maxBufferedAmount {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
		if channel.ReadyState() != webrtc.DataChannelStateOpen {
			return errors.New("DataChannel is not open")
		}
	}
	return nil
}

func validFileMetadata(metadata transferprotocol.FileMetadata) bool {
	return metadata.Name != "" && len(metadata.Name) <= 255 && metadata.Size >= 0 && metadata.ChunkSize > 0 && metadata.ChunkSize <= 1024*1024 && metadata.TotalChunks == transferprotocol.TotalChunks(metadata.Size, metadata.ChunkSize) && (metadata.SHA256 == "" || validSHA256(metadata.SHA256))
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func matchingPartial(metaPath, partPath string, offered transferprotocol.FileMetadata) int64 {
	encoded, err := os.ReadFile(metaPath)
	if err != nil {
		return 0
	}
	var partial partialMetadata
	if json.Unmarshal(encoded, &partial) != nil || partial.File.Name != offered.Name || partial.File.Size != offered.Size || partial.File.LastModified != offered.LastModified || partial.File.ChunkSize != offered.ChunkSize || (partial.File.SHA256 != "" && offered.SHA256 != "" && !strings.EqualFold(partial.File.SHA256, offered.SHA256)) {
		return 0
	}
	info, err := os.Stat(partPath)
	if err != nil || info.Size() != partial.ReceivedBytes || partial.ReceivedBytes < 0 || partial.ReceivedBytes > offered.Size || partial.ReceivedBytes%int64(offered.ChunkSize) != 0 {
		return 0
	}
	return partial.ReceivedBytes
}

func writePartialMetadata(path string, metadata partialMetadata) error {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	temporary := path + ".new"
	if err := os.WriteFile(temporary, encoded, 0o600); err != nil {
		return err
	}
	return replaceFile(temporary, path)
}

func randomID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buffer)
}
