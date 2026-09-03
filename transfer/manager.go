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

	quickdropchannel "github.com/Aruelius/quickdrop-go/channel"
	transferprotocol "github.com/Aruelius/quickdrop-go/protocol"
	"github.com/pion/webrtc/v4"
)

const (
	maxBufferedAmount         = 2 * 1024 * 1024
	maxUnacknowledged         = 8 * 1024 * 1024
	defaultDurableAckInterval = 32 * 1024 * 1024
	maxReorderBufferBytes     = 64 * 1024 * 1024
	receiveEventQueueSize     = 256
	progressAckInterval       = 512 * 1024
	progressAckPeriod         = 100 * time.Millisecond
)

type TransferOptions struct {
	ReceiveDir               string
	AutoAccept               bool
	AcceptFile               func(transferprotocol.FileMetadata) bool
	Overwrite                bool
	MaxFileSize              int64
	BandwidthLimitBPS        uint64
	DurableAckInterval       int64
	MaxReorderBufferBytes    int64
	ReceiveCompletionTimeout time.Duration
	OnEvent                  func(TransferEvent)
}

type TransferEvent struct {
	Type                 string `json:"type"`
	TransferID           string `json:"transferId,omitempty"`
	Name                 string `json:"name,omitempty"`
	Direction            string `json:"direction,omitempty"`
	Transferred          int64  `json:"transferred,omitempty"`
	Size                 int64  `json:"size,omitempty"`
	SHA256               string `json:"sha256,omitempty"`
	Path                 string `json:"path,omitempty"`
	Text                 string `json:"text,omitempty"`
	Message              string `json:"message,omitempty"`
	CheckpointDurationMS int64  `json:"checkpointDurationMs,omitempty"`
}

type TransferManager struct {
	channel        quickdropchannel.Channel
	options        TransferOptions
	chunkSize      int
	mu             sync.Mutex
	receiveStartMu sync.Mutex
	senders        map[string]*sendState
	receivers      map[string]*receiveState
	closed         chan struct{}
	writable       chan struct{}
	closeOnce      sync.Once
}

type sendState struct {
	accept   chan int64
	acks     chan struct{}
	cancel   chan error
	mu       sync.Mutex
	acked    int64
	received int64
	name     string
	size     int64
	once     sync.Once
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
	done     chan struct{}
	doneOnce sync.Once
}

type partialMetadata struct {
	File          transferprotocol.FileMetadata `json:"file"`
	ReceivedBytes int64                         `json:"receivedBytes"`
}

func NewTransferManager(channel quickdropchannel.Channel, maxMessageSize uint32, options TransferOptions) *TransferManager {
	chunkSize := transferprotocol.DefaultChunkSize
	if maxMessageSize > 4096 {
		chunkSize = min(transferprotocol.TargetChunkSize, int(maxMessageSize)-4096)
	}
	if options.DurableAckInterval <= 0 {
		options.DurableAckInterval = defaultDurableAckInterval
	}
	if options.MaxReorderBufferBytes <= 0 {
		options.MaxReorderBufferBytes = maxReorderBufferBytes
	}
	if options.ReceiveCompletionTimeout <= 0 {
		options.ReceiveCompletionTimeout = 30 * time.Second
	}
	manager := &TransferManager{channel: channel, options: options, chunkSize: chunkSize, senders: make(map[string]*sendState), receivers: make(map[string]*receiveState), closed: make(chan struct{}), writable: make(chan struct{}, 1)}
	channel.OnMessage(manager.handleMessage)
	channel.SetBufferedAmountLowThreshold(maxBufferedAmount / 2)
	channel.OnBufferedAmountLow(func() {
		select {
		case manager.writable <- struct{}{}:
		default:
		}
	})
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
	metadata := transferprotocol.FileMetadata{Name: info.Name(), Size: info.Size(), MIME: mime.TypeByExtension(filepath.Ext(info.Name())), ChunkSize: m.chunkSize, TotalChunks: transferprotocol.TotalChunks(info.Size(), m.chunkSize), LastModified: info.ModTime().UnixMilli(), SHA256: digest, CommitAck: true, ProgressAck: true, Resume: true}
	if metadata.MIME == "" {
		metadata.MIME = "application/octet-stream"
	}
	state := &sendState{accept: make(chan int64, 1), acks: make(chan struct{}, 1), cancel: make(chan error, 1), name: metadata.Name, size: metadata.Size}
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
	state.mu.Lock()
	state.acked = offset
	state.received = offset
	state.mu.Unlock()
	index := offset / int64(m.chunkSize)
	buffer := make([]byte, m.chunkSize)
	started := time.Now()
	sentThisRun := int64(0)
	for offset < info.Size() {
		state.mu.Lock()
		received := state.received
		state.mu.Unlock()
		if offset-received >= maxUnacknowledged {
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
		if err := m.sendBinary(ctx, packet); err != nil {
			return err
		}
		offset += int64(n)
		index++
		sentThisRun += int64(len(packet))
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
	select {
	case receiver.events <- receiveEvent{header: &header, data: payload}:
	case <-receiver.done:
	case <-m.closed:
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
			previous := sender.received
			if message.ReceivedBytes > sender.acked {
				sender.acked = message.ReceivedBytes
			}
			if message.ReceivedBytes > sender.received {
				sender.received = message.ReceivedBytes
			}
			confirmed, name, size := sender.received, sender.name, sender.size
			sender.mu.Unlock()
			if confirmed > previous {
				m.emit(TransferEvent{Type: "progress", TransferID: message.TransferID, Direction: "send", Name: name, Transferred: confirmed, Size: size})
			}
			select {
			case sender.acks <- struct{}{}:
			default:
			}
		}
	case "file_progress":
		m.mu.Lock()
		sender := m.senders[message.TransferID]
		m.mu.Unlock()
		if sender != nil {
			sender.mu.Lock()
			previous := sender.received
			if message.ReceivedBytes > sender.received {
				sender.received = message.ReceivedBytes
			}
			confirmed, name, size := sender.received, sender.name, sender.size
			sender.mu.Unlock()
			if confirmed > previous {
				m.emit(TransferEvent{Type: "progress", TransferID: message.TransferID, Direction: "send", Name: name, Transferred: confirmed, Size: size})
			}
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
			select {
			case receiver.events <- receiveEvent{end: &message}:
			case <-receiver.done:
			case <-m.closed:
			}
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
	state := &receiveState{metadata: metadata, path: target, partPath: partPath, metaPath: metaPath, file: file, events: make(chan receiveEvent, receiveEventQueueSize), done: make(chan struct{})}
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
	lastProgressAck := offset
	lastProgressAckAt := time.Now()
	pending := make(map[int64]receiveEvent)
	var pendingBytes int64
	var end *transferprotocol.Control
	var endTimer *time.Timer
	var endDeadline <-chan time.Time
	checkpoint := func() error {
		started := time.Now()
		if err := state.file.Sync(); err != nil {
			return err
		}
		duration := time.Since(started)
		lastDurable = expectedOffset
		if err := writePartialMetadata(state.metaPath, partialMetadata{File: state.metadata, ReceivedBytes: lastDurable}); err != nil {
			return err
		}
		m.emit(TransferEvent{Type: "checkpoint", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Transferred: lastDurable, Size: state.metadata.Size, Path: state.path, CheckpointDurationMS: duration.Milliseconds()})
		// The final acknowledgement is sent only after SHA-256 verification and
		// the partial file has been atomically promoted to its destination.
		if expectedOffset < state.metadata.Size || !state.metadata.CommitAck {
			_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_ack", TransferID: transferID, ReceivedBytes: lastDurable})
		}
		return nil
	}
	complete := func(message *transferprotocol.Control) bool {
		if message == nil || expectedOffset != state.metadata.Size || len(pending) != 0 {
			return false
		}
		digest := hex.EncodeToString(hash.Sum(nil))
		expectedHash := state.metadata.SHA256
		if expectedHash == "" {
			expectedHash = message.SHA256
		}
		if expectedHash != "" && !strings.EqualFold(expectedHash, digest) {
			m.receiveIntegrityFailed(transferID, state, errors.New("SHA-256 verification failed"))
			return true
		}
		if lastDurable != expectedOffset {
			if err := checkpoint(); err != nil {
				m.receiveFailed(transferID, state.metadata.Name, err)
				return true
			}
		}
		if err := state.file.Close(); err != nil {
			m.receiveFailed(transferID, state.metadata.Name, err)
			return true
		}
		if m.options.Overwrite {
			_ = os.Remove(state.path)
		}
		if err := os.Rename(state.partPath, state.path); err != nil {
			m.receiveFailed(transferID, state.metadata.Name, err)
			return true
		}
		_ = os.Remove(state.metaPath)
		_ = m.sendControl(transferprotocol.Control{Version: 1, Type: "file_ack", TransferID: transferID, ReceivedBytes: state.metadata.Size})
		m.finishReceiver(transferID)
		m.emit(TransferEvent{Type: "completed", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Transferred: state.metadata.Size, Size: state.metadata.Size, SHA256: digest, Path: state.path})
		return true
	}
	for {
		select {
		case event := <-state.events:
			if event.cancel != nil {
				m.finishReceiver(transferID)
				m.emit(TransferEvent{Type: "cancelled", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Message: event.cancel.Error()})
				return
			}
			if event.end != nil {
				end = event.end
				if complete(end) {
					return
				}
				if endTimer == nil {
					endTimer = time.NewTimer(m.options.ReceiveCompletionTimeout)
					defer endTimer.Stop()
					endDeadline = endTimer.C
				}
				continue
			}
			if event.header == nil {
				continue
			}
			header := event.header
			if header.TransferID != transferID || header.Index < 0 || header.Index >= state.metadata.TotalChunks || header.Length != len(event.data) {
				m.receiveFailed(transferID, state.metadata.Name, errors.New("file chunk index or length mismatch"))
				return
			}
			chunkOffset := header.Index * int64(state.metadata.ChunkSize)
			chunkLength := min(int64(state.metadata.ChunkSize), state.metadata.Size-chunkOffset)
			if header.Offset != chunkOffset || int64(header.Length) != chunkLength || chunkOffset < 0 || chunkOffset+chunkLength > state.metadata.Size {
				m.receiveFailed(transferID, state.metadata.Name, errors.New("file chunk offset or length mismatch"))
				return
			}
			if header.Index < expectedIndex {
				continue
			}
			if _, duplicate := pending[header.Index]; duplicate {
				m.receiveFailed(transferID, state.metadata.Name, errors.New("duplicate pending file chunk"))
				return
			}
			pending[header.Index] = event
			pendingBytes += int64(len(event.data))
			if pendingBytes > m.options.MaxReorderBufferBytes {
				m.receiveFailed(transferID, state.metadata.Name, errors.New("file chunk reorder buffer exceeded"))
				return
			}
			for {
				next, ok := pending[expectedIndex]
				if !ok {
					break
				}
				delete(pending, expectedIndex)
				pendingBytes -= int64(len(next.data))
				if next.header.Offset != expectedOffset {
					m.receiveFailed(transferID, state.metadata.Name, errors.New("file chunk continuity mismatch"))
					return
				}
				if _, err := state.file.Write(next.data); err != nil {
					m.receiveFailed(transferID, state.metadata.Name, err)
					return
				}
				_, _ = hash.Write(next.data)
				expectedOffset += int64(len(next.data))
				expectedIndex++
				if expectedOffset-lastDurable >= m.options.DurableAckInterval || expectedOffset == state.metadata.Size {
					if err := checkpoint(); err != nil {
						m.receiveFailed(transferID, state.metadata.Name, err)
						return
					}
				}
				m.emit(TransferEvent{Type: "progress", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Transferred: expectedOffset, Size: state.metadata.Size, Path: state.path})
				now := time.Now()
				if expectedOffset < state.metadata.Size && (expectedOffset-lastProgressAck >= progressAckInterval || now.Sub(lastProgressAckAt) >= progressAckPeriod) {
					lastProgressAck = expectedOffset
					lastProgressAckAt = now
					messageType := "file_ack"
					if state.metadata.ProgressAck {
						messageType = "file_progress"
					}
					_ = m.sendControl(transferprotocol.Control{Version: 1, Type: messageType, TransferID: transferID, ReceivedBytes: expectedOffset})
				}
			}
			if complete(end) {
				return
			}
		case <-endDeadline:
			m.receiveFailed(transferID, state.metadata.Name, errors.New("timed out waiting for missing file chunks"))
			return
		case <-state.done:
			return
		case <-m.closed:
			m.finishReceiver(transferID)
			m.emit(TransferEvent{Type: "failed", TransferID: transferID, Direction: "receive", Name: state.metadata.Name, Message: "DataChannel closed during transfer"})
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
		state.doneOnce.Do(func() { close(state.done) })
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

func (m *TransferManager) sendBinary(ctx context.Context, packet []byte) error {
	started := time.Now()
	for {
		if err := m.waitForBuffer(ctx, len(packet)); err != nil {
			return err
		}
		if err := m.channel.Send(packet); err == nil {
			return nil
		} else if m.channel.ReadyState() != webrtc.DataChannelStateOpen {
			return err
		} else if time.Since(started) >= 30*time.Second {
			return fmt.Errorf("DataChannel send queue remained unavailable: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.closed:
			return errors.New("DataChannel is closed")
		case <-m.writable:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (m *TransferManager) waitForBuffer(ctx context.Context, packetSize int) error {
	required := uint64(packetSize)
	if required > maxBufferedAmount {
		return errors.New("DataChannel packet exceeds send queue high water mark")
	}
	for m.channel.BufferedAmount()+required > maxBufferedAmount {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.closed:
			return errors.New("DataChannel is closed")
		case <-m.writable:
		case <-time.After(100 * time.Millisecond):
		}
		if m.channel.ReadyState() != webrtc.DataChannelStateOpen {
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
