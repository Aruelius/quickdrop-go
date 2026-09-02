// Package protocol implements the language-neutral QuickDrop DataChannel
// framing shared by the browser and native clients.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	Version             = 1
	PacketTypeFileChunk = 1
	FixedHeaderSize     = 6
	MaxHeaderSize       = 64 * 1024
	DefaultChunkSize    = 12 * 1024
	TargetChunkSize     = 64 * 1024
)

var ErrInvalidPacket = errors.New("invalid QuickDrop binary packet")

type FileMetadata struct {
	Name         string `json:"name"`
	Size         int64  `json:"size"`
	MIME         string `json:"mime"`
	ChunkSize    int    `json:"chunkSize"`
	TotalChunks  int64  `json:"totalChunks"`
	LastModified int64  `json:"lastModified"`
	SHA256       string `json:"sha256,omitempty"`
	CommitAck    bool   `json:"commitAck,omitempty"`
	Resume       bool   `json:"resume,omitempty"`
}

type ChunkHeader struct {
	TransferID string `json:"transferId"`
	Index      int64  `json:"index"`
	Offset     int64  `json:"offset"`
	Length     int    `json:"length"`
}

type Control struct {
	Version          int             `json:"version"`
	Type             string          `json:"type"`
	ID               string          `json:"id,omitempty"`
	Timestamp        int64           `json:"timestamp,omitempty"`
	Payload          json.RawMessage `json:"payload,omitempty"`
	TransferID       string          `json:"transferId,omitempty"`
	File             *FileMetadata   `json:"file,omitempty"`
	Acknowledgements bool            `json:"acknowledgements,omitempty"`
	ReceivedBytes    int64           `json:"receivedBytes,omitempty"`
	Reason           string          `json:"reason,omitempty"`
	SHA256           string          `json:"sha256,omitempty"`
}

func EncodeChunk(header ChunkHeader, payload []byte) ([]byte, error) {
	if header.TransferID == "" || header.Index < 0 || header.Offset < 0 || header.Length != len(payload) {
		return nil, fmt.Errorf("%w: invalid chunk header", ErrInvalidPacket)
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode chunk header: %w", err)
	}
	if len(headerJSON) == 0 || len(headerJSON) > MaxHeaderSize {
		return nil, fmt.Errorf("%w: header size", ErrInvalidPacket)
	}
	packet := make([]byte, FixedHeaderSize+len(headerJSON)+len(payload))
	packet[0] = Version
	packet[1] = PacketTypeFileChunk
	binary.BigEndian.PutUint32(packet[2:6], uint32(len(headerJSON)))
	copy(packet[FixedHeaderSize:], headerJSON)
	copy(packet[FixedHeaderSize+len(headerJSON):], payload)
	return packet, nil
}

func DecodeChunk(packet []byte) (ChunkHeader, []byte, error) {
	if len(packet) < FixedHeaderSize || packet[0] != Version || packet[1] != PacketTypeFileChunk {
		return ChunkHeader{}, nil, ErrInvalidPacket
	}
	headerLength := int(binary.BigEndian.Uint32(packet[2:6]))
	if headerLength <= 0 || headerLength > MaxHeaderSize || FixedHeaderSize+headerLength > len(packet) {
		return ChunkHeader{}, nil, ErrInvalidPacket
	}
	var header ChunkHeader
	if err := json.Unmarshal(packet[FixedHeaderSize:FixedHeaderSize+headerLength], &header); err != nil {
		return ChunkHeader{}, nil, fmt.Errorf("%w: %v", ErrInvalidPacket, err)
	}
	payload := packet[FixedHeaderSize+headerLength:]
	if header.TransferID == "" || header.Index < 0 || header.Offset < 0 || header.Length != len(payload) {
		return ChunkHeader{}, nil, ErrInvalidPacket
	}
	return header, payload, nil
}

func EncodeControl(value any) ([]byte, error) {
	return json.Marshal(value)
}

func DecodeControl(raw []byte) (Control, error) {
	var message Control
	if err := json.Unmarshal(raw, &message); err != nil {
		return Control{}, err
	}
	if message.Version != Version || message.Type == "" {
		return Control{}, errors.New("unsupported QuickDrop control message")
	}
	return message, nil
}

func TotalChunks(size int64, chunkSize int) int64 {
	if size <= 0 || chunkSize <= 0 {
		return 0
	}
	return (size + int64(chunkSize) - 1) / int64(chunkSize)
}
