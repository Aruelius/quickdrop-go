package protocol

import (
	"bytes"
	"testing"
)

func TestChunkRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5a}, 4096)
	header := ChunkHeader{TransferID: "transfer-123", Index: 2, Offset: 8192, Length: len(payload)}
	packet, err := EncodeChunk(header, payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, data, err := DecodeChunk(packet)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != header || !bytes.Equal(data, payload) {
		t.Fatalf("round trip mismatch: %#v", decoded)
	}
}

func TestChunkRejectsLengthMismatch(t *testing.T) {
	_, err := EncodeChunk(ChunkHeader{TransferID: "transfer-123", Length: 2}, []byte{1})
	if err == nil {
		t.Fatal("expected length mismatch to fail")
	}
}

func TestTotalChunks(t *testing.T) {
	if got := TotalChunks(0, 10); got != 0 {
		t.Fatalf("zero file: %d", got)
	}
	if got := TotalChunks(21, 10); got != 3 {
		t.Fatalf("got %d", got)
	}
}
