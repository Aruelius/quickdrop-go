// Package channel defines the transport surface used by QuickDrop transfers.
// A pion DataChannel implements Channel directly; newer peers may expose a
// logical channel backed by several independent WebRTC connections.
package channel

import "github.com/pion/webrtc/v4"

type Channel interface {
	Send([]byte) error
	SendText(string) error
	BufferedAmount() uint64
	SetBufferedAmountLowThreshold(uint64)
	ReadyState() webrtc.DataChannelState
	OnMessage(func(webrtc.DataChannelMessage))
	OnBufferedAmountLow(func())
	OnClose(func())
	OnError(func(error))
	Close() error
}

type ConnectionCounter interface {
	ConnectionCount() int
}
