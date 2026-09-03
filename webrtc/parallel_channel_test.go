package peer

import (
	"bytes"
	"sync"
	"testing"
	"time"

	pion "github.com/pion/webrtc/v4"
)

func TestParallelChannelsNegotiateAndCarryBinaryMessages(t *testing.T) {
	leftAPI := testWebRTCAPI()
	rightAPI := testWebRTCAPI()
	leftPC, leftPrimary, leftPriority, rightPC, rightPrimary, rightPriority := connectTestDataChannels(t, leftAPI, rightAPI)
	defer leftPC.Close()
	defer rightPC.Close()

	left := newParallelChannel(leftPrimary, leftPriority, parallelChannelOptions{
		initiator: true, connections: 3,
		newPeerConnection: func() (*pion.PeerConnection, error) { return leftAPI.NewPeerConnection(pion.Configuration{}) },
	})
	right := newParallelChannel(rightPrimary, rightPriority, parallelChannelOptions{
		initiator: false, connections: 3,
		newPeerConnection: func() (*pion.PeerConnection, error) { return rightAPI.NewPeerConnection(pion.Configuration{}) },
	})
	defer left.Close()
	defer right.Close()

	received := make(chan []byte, 32)
	right.OnMessage(func(message pion.DataChannelMessage) {
		if !message.IsString {
			received <- append([]byte(nil), message.Data...)
		}
	})
	left.start()
	right.start()
	waitForParallelConnections(t, left, 3)
	waitForParallelConnections(t, right, 3)

	for index := byte(0); index < 12; index++ {
		payload := bytes.Repeat([]byte{index}, 1024)
		if err := left.Send(payload); err != nil {
			t.Fatal(err)
		}
	}
	for index := byte(0); index < 12; index++ {
		select {
		case payload := <-received:
			if len(payload) != 1024 {
				t.Fatalf("payload length=%d", len(payload))
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for parallel payload")
		}
	}
	stripedLanes := 0
	for _, connection := range left.secondaryPeerConnections() {
		for _, report := range connection.GetStats() {
			stats, ok := report.(pion.DataChannelStats)
			if ok && stats.Label != "" && stats.Label != "quickdrop-data" && stats.MessagesSent > 0 {
				stripedLanes++
			}
		}
	}
	if stripedLanes != 2 {
		t.Fatalf("binary traffic used %d secondary lanes, expected 2", stripedLanes)
	}
}

func TestParallelChannelKeepsSingleConnectionWithoutRemoteCapability(t *testing.T) {
	api := testWebRTCAPI()
	leftPC, leftPrimary, leftPriority, rightPC, _, _ := connectTestDataChannels(t, api, api)
	defer leftPC.Close()
	defer rightPC.Close()
	channel := newParallelChannel(leftPrimary, leftPriority, parallelChannelOptions{
		initiator: true, connections: 4,
		newPeerConnection: func() (*pion.PeerConnection, error) { return api.NewPeerConnection(pion.Configuration{}) },
	})
	defer channel.Close()
	channel.start()
	time.Sleep(50 * time.Millisecond)
	if count := channel.ConnectionCount(); count != 1 {
		t.Fatalf("connections=%d", count)
	}
}

func testWebRTCAPI() *pion.API {
	setting := pion.SettingEngine{}
	setting.SetIncludeLoopbackCandidate(true)
	setting.SetSCTPMaxReceiveBufferSize(4 * 1024 * 1024)
	return pion.NewAPI(pion.WithSettingEngine(setting))
}

func connectTestDataChannels(t *testing.T, leftAPI, rightAPI *pion.API) (*pion.PeerConnection, *pion.DataChannel, *pion.DataChannel, *pion.PeerConnection, *pion.DataChannel, *pion.DataChannel) {
	t.Helper()
	leftPC, err := leftAPI.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	rightPC, err := rightAPI.NewPeerConnection(pion.Configuration{})
	if err != nil {
		_ = leftPC.Close()
		t.Fatal(err)
	}
	negotiated, ordered, priorityID := true, true, uint16(1023)
	leftPriority, err := leftPC.CreateDataChannel("quickdrop-priority", &pion.DataChannelInit{Negotiated: &negotiated, Ordered: &ordered, ID: &priorityID})
	if err != nil {
		t.Fatal(err)
	}
	rightPriority, err := rightPC.CreateDataChannel("quickdrop-priority", &pion.DataChannelInit{Negotiated: &negotiated, Ordered: &ordered, ID: &priorityID})
	if err != nil {
		t.Fatal(err)
	}
	rightPrimaryReady := make(chan *pion.DataChannel, 1)
	rightPC.OnDataChannel(func(channel *pion.DataChannel) {
		if channel.Label() == "quickdrop-data" {
			rightPrimaryReady <- channel
		}
	})
	leftPrimary, err := leftPC.CreateDataChannel("quickdrop-data", &pion.DataChannelInit{Ordered: &ordered})
	if err != nil {
		t.Fatal(err)
	}
	offer, err := leftPC.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := leftPC.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-pion.GatheringCompletePromise(leftPC)
	if err := rightPC.SetRemoteDescription(*leftPC.LocalDescription()); err != nil {
		t.Fatal(err)
	}
	answer, err := rightPC.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rightPC.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	<-pion.GatheringCompletePromise(rightPC)
	if err := leftPC.SetRemoteDescription(*rightPC.LocalDescription()); err != nil {
		t.Fatal(err)
	}
	var rightPrimary *pion.DataChannel
	select {
	case rightPrimary = <-rightPrimaryReady:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for remote data channel")
	}
	var opened sync.WaitGroup
	opened.Add(4)
	for _, channel := range []*pion.DataChannel{leftPrimary, rightPrimary, leftPriority, rightPriority} {
		channel.OnOpen(opened.Done)
	}
	done := make(chan struct{})
	go func() { opened.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for data channels to open")
	}
	return leftPC, leftPrimary, leftPriority, rightPC, rightPrimary, rightPriority
}

func waitForParallelConnections(t *testing.T, channel *parallelChannel, expected int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if channel.ConnectionCount() == expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("connections=%d, expected=%d", channel.ConnectionCount(), expected)
}
