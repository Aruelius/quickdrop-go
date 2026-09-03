package peer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	pion "github.com/pion/webrtc/v4"
)

const (
	defaultParallelConnections        = 4
	maxParallelConnections            = 8
	maxParallelSDPSize                = 128 * 1024
	maxParallelCandidateSize          = 4096
	maxPendingCandidatesPerConnection = 64
)

type parallelControl struct {
	Version     int                      `json:"version"`
	Type        string                   `json:"type"`
	Connections int                      `json:"connections,omitempty"`
	Lane        int                      `json:"lane,omitempty"`
	Description *pion.SessionDescription `json:"description,omitempty"`
	Candidate   *pion.ICECandidateInit   `json:"candidate,omitempty"`
}

type parallelLane struct {
	pc      *pion.PeerConnection
	channel *pion.DataChannel
	open    bool
}

type parallelChannelOptions struct {
	initiator         bool
	connections       int
	newPeerConnection func() (*pion.PeerConnection, error)
}

// parallelChannel preserves the protocol/control ordering of the primary
// channel while striping binary file chunks across independent SCTP
// associations. Old peers ignore the capability message and remain on the
// primary channel.
type parallelChannel struct {
	primary  *pion.DataChannel
	priority *pion.DataChannel
	options  parallelChannelOptions

	mu                    sync.Mutex
	lanes                 map[int]*parallelLane
	pendingCandidates     map[int][]pion.ICECandidateInit
	negotiatedConnections int
	peerSupportsPriority  bool
	binaryCursor          uint64
	onMessage             func(pion.DataChannelMessage)
	onClose               func()
	onError               func(error)
	onBufferedAmountLow   func()
	bufferedLowThreshold  uint64
	pendingMessages       []pion.DataChannelMessage
	closed                bool
	closeOnce             sync.Once
	sendMu                sync.Mutex
	negotiation           chan parallelControl
	negotiationDone       chan struct{}
}

func newParallelChannel(primary, priority *pion.DataChannel, options parallelChannelOptions) *parallelChannel {
	options.connections = clampParallelConnections(options.connections)
	channel := &parallelChannel{
		primary: primary, priority: priority, options: options,
		lanes: make(map[int]*parallelLane), pendingCandidates: make(map[int][]pion.ICECandidateInit),
		negotiatedConnections: 1, negotiation: make(chan parallelControl, 256), negotiationDone: make(chan struct{}),
	}
	primary.OnMessage(channel.handlePrimaryMessage)
	primary.OnClose(channel.handlePrimaryClose)
	primary.OnError(channel.emitError)
	primary.OnBufferedAmountLow(channel.emitBufferedAmountLow)
	if priority != nil {
		priority.OnMessage(channel.handlePriorityMessage)
		priority.OnError(func(error) {})
	}
	go channel.negotiationLoop()
	return channel
}

func (c *parallelChannel) start() {
	c.sendRawText(`{"version":1,"type":"channel_capability","priorityCancel":true}`)
	c.sendParallel(parallelControl{Version: 1, Type: "parallel_capability", Connections: c.options.connections})
}

func (c *parallelChannel) Send(payload []byte) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	candidates := c.binaryCandidates()
	if len(candidates) == 0 {
		return errors.New("DataChannel is not open")
	}
	start := int(c.binaryCursor % uint64(len(candidates)))
	candidates = append(append(make([]*pion.DataChannel, 0, len(candidates)), candidates[start:]...), candidates[:start]...)
	c.binaryCursor++
	var lastErr error
	for len(candidates) > 0 {
		least := 0
		for index := 1; index < len(candidates); index++ {
			if candidates[index].BufferedAmount() < candidates[least].BufferedAmount() {
				least = index
			}
		}
		candidate := candidates[least]
		if err := candidate.Send(payload); err == nil {
			return nil
		} else {
			lastErr = err
		}
		candidates = append(candidates[:least], candidates[least+1:]...)
	}
	return lastErr
}

func (c *parallelChannel) SendText(payload string) error {
	if isPriorityCancel(payload) {
		c.mu.Lock()
		usePriority := c.peerSupportsPriority && c.priority != nil && c.priority.ReadyState() == pion.DataChannelStateOpen
		priority := c.priority
		c.mu.Unlock()
		if usePriority {
			return priority.SendText(payload)
		}
	}
	return c.sendRawText(payload)
}

func (c *parallelChannel) sendRawText(payload string) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.primary.ReadyState() != pion.DataChannelStateOpen {
		return errors.New("DataChannel is not open")
	}
	return c.primary.SendText(payload)
}

func (c *parallelChannel) BufferedAmount() uint64 {
	candidates := c.binaryCandidates()
	if len(candidates) == 0 {
		return c.primary.BufferedAmount()
	}
	minimum := candidates[0].BufferedAmount()
	for _, candidate := range candidates[1:] {
		if amount := candidate.BufferedAmount(); amount < minimum {
			minimum = amount
		}
	}
	return minimum
}

func (c *parallelChannel) SetBufferedAmountLowThreshold(threshold uint64) {
	c.mu.Lock()
	c.bufferedLowThreshold = threshold
	lanes := make([]*parallelLane, 0, len(c.lanes))
	for _, lane := range c.lanes {
		lanes = append(lanes, lane)
	}
	c.mu.Unlock()
	c.primary.SetBufferedAmountLowThreshold(threshold)
	for _, lane := range lanes {
		if lane.channel != nil {
			lane.channel.SetBufferedAmountLowThreshold(threshold)
		}
	}
}

func (c *parallelChannel) ReadyState() pion.DataChannelState { return c.primary.ReadyState() }

func (c *parallelChannel) OnMessage(handler func(pion.DataChannelMessage)) {
	c.mu.Lock()
	c.onMessage = handler
	pending := c.pendingMessages
	c.pendingMessages = nil
	c.mu.Unlock()
	for _, message := range pending {
		handler(message)
	}
}

func (c *parallelChannel) OnBufferedAmountLow(handler func()) {
	c.mu.Lock()
	c.onBufferedAmountLow = handler
	c.mu.Unlock()
}

func (c *parallelChannel) OnClose(handler func()) {
	c.mu.Lock()
	c.onClose = handler
	closed := c.closed
	c.mu.Unlock()
	if closed && handler != nil {
		go handler()
	}
}

func (c *parallelChannel) OnError(handler func(error)) {
	c.mu.Lock()
	c.onError = handler
	c.mu.Unlock()
}

func (c *parallelChannel) Close() error {
	return c.shutdown(true)
}

func (c *parallelChannel) shutdown(closePrimary bool) error {
	var result error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		lanes := make([]*parallelLane, 0, len(c.lanes))
		for _, lane := range c.lanes {
			lanes = append(lanes, lane)
		}
		c.lanes = make(map[int]*parallelLane)
		onClose := c.onClose
		c.mu.Unlock()
		for _, lane := range lanes {
			_ = lane.pc.Close()
		}
		if c.priority != nil {
			_ = c.priority.Close()
		}
		if closePrimary {
			result = c.primary.Close()
		}
		close(c.negotiationDone)
		if onClose != nil {
			onClose()
		}
	})
	return result
}

func (c *parallelChannel) ConnectionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 1
	for _, lane := range c.lanes {
		if lane.open && lane.channel.ReadyState() == pion.DataChannelStateOpen {
			count++
		}
	}
	return count
}

func (c *parallelChannel) secondaryPeerConnections() []*pion.PeerConnection {
	c.mu.Lock()
	defer c.mu.Unlock()
	connections := make([]*pion.PeerConnection, 0, len(c.lanes))
	for _, lane := range c.lanes {
		if lane.open && lane.pc.ConnectionState() != pion.PeerConnectionStateClosed {
			connections = append(connections, lane.pc)
		}
	}
	return connections
}

func (c *parallelChannel) handlePrimaryMessage(message pion.DataChannelMessage) {
	if message.IsString {
		var value struct {
			Version        int    `json:"version"`
			Type           string `json:"type"`
			PriorityCancel bool   `json:"priorityCancel"`
		}
		if json.Unmarshal(message.Data, &value) == nil && value.Version == 1 {
			if value.Type == "channel_capability" {
				c.mu.Lock()
				c.peerSupportsPriority = value.PriorityCancel
				c.mu.Unlock()
				return
			}
			if strings.HasPrefix(value.Type, "parallel_") {
				var control parallelControl
				if json.Unmarshal(message.Data, &control) == nil {
					c.enqueueNegotiation(control)
				}
				return
			}
		}
	}
	c.deliver(message)
}

func (c *parallelChannel) handlePriorityMessage(message pion.DataChannelMessage) {
	if message.IsString {
		c.deliver(message)
	}
}

func (c *parallelChannel) deliver(message pion.DataChannelMessage) {
	c.mu.Lock()
	handler := c.onMessage
	if handler == nil {
		if len(c.pendingMessages) < 64 {
			c.pendingMessages = append(c.pendingMessages, message)
		}
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	handler(message)
}

func (c *parallelChannel) enqueueNegotiation(control parallelControl) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return
	}
	select {
	case c.negotiation <- control:
	case <-c.negotiationDone:
	default:
		c.emitError(errors.New("parallel negotiation queue is full"))
	}
}

func (c *parallelChannel) negotiationLoop() {
	for {
		select {
		case control := <-c.negotiation:
			if err := c.handleParallelControl(control); err != nil {
				c.emitError(fmt.Errorf("parallel connection negotiation: %w", err))
			}
		case <-c.negotiationDone:
			return
		}
	}
}

func (c *parallelChannel) handleParallelControl(control parallelControl) error {
	if control.Version != 1 {
		return nil
	}
	switch control.Type {
	case "parallel_capability":
		if control.Connections < 1 || control.Connections > maxParallelConnections {
			return nil
		}
		c.mu.Lock()
		c.negotiatedConnections = min(c.options.connections, control.Connections)
		connections := c.negotiatedConnections
		c.mu.Unlock()
		if c.options.initiator {
			for lane := 1; lane < connections; lane++ {
				if err := c.createOffer(lane); err != nil {
					c.removeLane(lane)
				}
			}
		}
	case "parallel_offer":
		if !c.options.initiator {
			return c.acceptOffer(control)
		}
	case "parallel_answer":
		if c.options.initiator {
			return c.acceptAnswer(control)
		}
	case "parallel_ice":
		return c.acceptCandidate(control)
	}
	return nil
}

func (c *parallelChannel) createOffer(index int) error {
	if !c.validLane(index) || c.options.newPeerConnection == nil {
		return nil
	}
	pc, err := c.options.newPeerConnection()
	if err != nil {
		return err
	}
	lane := &parallelLane{pc: pc}
	if !c.installLane(index, lane) {
		_ = pc.Close()
		return nil
	}
	c.configurePeerConnection(index, lane)
	ordered := false
	data, err := pc.CreateDataChannel(fmt.Sprintf("quickdrop-lane-%d", index), &pion.DataChannelInit{Ordered: &ordered})
	if err != nil {
		return err
	}
	c.attachLaneChannel(index, lane, data)
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	description := pc.LocalDescription()
	return c.sendParallel(parallelControl{Version: 1, Type: "parallel_offer", Lane: index, Description: description})
}

func (c *parallelChannel) acceptOffer(control parallelControl) error {
	if !c.validDescription(control, pion.SDPTypeOffer) || c.options.newPeerConnection == nil {
		return nil
	}
	pc, err := c.options.newPeerConnection()
	if err != nil {
		return err
	}
	lane := &parallelLane{pc: pc}
	if !c.installLane(control.Lane, lane) {
		_ = pc.Close()
		return nil
	}
	c.configurePeerConnection(control.Lane, lane)
	if err := pc.SetRemoteDescription(*control.Description); err != nil {
		return err
	}
	if err := c.flushLaneCandidates(control.Lane, pc); err != nil {
		return err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return err
	}
	return c.sendParallel(parallelControl{Version: 1, Type: "parallel_answer", Lane: control.Lane, Description: pc.LocalDescription()})
}

func (c *parallelChannel) acceptAnswer(control parallelControl) error {
	if !c.validDescription(control, pion.SDPTypeAnswer) {
		return nil
	}
	c.mu.Lock()
	lane := c.lanes[control.Lane]
	c.mu.Unlock()
	if lane == nil {
		return nil
	}
	if err := lane.pc.SetRemoteDescription(*control.Description); err != nil {
		return err
	}
	return c.flushLaneCandidates(control.Lane, lane.pc)
}

func (c *parallelChannel) acceptCandidate(control parallelControl) error {
	if control.Candidate == nil || !c.validLane(control.Lane) || len(control.Candidate.Candidate) > maxParallelCandidateSize {
		return nil
	}
	c.mu.Lock()
	lane := c.lanes[control.Lane]
	if lane == nil || lane.pc.RemoteDescription() == nil {
		pending := c.pendingCandidates[control.Lane]
		if len(pending) < maxPendingCandidatesPerConnection {
			c.pendingCandidates[control.Lane] = append(pending, *control.Candidate)
		}
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	return lane.pc.AddICECandidate(*control.Candidate)
}

func (c *parallelChannel) configurePeerConnection(index int, lane *parallelLane) {
	lane.pc.OnICECandidate(func(candidate *pion.ICECandidate) {
		if candidate == nil || !c.isCurrentLane(index, lane) {
			return
		}
		value := candidate.ToJSON()
		_ = c.sendParallel(parallelControl{Version: 1, Type: "parallel_ice", Lane: index, Candidate: &value})
	})
	lane.pc.OnDataChannel(func(data *pion.DataChannel) {
		if c.isCurrentLane(index, lane) && data.Label() == fmt.Sprintf("quickdrop-lane-%d", index) {
			c.attachLaneChannel(index, lane, data)
		} else {
			_ = data.Close()
		}
	})
	lane.pc.OnConnectionStateChange(func(state pion.PeerConnectionState) {
		if state == pion.PeerConnectionStateFailed || state == pion.PeerConnectionStateClosed {
			c.removeLaneIfCurrent(index, lane)
		}
	})
}

func (c *parallelChannel) attachLaneChannel(index int, lane *parallelLane, data *pion.DataChannel) {
	c.mu.Lock()
	if c.lanes[index] != lane {
		c.mu.Unlock()
		_ = data.Close()
		return
	}
	lane.channel = data
	threshold := c.bufferedLowThreshold
	c.mu.Unlock()
	data.SetBufferedAmountLowThreshold(threshold)
	data.OnBufferedAmountLow(c.emitBufferedAmountLow)
	data.OnMessage(func(message pion.DataChannelMessage) {
		if message.IsString {
			c.emitError(errors.New("parallel file lane received a text message"))
			return
		}
		c.deliver(message)
	})
	data.OnError(func(error) { c.removeLaneIfCurrent(index, lane) })
	data.OnClose(func() { c.removeLaneIfCurrent(index, lane) })
	data.OnOpen(func() {
		c.mu.Lock()
		if c.lanes[index] == lane {
			lane.open = true
		}
		c.mu.Unlock()
	})
}

func (c *parallelChannel) installLane(index int, lane *parallelLane) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.lanes[index] != nil {
		return false
	}
	c.lanes[index] = lane
	return true
}

func (c *parallelChannel) removeLane(index int) {
	c.mu.Lock()
	lane := c.lanes[index]
	delete(c.lanes, index)
	delete(c.pendingCandidates, index)
	c.mu.Unlock()
	if lane != nil {
		_ = lane.pc.Close()
	}
}

func (c *parallelChannel) removeLaneIfCurrent(index int, lane *parallelLane) {
	c.mu.Lock()
	if c.lanes[index] != lane {
		c.mu.Unlock()
		return
	}
	delete(c.lanes, index)
	delete(c.pendingCandidates, index)
	c.mu.Unlock()
	if lane.pc.ConnectionState() != pion.PeerConnectionStateClosed {
		_ = lane.pc.Close()
	}
}

func (c *parallelChannel) flushLaneCandidates(index int, pc *pion.PeerConnection) error {
	c.mu.Lock()
	pending := append([]pion.ICECandidateInit(nil), c.pendingCandidates[index]...)
	delete(c.pendingCandidates, index)
	c.mu.Unlock()
	for _, candidate := range pending {
		if err := pc.AddICECandidate(candidate); err != nil {
			return err
		}
	}
	return nil
}

func (c *parallelChannel) binaryCandidates() []*pion.DataChannel {
	c.mu.Lock()
	defer c.mu.Unlock()
	candidates := make([]*pion.DataChannel, 0, 1+len(c.lanes))
	if c.primary.ReadyState() == pion.DataChannelStateOpen {
		candidates = append(candidates, c.primary)
	}
	for _, lane := range c.lanes {
		if lane.open && lane.channel != nil && lane.channel.ReadyState() == pion.DataChannelStateOpen {
			candidates = append(candidates, lane.channel)
		}
	}
	return candidates
}

func (c *parallelChannel) validLane(index int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return index > 0 && index < c.negotiatedConnections
}

func (c *parallelChannel) validDescription(control parallelControl, kind pion.SDPType) bool {
	return c.validLane(control.Lane) && control.Description != nil && control.Description.Type == kind && len(control.Description.SDP) <= maxParallelSDPSize
}

func (c *parallelChannel) isCurrentLane(index int, lane *parallelLane) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.lanes[index] == lane
}

func (c *parallelChannel) sendParallel(control parallelControl) error {
	encoded, err := json.Marshal(control)
	if err != nil {
		return err
	}
	return c.sendRawText(string(encoded))
}

func (c *parallelChannel) handlePrimaryClose() {
	_ = c.shutdown(false)
}

func (c *parallelChannel) emitError(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	handler := c.onError
	c.mu.Unlock()
	if handler != nil {
		handler(err)
	}
}

func (c *parallelChannel) emitBufferedAmountLow() {
	c.mu.Lock()
	handler := c.onBufferedAmountLow
	c.mu.Unlock()
	if handler != nil {
		handler()
	}
}

func clampParallelConnections(value int) int {
	if value <= 0 {
		return defaultParallelConnections
	}
	return min(value, maxParallelConnections)
}

func isPriorityCancel(payload string) bool {
	var value struct {
		Type string `json:"type"`
	}
	return json.Unmarshal([]byte(payload), &value) == nil && value.Type == "file_cancel"
}
