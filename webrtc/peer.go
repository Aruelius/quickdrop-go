// Package peer establishes direct-first WebRTC DataChannels for QuickDrop.
package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"github.com/Aruelius/quickdrop-go/api"
	quickdropchannel "github.com/Aruelius/quickdrop-go/channel"
	"github.com/Aruelius/quickdrop-go/signaling"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

type PeerOptions struct {
	UDPPort               int
	DirectTimeout         time.Duration
	AllowRelay            bool
	RelayProvider         string
	ParallelConnections   int
	SCTPReceiveBufferSize uint32
	UDPBufferSize         int
}

type PeerResult struct {
	Channel         *webrtc.DataChannel
	Transport       quickdropchannel.Channel
	Policy          api.ICEPolicy
	Mode            string
	LocalCandidate  string
	RemoteCandidate string
	MaxMessageSize  uint32
}

type ConnectionStats struct {
	Connections            int     `json:"connections"`
	CurrentRTTMilliseconds float64 `json:"currentRttMs,omitempty"`
	BytesSent              uint64  `json:"bytesSent"`
	BytesReceived          uint64  `json:"bytesReceived"`
	PacketsDiscardedOnSend uint64  `json:"packetsDiscardedOnSend"`
}

type Peer struct {
	client    *api.Client
	creds     api.Credentials
	initiator bool
	options   PeerOptions
	signaling *signaling.Signaling
	udpMux    *ice.MultiUDPMuxDefault
	transport *parallelChannel

	mu                 sync.Mutex
	pc                 *webrtc.PeerConnection
	pendingCandidates  []webrtc.ICECandidateInit
	policy             api.ICEPolicy
	relayStarted       bool
	closed             bool
	result             chan PeerResult
	errors             chan error
	negotiationStarted chan struct{}
	resultOnce         sync.Once
	errorOnce          sync.Once
	startOnce          sync.Once
}

func New(client *api.Client, credentials api.Credentials, initiator bool, options PeerOptions) *Peer {
	if options.DirectTimeout <= 0 {
		options.DirectTimeout = 18 * time.Second
	}
	if options.RelayProvider == "" {
		options.RelayProvider = "platform"
	}
	options.ParallelConnections = clampParallelConnections(options.ParallelConnections)
	if options.SCTPReceiveBufferSize == 0 {
		options.SCTPReceiveBufferSize = 16 * 1024 * 1024
	}
	if options.UDPBufferSize <= 0 {
		options.UDPBufferSize = 8 * 1024 * 1024
	}
	return &Peer{client: client, creds: credentials, initiator: initiator, options: options, result: make(chan PeerResult, 1), errors: make(chan error, 1), negotiationStarted: make(chan struct{})}
}

func (p *Peer) Connect(ctx context.Context) (PeerResult, error) {
	direct, err := p.client.ICE(ctx, p.creds, "direct", "")
	if err != nil {
		return PeerResult{}, err
	}
	if p.options.UDPPort > 0 {
		p.udpMux, err = ice.NewMultiUDPMuxFromPort(
			p.options.UDPPort,
			ice.UDPMuxFromPortWithReadBufferSize(p.options.UDPBufferSize),
			ice.UDPMuxFromPortWithWriteBufferSize(p.options.UDPBufferSize),
		)
		if err != nil {
			return PeerResult{}, fmt.Errorf("listen UDP port %d: %w", p.options.UDPPort, err)
		}
	}
	if err := p.replaceConnection(direct, p.initiator); err != nil {
		p.closeMux()
		return PeerResult{}, err
	}
	p.signaling, err = signaling.Dial(ctx, p.client, p.creds)
	if err != nil {
		p.Close()
		return PeerResult{}, fmt.Errorf("connect signaling: %w", err)
	}
	go p.signalLoop(ctx)
	if p.initiator {
		select {
		case result := <-p.result:
			return result, nil
		case err := <-p.errors:
			return PeerResult{}, err
		case <-p.negotiationStarted:
		case <-ctx.Done():
			return PeerResult{}, ctx.Err()
		}
		timer := time.NewTimer(p.options.DirectTimeout)
		defer timer.Stop()
		select {
		case result := <-p.result:
			return result, nil
		case err := <-p.errors:
			return PeerResult{}, err
		case <-timer.C:
			if p.options.AllowRelay {
				if err := p.beginRelay(); err != nil {
					p.fail(err)
				}
				select {
				case result := <-p.result:
					return result, nil
				case err := <-p.errors:
					return PeerResult{}, err
				case <-ctx.Done():
					return PeerResult{}, ctx.Err()
				case <-time.After(30 * time.Second):
					return PeerResult{}, errors.New("TURN connection timed out")
				}
			}
			return PeerResult{}, errors.New("Direct P2P connection timed out")
		case <-ctx.Done():
			return PeerResult{}, ctx.Err()
		}
	}
	select {
	case result := <-p.result:
		return result, nil
	case err := <-p.errors:
		return PeerResult{}, err
	case <-ctx.Done():
		return PeerResult{}, ctx.Err()
	}
}

func (p *Peer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	pc, signaling, transport := p.pc, p.signaling, p.transport
	p.pc, p.signaling, p.transport = nil, nil, nil
	p.mu.Unlock()
	if signaling != nil {
		_ = signaling.Close()
	}
	if pc != nil {
		_ = pc.Close()
	}
	if transport != nil {
		_ = transport.Close()
	}
	p.closeMux()
	return nil
}

func (p *Peer) Stats() ConnectionStats {
	p.mu.Lock()
	primary, transport := p.pc, p.transport
	p.mu.Unlock()
	connections := make([]*webrtc.PeerConnection, 0, maxParallelConnections)
	if primary != nil {
		connections = append(connections, primary)
	}
	if transport != nil {
		connections = append(connections, transport.secondaryPeerConnections()...)
	}
	stats := ConnectionStats{Connections: len(connections)}
	var rttTotal float64
	var rttSamples int
	for _, connection := range connections {
		for _, item := range connection.GetStats() {
			pair, ok := item.(webrtc.ICECandidatePairStats)
			if !ok || pair.State != webrtc.StatsICECandidatePairStateSucceeded || !pair.Nominated {
				continue
			}
			stats.BytesSent += pair.BytesSent
			stats.BytesReceived += pair.BytesReceived
			stats.PacketsDiscardedOnSend += uint64(pair.PacketsDiscardedOnSend)
			if pair.CurrentRoundTripTime > 0 {
				rttTotal += pair.CurrentRoundTripTime * 1000
				rttSamples++
			}
		}
	}
	if rttSamples > 0 {
		stats.CurrentRTTMilliseconds = rttTotal / float64(rttSamples)
	}
	return stats
}

func (p *Peer) replaceConnection(configuration api.ICEConfiguration, createChannel bool) error {
	pc, err := p.newPeerConnection(configuration)
	if err != nil {
		return err
	}
	negotiated := true
	priorityID := uint16(1023)
	ordered := true
	priority, err := pc.CreateDataChannel("quickdrop-priority", &webrtc.DataChannelInit{Ordered: &ordered, Negotiated: &negotiated, ID: &priorityID})
	if err != nil {
		_ = pc.Close()
		return err
	}
	p.mu.Lock()
	previous := p.pc
	previousTransport := p.transport
	p.pc = pc
	p.transport = nil
	p.policy = configuration.Policy
	p.pendingCandidates = nil
	p.mu.Unlock()
	if previousTransport != nil {
		_ = previousTransport.Close()
	}
	if previous != nil {
		_ = previous.Close()
	}

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		p.mu.Lock()
		active := p.pc == pc
		signaling := p.signaling
		p.mu.Unlock()
		if active && signaling != nil {
			_ = signaling.Send("ice_candidate", candidate.ToJSON())
		}
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		p.mu.Lock()
		active := p.pc == pc
		policy := p.policy
		p.mu.Unlock()
		if !active {
			return
		}
		if state == webrtc.PeerConnectionStateFailed {
			if policy.Transport == "relay" {
				p.fail(errors.New("TURN Relay connection failed"))
			}
			if policy.Transport != "relay" && p.initiator && p.options.AllowRelay {
				if err := p.beginRelay(); err != nil {
					p.fail(err)
				}
			}
		}
	})
	pc.OnDataChannel(func(channel *webrtc.DataChannel) { p.attachChannel(pc, channel, priority, configuration) })
	if createChannel {
		channel, createErr := pc.CreateDataChannel("quickdrop-data", &webrtc.DataChannelInit{Ordered: &ordered})
		if createErr != nil {
			_ = pc.Close()
			return createErr
		}
		p.attachChannel(pc, channel, priority, configuration)
	}
	return nil
}

func (p *Peer) newPeerConnection(configuration api.ICEConfiguration) (*webrtc.PeerConnection, error) {
	setting := webrtc.SettingEngine{}
	setting.SetIncludeLoopbackCandidate(true)
	setting.SetSCTPMaxReceiveBufferSize(p.options.SCTPReceiveBufferSize)
	if p.udpMux != nil {
		setting.SetICEUDPMux(p.udpMux)
	}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(setting))
	servers := make([]webrtc.ICEServer, 0, len(configuration.ICEServers))
	for _, server := range configuration.ICEServers {
		servers = append(servers, webrtc.ICEServer{URLs: server.URLs, Username: server.Username, Credential: server.Credential})
	}
	transportPolicy := webrtc.ICETransportPolicyAll
	if configuration.Policy.Transport == "relay" {
		transportPolicy = webrtc.ICETransportPolicyRelay
	}
	return api.NewPeerConnection(webrtc.Configuration{ICEServers: servers, ICETransportPolicy: transportPolicy})
}

func (p *Peer) attachChannel(pc *webrtc.PeerConnection, channel, priority *webrtc.DataChannel, configuration api.ICEConfiguration) {
	if channel.Label() != "quickdrop-data" {
		_ = channel.Close()
		return
	}
	connections := p.options.ParallelConnections
	if configuration.Policy.Transport == "relay" {
		connections = 1
	}
	transport := newParallelChannel(channel, priority, parallelChannelOptions{
		initiator: p.initiator, connections: connections,
		newPeerConnection: func() (*webrtc.PeerConnection, error) { return p.newPeerConnection(configuration) },
	})
	transport.OnError(func(err error) { p.fail(fmt.Errorf("DataChannel: %w", err)) })
	channel.OnOpen(func() {
		p.mu.Lock()
		if p.pc != pc || p.closed {
			p.mu.Unlock()
			return
		}
		policy := p.policy
		p.transport = transport
		p.mu.Unlock()
		maxMessageSize := uint32(0)
		if transport := channel.Transport(); transport != nil {
			maxMessageSize = transport.GetCapabilities().MaxMessageSize
		}
		mode, localCandidate, remoteCandidate := selectedConnectionInfo(channel, policy)
		p.resultOnce.Do(func() {
			p.result <- PeerResult{Channel: channel, Transport: transport, Policy: policy, Mode: mode, LocalCandidate: localCandidate, RemoteCandidate: remoteCandidate, MaxMessageSize: maxMessageSize}
		})
		transport.start()
	})
}

func selectedConnectionInfo(channel *webrtc.DataChannel, policy api.ICEPolicy) (string, string, string) {
	if policy.Transport == "relay" {
		return "relay", "", ""
	}
	sctp := channel.Transport()
	if sctp == nil || sctp.Transport() == nil || sctp.Transport().ICETransport() == nil {
		return "direct", "", ""
	}
	transport := sctp.Transport().ICETransport()
	var pair *webrtc.ICECandidatePair
	for range 25 {
		selected, err := transport.GetSelectedCandidatePair()
		if err == nil && selected != nil && selected.Local != nil && selected.Remote != nil {
			pair = selected
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pair == nil {
		return "direct", "", ""
	}
	local, remote := candidateSummary(pair.Local), candidateSummary(pair.Remote)
	if pair.Local.Typ == webrtc.ICECandidateTypeRelay || pair.Remote.Typ == webrtc.ICECandidateTypeRelay {
		return "relay", local, remote
	}
	if pair.Local.Typ == webrtc.ICECandidateTypeHost && pair.Remote.Typ == webrtc.ICECandidateTypeHost {
		// Two processes on the same host can select a public-looking VPN or
		// overlay address. Equal host addresses still describe a local path,
		// while different public host addresses remain Internet P2P.
		if pair.Local.Address == pair.Remote.Address || isLocalAddress(pair.Local.Address) && isLocalAddress(pair.Remote.Address) {
			return "lan", local, remote
		}
	}
	return "direct", local, remote
}

func candidateSummary(candidate *webrtc.ICECandidate) string {
	if candidate == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s:%d", candidate.Typ.String(), candidate.Protocol.String(), candidate.Address, candidate.Port)
}

func isLocalAddress(value string) bool {
	address, err := netip.ParseAddr(value)
	if err != nil {
		return false
	}
	if address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() {
		return true
	}
	// ICE may bind a host candidate to a VPN or userspace network adapter.
	// Shared-address space and the benchmarking block are not publicly routed
	// and therefore represent a local/overlay path rather than Internet P2P.
	for _, prefix := range []string{"100.64.0.0/10", "198.18.0.0/15"} {
		if network := netip.MustParsePrefix(prefix); network.Contains(address) {
			return true
		}
	}
	return false
}

func (p *Peer) signalLoop(ctx context.Context) {
	p.mu.Lock()
	signaling := p.signaling
	p.mu.Unlock()
	if signaling == nil {
		return
	}
	for {
		select {
		case message, ok := <-signaling.Messages():
			if !ok {
				return
			}
			if err := p.handleSignal(ctx, message); err != nil {
				p.fail(err)
				return
			}
		case err := <-signaling.Errors():
			p.fail(fmt.Errorf("signaling disconnected: %w", err))
			return
		case <-ctx.Done():
			return
		}
	}
}

func (p *Peer) handleSignal(ctx context.Context, message signaling.Signal) error {
	switch message.Type {
	case "peer_joined":
		if p.initiator {
			p.startOnce.Do(func() { close(p.negotiationStarted) })
			return p.sendOffer()
		}
	case "offer":
		var offer webrtc.SessionDescription
		if err := json.Unmarshal(message.Payload, &offer); err != nil {
			return err
		}
		pc := p.activePC()
		if pc == nil {
			return errors.New("peer connection is closed")
		}
		if err := pc.SetRemoteDescription(offer); err != nil {
			return err
		}
		if err := p.flushCandidates(pc); err != nil {
			return err
		}
		answer, err := pc.CreateAnswer(nil)
		if err != nil {
			return err
		}
		if err := pc.SetLocalDescription(answer); err != nil {
			return err
		}
		return p.sendSignal("answer", pc.LocalDescription())
	case "answer":
		var answer webrtc.SessionDescription
		if err := json.Unmarshal(message.Payload, &answer); err != nil {
			return err
		}
		pc := p.activePC()
		if pc == nil {
			return errors.New("peer connection is closed")
		}
		if err := pc.SetRemoteDescription(answer); err != nil {
			return err
		}
		return p.flushCandidates(pc)
	case "ice_candidate":
		var candidate webrtc.ICECandidateInit
		if err := json.Unmarshal(message.Payload, &candidate); err != nil {
			return err
		}
		pc := p.activePC()
		if pc == nil {
			return nil
		}
		if pc.RemoteDescription() == nil {
			p.mu.Lock()
			p.pendingCandidates = append(p.pendingCandidates, candidate)
			p.mu.Unlock()
			return nil
		}
		return pc.AddICECandidate(candidate)
	case "transport_restart":
		if p.initiator {
			return nil
		}
		provider := readProvider(message.Payload, p.options.RelayProvider)
		relay, err := p.client.ICE(ctx, p.creds, "relay", provider)
		if err != nil {
			_ = p.sendSignal("transport_unavailable", map[string]string{"provider": provider})
			return nil
		}
		if err := p.replaceConnection(relay, false); err != nil {
			return err
		}
		return p.sendSignal("transport_ready", map[string]string{"provider": provider})
	case "transport_ready":
		if !p.initiator {
			return nil
		}
		provider := readProvider(message.Payload, p.options.RelayProvider)
		relay, err := p.client.ICE(ctx, p.creds, "relay", provider)
		if err != nil {
			return err
		}
		if err := p.replaceConnection(relay, true); err != nil {
			return err
		}
		return p.sendOffer()
	case "transport_unavailable":
		return errors.New("the remote peer cannot use TURN")
	case "direct_failed":
		return errors.New("Direct P2P failed")
	case "peer_left":
		return errors.New("peer disconnected")
	case "error":
		if message.Message != "" {
			return errors.New(message.Message)
		}
		return errors.New("signaling server rejected the connection")
	}
	return nil
}

func (p *Peer) sendOffer() error {
	pc := p.activePC()
	if pc == nil {
		return errors.New("peer connection is closed")
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	return p.sendSignal("offer", pc.LocalDescription())
}

func (p *Peer) flushCandidates(pc *webrtc.PeerConnection) error {
	p.mu.Lock()
	pending := append([]webrtc.ICECandidateInit(nil), p.pendingCandidates...)
	p.pendingCandidates = nil
	p.mu.Unlock()
	for _, candidate := range pending {
		if err := pc.AddICECandidate(candidate); err != nil {
			return err
		}
	}
	return nil
}

func (p *Peer) activePC() *webrtc.PeerConnection { p.mu.Lock(); defer p.mu.Unlock(); return p.pc }

func (p *Peer) beginRelay() error {
	p.mu.Lock()
	if p.relayStarted || p.closed {
		p.mu.Unlock()
		return nil
	}
	p.relayStarted = true
	p.mu.Unlock()
	p.mu.Lock()
	connected := p.signaling != nil
	p.mu.Unlock()
	if !connected {
		return errors.New("signaling is not connected")
	}
	if _, err := p.client.ICE(context.Background(), p.creds, "relay", p.options.RelayProvider); err != nil {
		return fmt.Errorf("TURN unavailable: %w", err)
	}
	return p.sendSignal("transport_restart", map[string]string{"provider": p.options.RelayProvider})
}

func (p *Peer) sendSignal(messageType string, payload any) error {
	p.mu.Lock()
	signaling := p.signaling
	p.mu.Unlock()
	if signaling == nil {
		return errors.New("signaling is not connected")
	}
	return signaling.Send(messageType, payload)
}

func (p *Peer) fail(err error) {
	if err == nil {
		return
	}
	p.errorOnce.Do(func() { p.errors <- err })
}

func (p *Peer) closeMux() {
	if p.udpMux != nil {
		_ = p.udpMux.Close()
		p.udpMux = nil
	}
}

func readProvider(raw json.RawMessage, fallback string) string {
	var value struct {
		Provider string `json:"provider"`
	}
	if json.Unmarshal(raw, &value) == nil && (value.Provider == "platform" || value.Provider == "custom") {
		return value.Provider
	}
	if fallback == "custom" {
		return "custom"
	}
	return "platform"
}
