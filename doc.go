// Package quickdrop is a convenience facade over the public SDK packages.
// Applications that need finer dependency boundaries can import api, peer,
// presence, signaling, transfer, and protocol directly.
package quickdrop

import (
	"github.com/Aruelius/quickdrop-go/api"
	quickdropchannel "github.com/Aruelius/quickdrop-go/channel"
	"github.com/Aruelius/quickdrop-go/transfer"
	peer "github.com/Aruelius/quickdrop-go/webrtc"
)

type Client = api.Client
type ClientOptions = api.ClientOptions
type Credentials = api.Credentials
type ICEPolicy = api.ICEPolicy
type ICEConfiguration = api.ICEConfiguration
type Peer = peer.Peer
type PeerOptions = peer.PeerOptions
type PeerResult = peer.PeerResult
type ConnectionStats = peer.ConnectionStats
type Channel = quickdropchannel.Channel
type TransferManager = transfer.TransferManager
type TransferOptions = transfer.TransferOptions
type TransferEvent = transfer.TransferEvent

var New = api.New
var NewWithOptions = api.NewWithOptions
var NewPeer = peer.New
var NewTransferManager = transfer.NewTransferManager
