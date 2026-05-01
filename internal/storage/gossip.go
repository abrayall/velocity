package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/mdns"
	"github.com/hashicorp/memberlist"

	"velocity/internal/log"
)

const (
	// mDNS service name for velocity instances
	mdnsServiceName = "_velocity._tcp"
	// Default gossip port
	defaultGossipPort = 7946
)

// GossipConfig configures the gossip-based cache invalidator.
type GossipConfig struct {
	// NodeName is a unique name for this node. Defaults to hostname.
	NodeName string
	// BindAddr is the address to bind the gossip listener. Defaults to "0.0.0.0".
	BindAddr string
	// BindPort is the port for gossip communication. Defaults to 7946.
	BindPort int
	// AdvertiseAddr is the address advertised to other nodes. Defaults to auto-detected.
	AdvertiseAddr string
	// AdvertisePort is the port advertised to other nodes. Defaults to BindPort.
	AdvertisePort int
	// Peers is an optional list of known peer addresses to join (host:port).
	// If empty, mDNS discovery is used.
	Peers []string
	// EnableMDNS enables mDNS for automatic peer discovery on the local network.
	EnableMDNS bool
}

// DefaultGossipConfig returns sensible defaults with mDNS enabled.
func DefaultGossipConfig() GossipConfig {
	hostname, _ := os.Hostname()
	return GossipConfig{
		NodeName:   hostname,
		BindAddr:   "0.0.0.0",
		BindPort:   defaultGossipPort,
		EnableMDNS: true,
	}
}

// GossipInvalidator uses hashicorp/memberlist for peer-to-peer cache invalidation.
// Peers discover each other via mDNS or explicit seed nodes.
// Invalidation messages are broadcast via memberlist's gossip protocol.
type GossipInvalidator struct {
	list       *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	handler    func(keys []string)
	mdnsServer *mdns.Server
	mu         sync.RWMutex
	stopCh     chan struct{}
}

// invalidateMessage is the gossip payload for cache invalidation.
type invalidateMessage struct {
	Keys []string `json:"keys"`
}

// NewGossipInvalidator creates and starts a gossip-based cache invalidator.
func NewGossipInvalidator(cfg GossipConfig) (*GossipInvalidator, error) {
	gi := &GossipInvalidator{
		stopCh: make(chan struct{}),
	}

	// Configure memberlist
	mlConfig := memberlist.DefaultLANConfig()
	mlConfig.Name = cfg.NodeName
	mlConfig.BindAddr = cfg.BindAddr
	mlConfig.BindPort = cfg.BindPort
	if cfg.AdvertiseAddr != "" {
		mlConfig.AdvertiseAddr = cfg.AdvertiseAddr
	}
	if cfg.AdvertisePort > 0 {
		mlConfig.AdvertisePort = cfg.AdvertisePort
	} else {
		mlConfig.AdvertisePort = cfg.BindPort
	}

	// Suppress memberlist logs — use our own logging
	mlConfig.LogOutput = &logWriter{}

	// Set up the delegate for receiving broadcasts
	delegate := &gossipDelegate{gi: gi}
	mlConfig.Delegate = delegate
	mlConfig.Events = &gossipEvents{}

	// Create the memberlist
	list, err := memberlist.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}
	gi.list = list

	// Set up broadcast queue
	gi.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int { return list.NumMembers() },
		RetransmitMult: 3,
	}

	// Join known peers if provided
	if len(cfg.Peers) > 0 {
		joined, err := list.Join(cfg.Peers)
		if err != nil {
			log.Error("Cluster: failed to join peers: %v", err)
		} else {
			log.Info("Cluster: joined %d peer(s)", joined)
		}
	}

	// Start mDNS advertisement and discovery
	if cfg.EnableMDNS {
		gi.startMDNS(cfg)
	}

	log.Info("Cluster: node '%s' started on %s:%d (%d members)",
		cfg.NodeName, cfg.BindAddr, cfg.BindPort, list.NumMembers())

	return gi, nil
}

// Publish broadcasts invalidation keys to all peers via gossip.
func (gi *GossipInvalidator) Publish(_ context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	msg := invalidateMessage{Keys: keys}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal invalidation message: %w", err)
	}

	gi.broadcasts.QueueBroadcast(&gossipBroadcast{data: data})
	log.Debug("Cluster: published invalidation for %d key(s)", len(keys))
	return nil
}

// Subscribe registers a handler for incoming invalidation messages from peers.
func (gi *GossipInvalidator) Subscribe(handler func(keys []string)) error {
	gi.mu.Lock()
	gi.handler = handler
	gi.mu.Unlock()
	return nil
}

// Close shuts down the gossip layer and mDNS.
func (gi *GossipInvalidator) Close() error {
	close(gi.stopCh)

	if gi.mdnsServer != nil {
		gi.mdnsServer.Shutdown()
	}

	if gi.list != nil {
		return gi.list.Leave(5 * time.Second)
	}
	return nil
}

// handleMessage processes an incoming invalidation broadcast from a peer.
func (gi *GossipInvalidator) handleMessage(data []byte) {
	var msg invalidateMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Error("Cluster: failed to unmarshal message: %v", err)
		return
	}

	gi.mu.RLock()
	handler := gi.handler
	gi.mu.RUnlock()

	if handler != nil && len(msg.Keys) > 0 {
		log.Debug("Cluster: received invalidation for %d key(s)", len(msg.Keys))
		handler(msg.Keys)
	}
}

// startMDNS advertises this node via mDNS and discovers peers.
func (gi *GossipInvalidator) startMDNS(cfg GossipConfig) {
	port := cfg.BindPort
	host, _ := os.Hostname()

	// Advertise this node
	service, err := mdns.NewMDNSService(host, mdnsServiceName, "", "", port, nil, []string{"velocity cache invalidation"})
	if err != nil {
		log.Error("Cluster: failed to create mDNS service: %v", err)
		return
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		log.Error("Cluster: failed to start mDNS server: %v", err)
		return
	}
	gi.mdnsServer = server
	log.Info("Cluster: mDNS advertising on %s", mdnsServiceName)

	// Discover peers in background
	log.Info("Cluster: listening for peers via mDNS (%s)", mdnsServiceName)
	go gi.discoverPeers()
}

// discoverPeers periodically looks for other velocity instances via mDNS.
func (gi *GossipInvalidator) discoverPeers() {
	// Initial discovery after a short delay to allow the node to start
	time.Sleep(2 * time.Second)
	gi.runDiscovery()

	// Periodic re-discovery
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gi.runDiscovery()
		case <-gi.stopCh:
			return
		}
	}
}

// Members returns all known cluster members (name + address).
func (gi *GossipInvalidator) Members() []map[string]string {
	if gi.list == nil {
		return nil
	}
	members := gi.list.Members()
	result := make([]map[string]string, 0, len(members))
	for _, m := range members {
		result = append(result, map[string]string{
			"name": m.Name,
			"addr": net.JoinHostPort(m.Addr.String(), strconv.Itoa(int(m.Port))),
		})
	}
	return result
}

// NumMembers returns the number of known cluster members.
func (gi *GossipInvalidator) NumMembers() int {
	if gi.list == nil {
		return 0
	}
	return gi.list.NumMembers()
}

func (gi *GossipInvalidator) runDiscovery() {
	log.Debug("Cluster: scanning for peers via mDNS...")
	entriesCh := make(chan *mdns.ServiceEntry, 10)
	found := 0

	go func() {
		for entry := range entriesCh {
			addr := net.JoinHostPort(entry.AddrV4.String(), strconv.Itoa(entry.Port))

			// Skip ourselves
			if gi.isSelf(entry.AddrV4, entry.Port) {
				continue
			}

			// Try to join this peer
			joined, err := gi.list.Join([]string{addr})
			if err != nil {
				log.Debug("Cluster: mDNS peer join failed (%s): %v", addr, err)
			} else if joined > 0 {
				log.Info("Cluster: discovered peer via mDNS: %s", addr)
				found++
			}
		}
	}()

	params := mdns.DefaultParams(mdnsServiceName)
	params.Entries = entriesCh
	params.Timeout = 3 * time.Second
	params.DisableIPv6 = true

	if err := mdns.Query(params); err != nil {
		log.Debug("Cluster: mDNS query error: %v", err)
	}
	close(entriesCh)

	log.Debug("Cluster: scan complete, %d peers known", gi.list.NumMembers()-1)
}

func (gi *GossipInvalidator) isSelf(addr net.IP, port int) bool {
	if gi.list == nil {
		return false
	}
	self := gi.list.LocalNode()
	return self.Addr.Equal(addr) && int(self.Port) == port
}

// --- memberlist delegate ---

type gossipDelegate struct {
	gi *GossipInvalidator
}

func (d *gossipDelegate) NodeMeta(limit int) []byte              { return nil }
func (d *gossipDelegate) NotifyMsg(msg []byte)                   { d.gi.handleMessage(msg) }
func (d *gossipDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.gi.broadcasts.GetBroadcasts(overhead, limit)
}
func (d *gossipDelegate) LocalState(join bool) []byte            { return nil }
func (d *gossipDelegate) MergeRemoteState(buf []byte, join bool) {}

// --- gossip broadcast ---

type gossipBroadcast struct {
	data []byte
}

func (b *gossipBroadcast) Invalidates(other memberlist.Broadcast) bool { return false }
func (b *gossipBroadcast) Message() []byte                             { return b.data }
func (b *gossipBroadcast) Finished()                                   {}

// --- gossip events (logging only) ---

type gossipEvents struct{}

func (e *gossipEvents) NotifyJoin(node *memberlist.Node) {
	log.Info("Cluster: node joined: %s (%s)", node.Name, node.Addr)
}
func (e *gossipEvents) NotifyLeave(node *memberlist.Node) {
	log.Info("Cluster: node left: %s (%s)", node.Name, node.Addr)
}
func (e *gossipEvents) NotifyUpdate(node *memberlist.Node) {
	log.Debug("Cluster: node updated: %s", node.Name)
}

// --- log writer adapter ---

type logWriter struct{}

func (lw *logWriter) Write(p []byte) (n int, err error) {
	log.Debug("Cluster: %s", string(p))
	return len(p), nil
}
