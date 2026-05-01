package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
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
// Peers discover each other via mDNS, explicit seed nodes, or HTTP-based discovery.
// Invalidation messages are broadcast via memberlist's gossip protocol.
type GossipInvalidator struct {
	list       *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	handler    func(keys []string)
	mdnsServer *mdns.Server
	mu         sync.RWMutex
	stopCh     chan struct{}
	serviceURL string // HTTP discovery endpoint URL
	gossipPort int    // port memberlist is bound to
}

// invalidateMessage is the gossip payload for cache invalidation.
type invalidateMessage struct {
	Keys []string `json:"keys"`
}

// NewGossipInvalidator creates and starts a gossip-based cache invalidator.
func NewGossipInvalidator(cfg GossipConfig) (*GossipInvalidator, error) {
	gi := &GossipInvalidator{
		stopCh:     make(chan struct{}),
		gossipPort: cfg.BindPort,
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
	mlConfig.Events = &gossipEvents{localName: cfg.NodeName, clusterName: detectServiceName()}

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

	// Auto-discover peers via service DNS if no explicit peers given
	if len(cfg.Peers) == 0 {
		if serviceName := detectServiceName(); serviceName != "" {
			// Use HTTP-based discovery (port 8080 is routed by the service)
			gi.serviceURL = fmt.Sprintf("http://%s:8080/api/cluster/peers", serviceName)
			go gi.httpDiscoveryLoop()
		} else if cfg.EnableMDNS {
			gi.startMDNS(cfg)
		}
	} else {
		// Join explicit peers
		joined, err := list.Join(cfg.Peers)
		if err != nil {
			log.Debug("Peer join attempt: %v", err)
		} else if joined > 0 {
			log.Info("Joined %d cluster peer(s)", joined)
		}
		go gi.rejoinLoop(cfg.Peers)
	}

	return gi, nil
}

// rejoinLoop periodically attempts to join peers via service DNS to heal split clusters.
func (gi *GossipInvalidator) rejoinLoop(peers []string) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			joined, _ := gi.list.Join(peers)
			if joined > 0 {
				log.Info("Cluster healed: joined %d new peer(s)", joined)
			}
		case <-gi.stopCh:
			return
		}
	}
}

// httpDiscoveryLoop periodically queries the service HTTP endpoint to discover peer gossip addresses.
func (gi *GossipInvalidator) httpDiscoveryLoop() {
	// Wait for the HTTP server to start
	time.Sleep(3 * time.Second)
	gi.discoverViaHTTP()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gi.discoverViaHTTP()
		case <-gi.stopCh:
			return
		}
	}
}

func (gi *GossipInvalidator) discoverViaHTTP() {
	if gi.serviceURL == "" {
		return
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(gi.serviceURL)
	if err != nil {
		log.Debug("Peer discovery HTTP failed: %v", err)
		return
	}
	defer resp.Body.Close()

	var result struct {
		GossipAddr string   `json:"gossip_addr"`
		Peers      []string `json:"peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Debug("Peer discovery response parse failed: %v", err)
		return
	}

	// Try to join the peer that responded
	if result.GossipAddr != "" {
		joined, _ := gi.list.Join([]string{result.GossipAddr})
		if joined > 0 {
			log.Info("Found peer: %s", result.GossipAddr)
		}
	}

	// Also try any peers it knows about
	for _, peer := range result.Peers {
		joined, _ := gi.list.Join([]string{peer})
		if joined > 0 {
			log.Info("Found peer: %s", peer)
		}
	}
}

// ClusterPeersHandler returns an HTTP handler for the /api/cluster/peers endpoint.
// It responds with this node's gossip address and known peers.
func (gi *GossipInvalidator) ClusterPeersHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		localIP := ""
		if ips := getLocalIPs(); len(ips) > 0 {
			localIP = ips[0].String()
		}

		gossipAddr := fmt.Sprintf("%s:%d", localIP, gi.gossipPort)

		var peers []string
		if gi.list != nil {
			for _, m := range gi.list.Members() {
				addr := fmt.Sprintf("%s:%d", m.Addr.String(), m.Port)
				if addr != gossipAddr {
					peers = append(peers, addr)
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"gossip_addr": gossipAddr,
			"peers":       peers,
			"members":     gi.list.NumMembers(),
		})
	}
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
	log.Debug("Cluster invalidation published (%d keys)", len(keys))
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
		log.Error("Cluster invalidation message error: %v", err)
		return
	}

	gi.mu.RLock()
	handler := gi.handler
	gi.mu.RUnlock()

	if handler != nil && len(msg.Keys) > 0 {
		log.Debug("Cluster invalidation received (%d keys)", len(msg.Keys))
		handler(msg.Keys)
	}
}

// detectServiceName tries to derive the Kubernetes/DO service name from the hostname.
// Hostnames look like "velocity-server-6c758b648f-xscmq" — the service is "velocity-server".
// We strip the last two dash-separated segments (replicaset hash + pod hash).
func detectServiceName() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return ""
	}

	parts := strings.Split(hostname, "-")
	if len(parts) < 3 {
		return ""
	}

	// Service name is everything except the last two segments
	serviceName := strings.Join(parts[:len(parts)-2], "-")

	// Verify it resolves via DNS
	addrs, err := net.LookupHost(serviceName)
	if err != nil || len(addrs) == 0 {
		return ""
	}

	return serviceName
}

// getLocalIPs returns non-loopback IPv4 addresses from network interfaces.
func getLocalIPs() []net.IP {
	var ips []net.IP
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && ip.To4() != nil {
				ips = append(ips, ip)
			}
		}
	}
	return ips
}

// startMDNS advertises this node via mDNS and discovers peers.
func (gi *GossipInvalidator) startMDNS(cfg GossipConfig) {
	port := cfg.BindPort
	host, _ := os.Hostname()

	// Get IPs from interfaces directly (hostname may not resolve in containers)
	ips := getLocalIPs()

	// Advertise this node
	service, err := mdns.NewMDNSService(host, mdnsServiceName, "", "", port, ips, []string{"velocity cache invalidation"})
	if err != nil {
		log.Error("mDNS service creation failed: %v", err)
		return
	}

	// Silence the standard logger during mDNS setup (it logs IPv6 warnings directly)
	origOutput := stdlog.Writer()
	stdlog.SetOutput(io.Discard)
	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	stdlog.SetOutput(origOutput)
	if err != nil {
		log.Error("mDNS server start failed: %v", err)
		return
	}
	gi.mdnsServer = server

	// Discover peers in background
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
				log.Debug("Peer join failed (%s): %v", addr, err)
			} else if joined > 0 {
				log.Info("Found peer: %s", addr)
				found++
			}
		}
	}()

	params := mdns.DefaultParams(mdnsServiceName)
	params.Entries = entriesCh
	params.Timeout = 3 * time.Second
	params.DisableIPv6 = true

	origOutput := stdlog.Writer()
	stdlog.SetOutput(io.Discard)
	err := mdns.Query(params)
	stdlog.SetOutput(origOutput)
	if err != nil {
		log.Debug("mDNS query error: %v", err)
	}
	close(entriesCh)

	if gi.list.NumMembers() > 1 {
		members := gi.list.Members()
		var names []string
		for _, m := range members {
			if m.Name != gi.list.LocalNode().Name {
				names = append(names, m.Name)
			}
		}
		log.Info("Known cluster peers: %s", strings.Join(names, ", "))
	}
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

type gossipEvents struct {
	localName   string
	clusterName string
}

func (e *gossipEvents) NotifyJoin(node *memberlist.Node) {
	if node.Name == e.localName {
		if e.clusterName != "" {
			log.Info("Joined the cluster '%s' as %s.", e.clusterName, node.Name)
		} else {
			log.Info("Joined the cluster as %s.", node.Name)
		}
	} else {
		log.Info("Peer %s (%s) joined the cluster.", node.Name, node.Addr)
	}
}
func (e *gossipEvents) NotifyLeave(node *memberlist.Node) {
	if node.Name != e.localName {
		log.Info("Peer %s (%s) left the cluster.", node.Name, node.Addr)
	}
}
func (e *gossipEvents) NotifyUpdate(node *memberlist.Node) {
	log.Debug("Peer updated: %s", node.Name)
}

// --- log writer adapter ---

type logWriter struct{}

func (lw *logWriter) Write(p []byte) (n int, err error) {
	log.Debug("memberlist: %s", string(p))
	return len(p), nil
}
