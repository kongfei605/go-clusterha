package clusterha

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type tlsStreamLayer struct {
	net.Listener
	advertise net.Addr
	serverTLS *tls.Config
	clientTLS *tls.Config
	peers     *peerRegistry
}

type peerRegistry struct {
	mu      sync.RWMutex
	members map[string]Member
}

func newPeerRegistry(members []Member) *peerRegistry {
	r := &peerRegistry{}
	r.Replace(members)
	return r
}

func (r *peerRegistry) Replace(members []Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members = make(map[string]Member, len(members))
	for _, member := range members {
		r.members[member.NodeID] = member
	}
}

func (r *peerRegistry) Upsert(member Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.members == nil {
		r.members = make(map[string]Member)
	}
	r.members[member.NodeID] = member
}

func (r *peerRegistry) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.members, nodeID)
}

func (r *peerRegistry) Member(nodeID string) (Member, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	member, ok := r.members[nodeID]
	return member, ok
}

func (r *peerRegistry) NodeIDByAddress(address string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for nodeID, member := range r.members {
		if member.Addr == address {
			return nodeID, true
		}
	}
	return "", false
}

func newTLSStreamLayer(cfg Config, peers *peerRegistry) (*tlsStreamLayer, error) {
	cert, roots, err := loadTLSIdentity(cfg.InternalTLS)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", cfg.RaftBindAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on raft address %s: %w", cfg.RaftBindAddr, err)
	}
	advertise, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertiseAddr)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("resolve raft advertise address: %w", err)
	}
	serverTLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
	}
	serverTLS.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("raft peer did not present a certificate")
		}
		identity := state.PeerCertificates[0].Subject.CommonName
		if !containsString(state.PeerCertificates[0].Subject.Organization, cfg.ClusterID) {
			return fmt.Errorf("raft peer certificate is not scoped to cluster %q", cfg.ClusterID)
		}
		if _, ok := peers.Member(identity); !ok {
			return fmt.Errorf("raft peer certificate identity %q is not a committed member", identity)
		}
		return nil
	}
	clientTLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
		ServerName:   cfg.InternalTLS.ServerName,
	}
	return &tlsStreamLayer{Listener: listener, advertise: advertise, serverTLS: serverTLS, clientTLS: clientTLS, peers: peers}, nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (l *tlsStreamLayer) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return tls.Server(conn, l.serverTLS), nil
}

func (l *tlsStreamLayer) Addr() net.Addr {
	return l.advertise
}

func (l *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	config := l.clientTLS.Clone()
	expectedID, ok := l.peers.NodeIDByAddress(string(address))
	if !ok {
		return nil, fmt.Errorf("raft address %s is not in committed membership", address)
	}
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("raft server did not present a certificate")
		}
		if identity := state.PeerCertificates[0].Subject.CommonName; identity != expectedID {
			return fmt.Errorf("raft server certificate identity %q does not match member %q", identity, expectedID)
		}
		return nil
	}
	return tls.DialWithDialer(dialer, "tcp", string(address), config)
}

func loadTLSIdentity(cfg TLSConfig) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load internal TLS certificate: %w", err)
	}
	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("read internal TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, fmt.Errorf("internal TLS CA file %s contains no certificates", cfg.CAFile)
	}
	return cert, roots, nil
}
