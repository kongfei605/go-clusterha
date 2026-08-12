package clusterha

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

func (n *Node) startInternalServer() error {
	cert, roots, err := loadTLSIdentity(n.cfg.InternalTLS)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", n.cfg.InternalAPIBindAddr)
	if err != nil {
		return fmt.Errorf("listen on internal API address %s: %w", n.cfg.InternalAPIBindAddr, err)
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/v1/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, n.Status())
	})
	mux.HandleFunc("POST /internal/v1/barrier", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(n.cfg.MaxQuorumVerificationAge))
		defer cancel()
		if err := n.VerifyBusinessLeadership(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		n.refreshStatus(false)
		writeJSON(w, http.StatusOK, n.Status())
	})
	mux.HandleFunc("PUT /internal/v1/blobs/{hash...}", func(w http.ResponseWriter, r *http.Request) {
		hash := strings.TrimPrefix(r.PathValue("hash"), "/")
		if hash == "" {
			http.Error(w, "blob hash is required", http.StatusBadRequest)
			return
		}
		if r.ContentLength > n.cfg.MaxSnapshotBlobBytes {
			http.Error(w, "blob body exceeds configured limit", http.StatusRequestEntityTooLarge)
			return
		}
		select {
		case n.blobPutSem <- struct{}{}:
			defer func() { <-n.blobPutSem }()
		default:
			http.Error(w, "too many concurrent blob uploads", http.StatusTooManyRequests)
			return
		}
		n.mu.RLock()
		store := n.blobStore
		n.mu.RUnlock()
		if store == nil {
			http.Error(w, ErrNodeNotStarted.Error(), http.StatusServiceUnavailable)
			return
		}
		defer r.Body.Close()
		if err := store.PutHash(hash, http.MaxBytesReader(w, r.Body, n.cfg.MaxSnapshotBlobBytes)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"hash": hash})
	})
	mux.HandleFunc("GET /internal/v1/blobs/{hash...}", func(w http.ResponseWriter, r *http.Request) {
		hash := strings.TrimPrefix(r.PathValue("hash"), "/")
		if !n.isCommittedBlob(hash) {
			http.Error(w, "blob is not referenced by a committed manifest", http.StatusNotFound)
			return
		}
		n.mu.RLock()
		store := n.blobStore
		n.mu.RUnlock()
		if store == nil {
			http.Error(w, ErrNodeNotStarted.Error(), http.StatusServiceUnavailable)
			return
		}
		file, err := store.Open(hash)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		defer file.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, hash, time.Time{}, file)
	})
	mux.Handle("/internal/v1/proxy/", n.internalProxyHandler())
	server := &http.Server{
		Handler:           n.requireMemberCertificate(mux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	tlsListener := tls.NewListener(listener, tlsConfig)
	n.mu.Lock()
	n.internalServer = server
	n.internalListener = tlsListener
	n.mu.Unlock()
	go func() {
		_ = server.Serve(tlsListener)
	}()
	return nil
}

func (n *Node) requireMemberCertificate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "authenticated cluster member certificate required", http.StatusUnauthorized)
			return
		}
		identity := r.TLS.PeerCertificates[0].Subject.CommonName
		if !containsString(r.TLS.PeerCertificates[0].Subject.Organization, n.cfg.ClusterID) {
			http.Error(w, "certificate cluster identity does not match", http.StatusForbidden)
			return
		}
		if _, ok := n.peers.Member(identity); !ok {
			http.Error(w, "certificate identity is not a committed cluster member", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
