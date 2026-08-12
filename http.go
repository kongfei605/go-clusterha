package clusterha

import (
	"encoding/json"
	"net/http"
)

type HTTPHandler struct {
	node *Node
}

func NewHTTPHandler(node *Node) *HTTPHandler {
	return &HTTPHandler{node: node}
}

func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /livez", h.Live)
	mux.HandleFunc("GET /readyz", h.Ready)
	mux.HandleFunc("GET /api/v1/cluster/status", h.Status)
	mux.HandleFunc("GET /api/v1/cluster/nodes", h.Nodes)
	mux.HandleFunc("GET /api/v1/cluster/snapshot/status", h.SnapshotStatus)
}

func (h *HTTPHandler) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *HTTPHandler) Ready(w http.ResponseWriter, _ *http.Request) {
	status := h.node.Status()
	if !status.Ready {
		writeJSON(w, http.StatusServiceUnavailable, status)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *HTTPHandler) Status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.node.Status())
}

func (h *HTTPHandler) Nodes(w http.ResponseWriter, _ *http.Request) {
	status := h.node.Status()
	if !status.Enabled {
		writeJSON(w, http.StatusOK, []MemberStatus{{NodeID: status.NodeID, Suffrage: RoleStandalone}})
		return
	}
	members, err := h.node.Members()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error(), "node": status})
		return
	}
	writeJSON(w, http.StatusOK, members)
}

func (h *HTTPHandler) SnapshotStatus(w http.ResponseWriter, _ *http.Request) {
	status := h.node.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled": status.Enabled,
		"ready":   status.Ready,
		"reason":  status.Reason,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
