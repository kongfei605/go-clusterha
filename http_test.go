package clusterha

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPHandlerReadyWhenDisabled(t *testing.T) {
	node, err := NewNode(Config{})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewHTTPHandler(node).RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestMembershipAdminAuthorizationFailsClosed(t *testing.T) {
	node := &Node{cfg: Config{MembershipAdminNodeIDs: []string{"node-a"}}}
	request := httptest.NewRequest(http.MethodPost, "/internal/v1/admin/nodes", nil)
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Subject: pkix.Name{CommonName: "node-b"}}}}
	if node.authorizeMembershipAdmin(request) {
		t.Fatal("non-admin member was authorized")
	}
	request.TLS.PeerCertificates[0].Subject.CommonName = "node-a"
	if !node.authorizeMembershipAdmin(request) {
		t.Fatal("configured admin member was rejected")
	}
}

func TestHTTPHandlerNotReadyBeforeEnabledNodeStarts(t *testing.T) {
	node, err := NewNode(validEnabledConfig())
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewHTTPHandler(node).RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHTTPHandlerDoesNotExposeMembershipWrites(t *testing.T) {
	node, err := NewNode(validEnabledConfig())
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewHTTPHandler(node).RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/nodes", strings.NewReader("{"))
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("join status=%d body=%s", rec.Code, rec.Body.String())
	}
}
