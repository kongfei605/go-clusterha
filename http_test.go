package clusterha

import (
	"net/http"
	"net/http/httptest"
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
