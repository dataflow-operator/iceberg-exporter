package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTokenSourceFetchAndRefresh(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(raw))
		n := len(bodies)
		mu.Unlock()
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			http.Error(w, "type", http.StatusBadRequest)
			return
		}
		if n == 1 {
			w.Write([]byte(`{"access_token":"aaa","expires_in":300}`))
			return
		}
		w.Write([]byte(`{"access_token":"bbb","expires_in":"120"}`))
	}))
	t.Cleanup(srv.Close)

	src := &tokenSource{
		url:          srv.URL,
		clientID:     "client",
		clientSecret: "secret",
		interval:     20 * time.Millisecond,
		http:         srv.Client(),
	}
	token, ttl, err := src.fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != "aaa" || ttl != 300*time.Second {
		t.Fatalf("token %q ttl %s", token, ttl)
	}
	if src.wait(ttl) != 20*time.Millisecond {
		t.Fatalf("interval wait %s", src.wait(ttl))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan string, 4)
	done := make(chan error, 1)
	go func() {
		done <- src.run(ctx, func(token string) { got <- token }, ttl, true)
	}()
	select {
	case token = <-got:
	case <-time.After(time.Second):
		t.Fatal("refresh did not update the bearer")
	}
	cancel()
	select {
	case err = <-done:
		if err != context.Canceled {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not stop")
	}
	if token != "bbb" {
		t.Fatalf("refreshed token %q", token)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, body := range bodies {
		if !strings.Contains(body, "grant_type=client_credentials") || !strings.Contains(body, "client_id=client") || !strings.Contains(body, "client_secret=secret") {
			t.Fatalf("form %q", body)
		}
	}
}

func TestTokenSourceKeepsBearerWhenRefreshFails(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "client_secret=secret failed", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	src := &tokenSource{
		url:          srv.URL,
		clientID:     "client",
		clientSecret: "secret",
		http:         srv.Client(),
	}
	_, _, err := src.fetch(context.Background())
	if err == nil || !strings.Contains(err.Error(), "[redacted]") || strings.Contains(err.Error(), "secret failed") {
		t.Fatalf("err %v", err)
	}
	if src.wait(5*time.Minute) != 4*time.Minute {
		t.Fatalf("wait %s", src.wait(5*time.Minute))
	}
	if src.wait(0) != tokenFallbackTTL {
		t.Fatalf("fallback %s", src.wait(0))
	}
}

func TestTokenSourceRunRetriesWithoutReplacingToken(t *testing.T) {
	t.Parallel()
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "no", http.StatusBadGateway)
			return
		}
		w.Write([]byte(`{"access_token":"later","expires_in":60}`))
	}))
	t.Cleanup(srv.Close)
	src := &tokenSource{
		url:          srv.URL,
		clientID:     "client",
		clientSecret: "secret",
		retry:        20 * time.Millisecond,
		http:         srv.Client(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan string, 1)
	go src.run(ctx, func(token string) { got <- token }, 0, false)
	select {
	case token := <-got:
		if token != "later" {
			t.Fatalf("token %q", token)
		}
	case <-time.After(time.Second):
		t.Fatal("failed refresh did not retry")
	}
}
