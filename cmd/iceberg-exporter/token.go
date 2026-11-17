package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"iceberg-exporter/internal/nessie"
)

const (
	tokenHTTPTimeout = 15 * time.Second
	tokenRetryWait   = 15 * time.Second
	tokenFallbackTTL = 3 * time.Minute
)

// tokenSource fetches a Keycloak client-credentials bearer and keeps the
// Nessie client on that token. The process stays up across refreshes.
type tokenSource struct {
	url          string
	clientID     string
	clientSecret string
	interval     time.Duration
	retry        time.Duration
	http         *http.Client
}

func refreshNessieToken(ctx context.Context, cfg config, client *nessie.Client) error {
	if cfg.KeycloakURL == "" {
		return nil
	}
	src := newTokenSource(cfg)
	fetchCtx, cancel := context.WithTimeout(ctx, tokenHTTPTimeout)
	token, ttl, err := src.fetch(fetchCtx)
	cancel()
	fresh := err == nil
	if err != nil {
		if strings.TrimSpace(cfg.NessieToken) == "" {
			return err
		}
		slog.Error("keycloak token", "err", err)
	} else {
		client.SetToken(token)
		slog.Info("keycloak token", "expires_in", ttl.String())
	}
	go func() {
		err := src.run(ctx, client.SetToken, ttl, fresh)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("keycloak refresh stopped", "err", err)
		}
	}()
	return nil
}

func newTokenSource(cfg config) *tokenSource {
	return &tokenSource{
		url:          cfg.KeycloakURL,
		clientID:     cfg.OAuthClientID,
		clientSecret: cfg.OAuthClientSecret,
		interval:     cfg.TokenRefresh,
		http:         &http.Client{Timeout: tokenHTTPTimeout},
	}
}

type tokenResponse struct {
	AccessToken string  `json:"access_token"`
	ExpiresIn   flexInt `json:"expires_in"`
}

type flexInt int

func (n *flexInt) UnmarshalJSON(raw []byte) error {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return nil
	}
	if strings.HasPrefix(text, "\"") {
		var decoded string
		if err := json.Unmarshal([]byte(text), &decoded); err != nil {
			return err
		}
		text = strings.TrimSpace(decoded)
		if text == "" {
			return nil
		}
	}
	var number float64
	if err := json.Unmarshal([]byte(text), &number); err != nil {
		return err
	}
	*n = flexInt(number)
	return nil
}

func (s *tokenSource) fetch(ctx context.Context) (string, time.Duration, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", s.clientID)
	form.Set("client_secret", s.clientSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("keycloak: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.http.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("keycloak: %s", requestErr(err))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, fmt.Errorf("keycloak: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, fmt.Errorf("keycloak: status %d: %s", resp.StatusCode, redactSecret(snippet(body), s.clientSecret))
	}
	var payload tokenResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", 0, fmt.Errorf("keycloak: %w", err)
	}
	token := strings.TrimSpace(payload.AccessToken)
	if token == "" {
		return "", 0, fmt.Errorf("keycloak: response has no access_token")
	}
	return token, time.Duration(payload.ExpiresIn) * time.Second, nil
}

// run waits, then fetches until ctx is cancelled. fresh means the caller already
// stored a token that expires in expiresIn, so the first wait is a full interval.
// A failed fetch keeps the previous bearer and retries.
func (s *tokenSource) run(ctx context.Context, set func(string), expiresIn time.Duration, fresh bool) error {
	wait := s.retryWait()
	if fresh {
		wait = s.wait(expiresIn)
	}
	for {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		token, ttl, err := s.fetch(ctx)
		if err != nil {
			slog.Error("keycloak token", "err", err)
			wait = s.retryWait()
			continue
		}
		set(token)
		slog.Info("keycloak token refreshed", "expires_in", ttl.String())
		wait = s.wait(ttl)
	}
}

func (s *tokenSource) wait(expiresIn time.Duration) time.Duration {
	if s.interval > 0 {
		return s.interval
	}
	if expiresIn <= 0 {
		return tokenFallbackTTL
	}
	wait := expiresIn * 4 / 5
	if wait < s.retryWait() {
		wait = s.retryWait()
	}
	if wait >= expiresIn {
		wait = expiresIn / 2
	}
	if wait <= 0 {
		return s.retryWait()
	}
	return wait
}

func (s *tokenSource) retryWait() time.Duration {
	if s.retry > 0 {
		return s.retry
	}
	return tokenRetryWait
}

func redactSecret(text, secret string) string {
	if secret == "" || text == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "[redacted]")
}

func snippet(body []byte) string {
	text := strings.Join(strings.Fields(string(body)), " ")
	if len(text) > 256 {
		text = text[:256]
	}
	return text
}

func requestErr(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return ue.Err.Error()
	}
	return err.Error()
}
