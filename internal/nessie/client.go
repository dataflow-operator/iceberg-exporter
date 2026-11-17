// Package nessie is the Nessie API v2 catalog adapter.
//
// Revision is one GET /api/v2/trees/{ref}. The commit hash is the revision
// token, so an unchanged hash does not list entries. Diff pages
// GET /api/v2/trees/{ref}@{from}/diff/{ref}@{to}. ListTables pages entries with
// content=true and a CEL filter for Iceberg tables and views, which returns
// metadata locations without a per-key contents call. History pages the commit
// log until a previously seen hash and reports the HEAD commit time and how
// many commits are newer. Only configured refs are walked. Nessie JVM metrics
// are not read.
package nessie

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"iceberg-exporter/internal/catalog"
)

var _ catalog.Historian = (*Client)(nil)

const (
	catalogType = "nessie"

	defaultHistoryMax = 100
	maxPages          = 10_000
	maxBodyBytes      = 32 << 20
	errorSnippetBytes = 512
)

// Config is the native Nessie catalog. Warehouse storage is not part of this
// client: Nessie returns metadata locations and the exporter reads those itself.
// Token is the raw bearer secret. BaseURL must not carry userinfo or a query.
type Config struct {
	// Name is the Prometheus catalog label.
	Name string
	// BaseURL is the Nessie server origin, with or without a /api/v2 suffix.
	BaseURL string
	// Token is sent as Authorization: Bearer. Empty means no header.
	Token string
	// Refs are the only branches and tags ListRefs returns. An empty kind is a branch.
	Refs []catalog.Ref
	// BaseRef is the MultiRef baseline. Empty means this catalog has no base ref.
	// When set, it must be one of Refs.
	BaseRef string
	// PageSize is the default max-records for entries and diff. Zero omits the parameter.
	PageSize int
	// HistoryMaxRecords is max-records for the commit log. Zero uses defaultHistoryMax.
	HistoryMaxRecords int
	// HTTPClient is optional. A nil client uses a 30s timeout and drops
	// Authorization when a redirect leaves the original host.
	HTTPClient *http.Client
}

// Client talks to one Nessie API v2 server.
// It implements catalog.Catalog, catalog.Revisioned, catalog.Diffable,
// catalog.MultiRef, and catalog.Historian.
type Client struct {
	name       string
	baseURL    string
	token      atomic.Pointer[string]
	refs       []catalog.Ref
	baseRef    catalog.Ref
	hasBase    bool
	pageSize   int
	historyMax int
	http       *http.Client
}

// History is one walk of a ref's commit log.
// It is catalog.CommitLog, so Client implements catalog.Historian.
type History = catalog.CommitLog

// New validates cfg and builds a client. The returned client is safe for
// concurrent Catalog calls.
func New(cfg Config) (*Client, error) {
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		return nil, errors.New("nessie: empty catalog name")
	}
	if cfg.PageSize < 0 || cfg.HistoryMaxRecords < 0 {
		return nil, errors.New("nessie: page size must not be negative")
	}
	baseURL, err := normalizeBaseURL(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	refs, baseRef, hasBase, err := normalizeRefs(cfg.Refs, cfg.BaseRef)
	if err != nil {
		return nil, err
	}
	historyMax := cfg.HistoryMaxRecords
	if historyMax == 0 {
		historyMax = defaultHistoryMax
	}
	client := &Client{
		name:       name,
		baseURL:    baseURL,
		refs:       refs,
		baseRef:    baseRef,
		hasBase:    hasBase,
		pageSize:   cfg.PageSize,
		historyMax: historyMax,
		http:       httpClient(cfg.HTTPClient),
	}
	client.SetToken(cfg.Token)
	return client, nil
}

// SetToken replaces the bearer used by later requests.
// An empty token removes the Authorization header. The catalog walk keeps running.
func (c *Client) SetToken(token string) {
	token = strings.TrimSpace(token)
	c.token.Store(&token)
}

func (c *Client) bearer() string {
	token := c.token.Load()
	if token == nil {
		return ""
	}
	return *token
}

func (c *Client) Name() string { return c.name }

func (c *Client) Type() string { return catalogType }

// Probe checks GET /api/v2/config.
func (c *Client) Probe(ctx context.Context) error {
	return c.getJSON(ctx, "/config", nil, nil)
}

// ListRefs returns the configured refs and does not call the server.
func (c *Client) ListRefs(ctx context.Context) ([]catalog.Ref, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]catalog.Ref, len(c.refs))
	copy(out, c.refs)
	return out, nil
}

// BaseRef reports the configured baseline. The bool is false when BaseRef was empty.
func (c *Client) BaseRef() (catalog.Ref, bool) {
	if !c.hasBase {
		return catalog.Ref{}, false
	}
	return c.baseRef, true
}

// Revision returns the commit hash from GET /api/v2/trees/{ref}.
// It does not read the commit log. Commit time is available from History.
func (c *Client) Revision(ctx context.Context, ref catalog.Ref) (catalog.Revision, error) {
	if err := requireRef(ref); err != nil {
		return catalog.Revision{}, err
	}
	path := "/trees/" + refPath(ref.Name, "")
	var body referenceEnvelope
	if err := c.getJSON(ctx, path, nil, &body); err != nil {
		return catalog.Revision{}, err
	}
	refDoc := body.ref()
	if refDoc.Hash == "" {
		return catalog.Revision{}, fmt.Errorf("nessie: %s: empty hash", path)
	}
	return catalog.Revision{Token: refDoc.Hash}, nil
}

// History pages GET /api/v2/trees/{ref}/history until knownHash.
// An empty knownHash returns the HEAD commit from the first log entry and does
// not follow the page token. NewCommits is then zero.
func (c *Client) History(ctx context.Context, ref catalog.Ref, knownHash string) (History, error) {
	if err := requireRef(ref); err != nil {
		return History{}, err
	}
	path := "/trees/" + refPath(ref.Name, "") + "/history"
	seen := make(map[string]struct{})
	var (
		out     History
		gotHead bool
		token   string
		count   int
	)
	for pageN := 0; pageN < maxPages; pageN++ {
		if err := ctx.Err(); err != nil {
			return History{}, err
		}
		q := url.Values{}
		q.Set("max-records", strconv.Itoa(c.historyMax))
		if token != "" {
			q.Set("page-token", token)
		}
		var resp logResponse
		if err := c.getJSON(ctx, path, q, &resp); err != nil {
			return History{}, err
		}
		if len(resp.LogEntries) == 0 && !gotHead {
			return History{}, fmt.Errorf("nessie: %s: empty commit log", path)
		}
		for _, entry := range resp.LogEntries {
			hash := entry.CommitMeta.Hash
			if !gotHead {
				commitTime, err := parseCommitTime(entry.CommitMeta.CommitTime)
				if err != nil {
					return History{}, fmt.Errorf("nessie: %s: commit time: %w", path, err)
				}
				if hash == "" {
					return History{}, fmt.Errorf("nessie: %s: empty head hash", path)
				}
				out.Head = catalog.Revision{Token: hash, CommitTime: commitTime}
				gotHead = true
			}
			if knownHash == "" {
				return out, nil
			}
			if hash == knownHash {
				out.NewCommits = count
				out.KnownReached = true
				return out, nil
			}
			count++
		}
		if !resp.HasMore {
			out.NewCommits = count
			return out, nil
		}
		next, err := nextToken(path, token, resp.Token, seen)
		if err != nil {
			return History{}, err
		}
		token = next
	}
	return History{}, fmt.Errorf("nessie: %s: page limit exceeded", path)
}

// Diff pages GET /api/v2/trees/{ref}@{from}/diff/{ref}@{to} and returns Iceberg
// table and view changes. A rename, which Nessie reports as a delete plus an
// add of the same content id, is one upsert so the catalog can keep the
// previous pointer.
func (c *Client) Diff(ctx context.Context, ref catalog.Ref, from, to catalog.Revision) ([]catalog.Change, error) {
	if err := requireRef(ref); err != nil {
		return nil, err
	}
	if from.Token == "" || to.Token == "" {
		return nil, fmt.Errorf("nessie: diff ref %q: empty revision token", ref.Name)
	}
	if from.SameToken(to) {
		return nil, nil
	}
	path := "/trees/" + refPath(ref.Name, from.Token) + "/diff/" + refPath(ref.Name, to.Token)
	seen := make(map[string]struct{})
	var token string
	var diffs []diffEntryJSON
	for pageN := 0; pageN < maxPages; pageN++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		q := url.Values{}
		if c.pageSize > 0 {
			q.Set("max-records", strconv.Itoa(c.pageSize))
		}
		if token != "" {
			q.Set("page-token", token)
		}
		var resp diffResponse
		if err := c.getJSON(ctx, path, q, &resp); err != nil {
			return nil, err
		}
		diffs = append(diffs, resp.Diffs...)
		if !resp.HasMore {
			return changesFrom(diffs)
		}
		next, err := nextToken(path, token, resp.Token, seen)
		if err != nil {
			return nil, err
		}
		token = next
	}
	return nil, fmt.Errorf("nessie: %s: page limit exceeded", path)
}

// ListTables returns one page of Iceberg tables and views.
// The request is GET /api/v2/trees/{ref}/entries?content=true with a CEL filter.
// Namespaces from opts are pushed into that filter and applied again locally.
func (c *Client) ListTables(ctx context.Context, ref catalog.Ref, opts catalog.ListOptions) (catalog.TablePage, error) {
	if err := ctx.Err(); err != nil {
		return catalog.TablePage{}, err
	}
	if err := requireRef(ref); err != nil {
		return catalog.TablePage{}, err
	}
	if opts.PageSize < 0 {
		return catalog.TablePage{}, errors.New("nessie: page size must not be negative")
	}
	q := url.Values{}
	q.Set("content", "true")
	q.Set("filter", entriesFilter(opts.Namespaces))
	if size := chosenPageSize(opts.PageSize, c.pageSize); size > 0 {
		q.Set("max-records", strconv.Itoa(size))
	}
	if opts.PageToken != "" {
		q.Set("page-token", opts.PageToken)
	}
	path := "/trees/" + refPath(ref.Name, "") + "/entries"
	var resp entriesResponse
	if err := c.getJSON(ctx, path, q, &resp); err != nil {
		return catalog.TablePage{}, err
	}
	tables := make([]catalog.TablePointer, 0, len(resp.Entries))
	for _, entry := range resp.Entries {
		p, ok, err := decodeContent(entry.Name.Elements, entry.Type, entry.ContentID, entry.Content)
		if err != nil {
			return catalog.TablePage{}, err
		}
		if !ok || !opts.Namespaces.Allows(p.Namespace) {
			continue
		}
		tables = append(tables, p)
	}
	page := catalog.TablePage{Tables: tables}
	if !resp.HasMore {
		return page, nil
	}
	if resp.Token == "" || resp.Token == opts.PageToken {
		return catalog.TablePage{}, fmt.Errorf("nessie: %s: invalid page token", path)
	}
	page.NextPageToken = resp.Token
	return page, nil
}

func (c *Client) getJSON(ctx context.Context, path string, query url.Values, dest any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	endpoint := c.baseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("nessie: %s: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")
	token := c.bearer()
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("nessie: %s: %s", path, requestErr(err))
	}
	defer resp.Body.Close()
	body, err := readLimited(resp.Body, maxBodyBytes)
	if err != nil {
		return fmt.Errorf("nessie: %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &statusError{path: path, status: resp.StatusCode, body: redact(snippet(body), token)}
	}
	if dest == nil {
		return nil
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return fmt.Errorf("nessie: %s: empty response", path)
	}
	if err := unmarshalJSON(body, dest); err != nil {
		return fmt.Errorf("nessie: %s: %w", path, err)
	}
	return nil
}

func chosenPageSize(request, fallback int) int {
	if request > 0 {
		return request
	}
	return fallback
}

func requireRef(ref catalog.Ref) error {
	if strings.TrimSpace(ref.Name) == "" {
		return errors.New("nessie: empty ref name")
	}
	return nil
}

func nextToken(path, current, next string, seen map[string]struct{}) (string, error) {
	if next == "" || next == current {
		return "", fmt.Errorf("nessie: %s: invalid page token", path)
	}
	if _, ok := seen[next]; ok {
		return "", fmt.Errorf("nessie: %s: repeated page token %q", path, next)
	}
	seen[next] = struct{}{}
	return next, nil
}

func normalizeBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("nessie: empty base URL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("nessie: base URL is invalid")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("nessie: base URL scheme must be http or https")
	}
	if u.Host == "" || u.Opaque != "" {
		return "", errors.New("nessie: base URL host is empty")
	}
	if u.User != nil {
		return "", errors.New("nessie: base URL must not contain userinfo")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("nessie: base URL must not contain a query or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(u.Path, "/api/v2") {
		u.Path += "/api/v2"
	}
	return u.String(), nil
}

func normalizeRefs(in []catalog.Ref, base string) ([]catalog.Ref, catalog.Ref, bool, error) {
	if len(in) == 0 {
		return nil, catalog.Ref{}, false, errors.New("nessie: no refs configured")
	}
	seen := make(map[string]struct{}, len(in))
	refs := make([]catalog.Ref, 0, len(in))
	for _, ref := range in {
		ref.Name = strings.TrimSpace(ref.Name)
		if ref.Name == "" {
			return nil, catalog.Ref{}, false, errors.New("nessie: empty ref name")
		}
		switch ref.Kind {
		case "":
			ref.Kind = catalog.RefBranch
		case catalog.RefBranch, catalog.RefTag:
		default:
			return nil, catalog.Ref{}, false, fmt.Errorf("nessie: ref %q: kind %q is not a branch or tag", ref.Name, ref.Kind)
		}
		if _, ok := seen[ref.Name]; ok {
			return nil, catalog.Ref{}, false, fmt.Errorf("nessie: duplicate ref %q", ref.Name)
		}
		seen[ref.Name] = struct{}{}
		refs = append(refs, ref)
	}
	base = strings.TrimSpace(base)
	if base == "" {
		return refs, catalog.Ref{}, false, nil
	}
	for _, ref := range refs {
		if ref.Name == base {
			return refs, ref, true, nil
		}
	}
	return nil, catalog.Ref{}, false, fmt.Errorf("nessie: base ref %q is not in the configured refs", base)
}

func refPath(name, hash string) string {
	escaped := escapePathSegment(name)
	if hash == "" {
		return escaped
	}
	return escaped + "@" + escapePathSegment(hash)
}

func escapePathSegment(s string) string {
	return strings.ReplaceAll(url.PathEscape(s), "@", "%40")
}

func httpClient(c *http.Client) *http.Client {
	if c == nil {
		c = &http.Client{Timeout: 30 * time.Second}
	} else {
		clone := *c
		c = &clone
	}
	if c.CheckRedirect == nil {
		c.CheckRedirect = dropAuthOnCrossHost
	}
	return c
}

func dropAuthOnCrossHost(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("nessie: too many redirects")
	}
	if len(via) > 0 && !sameHost(req.URL, via[0].URL) {
		req.Header.Del("Authorization")
	}
	return nil
}

func sameHost(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Host, b.Host)
}

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	buf, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > limit {
		return nil, fmt.Errorf("response exceeds %d bytes", limit)
	}
	return buf, nil
}

func snippet(body []byte) string {
	text := strings.Join(strings.Fields(string(body)), " ")
	if len(text) > errorSnippetBytes {
		text = text[:errorSnippetBytes]
	}
	return text
}

func redact(text, secret string) string {
	if secret == "" || text == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "[redacted]")
}

func requestErr(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return ue.Err.Error()
	}
	return err.Error()
}

func parseCommitTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

type statusError struct {
	path   string
	status int
	body   string
}

func (e *statusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("nessie: %s: status %d", e.path, e.status)
	}
	return fmt.Sprintf("nessie: %s: status %d: %s", e.path, e.status, e.body)
}

var (
	_ catalog.Catalog    = (*Client)(nil)
	_ catalog.Revisioned = (*Client)(nil)
	_ catalog.Diffable   = (*Client)(nil)
	_ catalog.MultiRef   = (*Client)(nil)
)
