package oa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// traceEnvVar, when set to "1", enables slog.Debug dumps of raw response
// bodies from every Zalo API call. Off by default. Response bodies may
// contain PII (user display names, phone numbers, user IDs) — do NOT
// enable in production without scrubbing review.
const traceEnvVar = "GOCLAW_ZALO_OA_TRACE"

// traceEnabled reports whether GOCLAW_ZALO_OA_TRACE is on for this process.
// Cached at package init; flipping the env live requires restart.
var traceEnabled = os.Getenv(traceEnvVar) == "1"

// uploadTimeout is generous because multipart uploads of a few MB over a
// mobile carrier can take longer than the default 15s API timeout.
// Host bases + path constants live in endpoints.go.
const uploadTimeout = 60 * time.Second

// Client wraps Zalo's OAuth + OpenAPI hosts.
type Client struct {
	http      *http.Client
	oauthBase string
	apiBase   string
}

// NewClient returns a Client with the given timeout. Transport is tuned
// for Zalo OA's observed behavior: keep-alive reuse (default), but with
// bounded idle-connection lifetime so stale connections don't sit around
// and cause spurious "awaiting headers" timeouts on the next call.
func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second // Zalo sometimes takes 10-20s under load
	}
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     60 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &Client{
		http:      &http.Client{Timeout: timeout, Transport: transport},
		oauthBase: defaultOAuthBase,
		apiBase:   defaultAPIBase,
	}
}

// ErrRateLimit indicates Zalo returned HTTP 429. Callers should back off
// (the polling loop switches to a 30s ticker until a successful cycle).
var ErrRateLimit = errors.New("zalo_oa: rate limited")

// APIError is returned when Zalo replies with a non-zero error envelope.
type APIError struct {
	Code    int    `json:"error"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("zalo api error %d: %s", e.Code, e.Message)
}

// isAuth reports whether this error indicates an invalid/expired access
// token at the OpenAPI layer (distinct from refresh-token death — that's
// classifyRefreshError's job). Code-based check plus a substring fallback
// for documentation drift. Code values live in errors.go.
func (e *APIError) isAuth() bool {
	if e == nil {
		return false
	}
	if isAccessTokenInvalid(e.Code) {
		return true
	}
	msg := strings.ToLower(e.Message)
	return strings.Contains(msg, "access_token") && (strings.Contains(msg, "invalid") || strings.Contains(msg, "expired"))
}

// apiGet performs GET apiBase+path with extra query params merged. Token
// rides in the `access_token` HEADER (the query-param form is NOT accepted
// by Zalo OA OpenAPI in practice; live endpoints 404 on that style).
// Surfaces 429 as ErrRateLimit so callers can switch into backoff.
func (c *Client) apiGet(ctx context.Context, path string, query url.Values, accessToken string) (json.RawMessage, error) {
	if accessToken == "" {
		return nil, fmt.Errorf("zalo_oa: empty access_token for %s", path)
	}
	u := c.apiBase + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request %s: %w", path, err)
	}
	req.Header.Set("access_token", accessToken)
	return c.do(req, path)
}

// apiPost POSTs application/json to apiBase+path with the access token
// in the `access_token` HEADER. Same envelope handling as apiGet.
//
// Logging note: only `path` is included in error messages — never the full
// URL (defence-in-depth even though the token is no longer in the URL).
func (c *Client) apiPost(ctx context.Context, path string, body any, accessToken string) (json.RawMessage, error) {
	if accessToken == "" {
		return nil, fmt.Errorf("zalo_oa: empty access_token for %s", path)
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase+path, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("build request %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("access_token", accessToken)
	return c.do(req, path)
}

// apiPostMultipart uploads a single file as multipart/form-data with the
// given form fields. Token is header-carried; same convention as apiPost.
func (c *Client) apiPostMultipart(ctx context.Context, path string, fileFieldName, fileName string, fileBytes []byte, fields map[string]string, accessToken string) (json.RawMessage, error) {
	if accessToken == "" {
		return nil, fmt.Errorf("zalo_oa: empty access_token for %s", path)
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			return nil, fmt.Errorf("write field %s: %w", k, err)
		}
	}
	part, err := mw.CreateFormFile(fileFieldName, fileName)
	if err != nil {
		return nil, fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(fileBytes); err != nil {
		return nil, fmt.Errorf("write file part: %w", err)
	}
	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("close multipart: %w", err)
	}

	// Use a per-request client with the longer upload timeout instead of
	// mutating the shared client.
	uploadClient := &http.Client{Timeout: uploadTimeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase+path, &buf)
	if err != nil {
		return nil, fmt.Errorf("build upload request %s: %w", path, err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("access_token", accessToken)
	return doRequest(uploadClient, req, path)
}

// do runs req against the shared http client and parses the envelope.
func (c *Client) do(req *http.Request, path string) (json.RawMessage, error) {
	return doRequest(c.http, req, path)
}

// doRequest executes the HTTP call and parses Zalo's envelope. Path-only
// in error messages — never the full URL (token leakage).
//
// Token redaction: net/http wraps transport errors in *url.Error which
// embeds the request URL (with `?access_token=...`) in its Error() string.
// We rewrite urlErr.URL to a token-free form before bubbling the error up
// so any upstream consumer that prints the error chain doesn't leak.
func doRequest(client *http.Client, req *http.Request, path string) (json.RawMessage, error) {
	resp, err := client.Do(req)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			urlErr.URL = path // strip host + token for safe Error()
		}
		return nil, fmt.Errorf("zalo api %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if traceEnabled {
		slog.Debug("zalo_oa.raw_response", "path", path, "status", resp.StatusCode, "body", string(raw))
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("%w (path=%s)", ErrRateLimit, path)
	}
	if resp.StatusCode >= 400 {
		var env APIError
		if jerr := json.Unmarshal(raw, &env); jerr == nil && (env.Code != 0 || env.Message != "") {
			return nil, &env
		}
		return nil, fmt.Errorf("zalo api %s: http %d", path, resp.StatusCode)
	}
	var env APIError
	if jerr := json.Unmarshal(raw, &env); jerr == nil && env.Code != 0 {
		return nil, &env
	}
	return raw, nil
}

// postForm POSTs application/x-www-form-urlencoded with optional headers,
// returns the raw decoded JSON body. HTTP-status errors and Zalo's in-body
// error envelope (`error != 0`) are both surfaced as errors.
func (c *Client) postForm(ctx context.Context, fullURL string, headers map[string]string, body url.Values) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, strings.NewReader(body.Encode()))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if traceEnabled {
		slog.Debug("zalo_oa.raw_response", "path", "oauth_token", "status", resp.StatusCode, "body", string(raw))
	}

	if resp.StatusCode >= 400 {
		// Best-effort decode of envelope for context; otherwise return status.
		var env APIError
		if jerr := json.Unmarshal(raw, &env); jerr == nil && (env.Code != 0 || env.Message != "") {
			return nil, &env
		}
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}

	// Zalo returns HTTP 200 with `{"error":N,"message":"..."}` for app-level errors.
	var env APIError
	if jerr := json.Unmarshal(raw, &env); jerr == nil && env.Code != 0 {
		return nil, &env
	}
	return raw, nil
}
