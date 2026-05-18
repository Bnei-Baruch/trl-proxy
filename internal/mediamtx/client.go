// Package mediamtx is a minimal REST client for MediaMTX v1.x (API v3).
//
// It is used to:
//   - periodically ping the API (feeds HealthAggregator);
//   - list paths and discover "zombie" publishers before opening our own
//     egress sessions;
//   - kick those zombies via POST /v3/rtspsessions/kick/{id} and friends.
//
// The client is intentionally tolerant of unknown/new fields — only a minimal
// schema is parsed, the rest is ignored.
package mediamtx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client wraps an http.Client for the MediaMTX API v3.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient constructs a client. baseURL is the API root, e.g.
// http://mediamtx.internal:9997.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			Timeout: timeout,
		},
	}
}

// Path is a simplified model of a MediaMTX path.
type Path struct {
	Name   string     `json:"name"`
	Source PathSource `json:"source"`
}

// PathSource describes the current publisher of a path. Type values include
// "rtspSession", "rtmpConn", "webRTCSession", "hlsSource", "udpSource", etc.
// ID is the session identifier passed to the corresponding kick endpoint.
type PathSource struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type pathsList struct {
	Items []Path `json:"items"`
}

// Ping performs a lightweight GET against the API and returns nil on 200.
// We use /v3/paths/list because it's always available and warms our cache.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.ListPaths(ctx)
	return err
}

// ListPaths returns all paths known to MediaMTX.
func (c *Client) ListPaths(ctx context.Context) ([]Path, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v3/paths/list", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mediamtx list paths: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("mediamtx list paths: http %d: %s", resp.StatusCode, string(body))
	}
	var pl pathsList
	if err := json.NewDecoder(resp.Body).Decode(&pl); err != nil {
		return nil, fmt.Errorf("mediamtx list paths: decode: %w", err)
	}
	return pl.Items, nil
}

// FindPublisher returns the source on the given path if one exists.
// If the path is missing or has no source, returns (PathSource{}, false, nil).
func (c *Client) FindPublisher(ctx context.Context, pathName string) (PathSource, bool, error) {
	paths, err := c.ListPaths(ctx)
	if err != nil {
		return PathSource{}, false, err
	}
	for _, p := range paths {
		if p.Name == pathName {
			if p.Source.ID == "" || p.Source.Type == "" {
				return PathSource{}, false, nil
			}
			return p.Source, true, nil
		}
	}
	return PathSource{}, false, nil
}

// Kick disconnects a publisher by its type+ID.
// 404 is treated as success (the session was already gone — normal race).
func (c *Client) Kick(ctx context.Context, src PathSource) error {
	endpoint, ok := kickEndpoint(src.Type)
	if !ok {
		return fmt.Errorf("mediamtx kick: unsupported source type %q", src.Type)
	}
	url := fmt.Sprintf("%s/%s/%s", c.baseURL, endpoint, src.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("mediamtx kick %s: %w", src.Type, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return nil
	case http.StatusNotFound:
		return nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("mediamtx kick %s %s: http %d: %s", src.Type, src.ID, resp.StatusCode, string(body))
	}
}

// KickAllOnPaths finds the publisher on each path and kicks it.
// Errors do not stop the loop — they are joined via errors.Join.
// Paths without a publisher are silently skipped.
func (c *Client) KickAllOnPaths(ctx context.Context, pathNames []string) error {
	paths, err := c.ListPaths(ctx)
	if err != nil {
		return fmt.Errorf("list paths before kick: %w", err)
	}
	index := make(map[string]PathSource, len(paths))
	for _, p := range paths {
		index[p.Name] = p.Source
	}

	var errs []error
	for _, name := range pathNames {
		src, ok := index[name]
		if !ok || src.ID == "" {
			continue
		}
		if err := c.Kick(ctx, src); err != nil {
			errs = append(errs, fmt.Errorf("path %s (%s/%s): %w", name, src.Type, src.ID, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// kickEndpoint maps a MediaMTX source type to its kick API endpoint.
// Returns a relative path without a leading slash.
func kickEndpoint(srcType string) (string, bool) {
	switch srcType {
	case "rtspSession":
		return "v3/rtspsessions/kick", true
	case "rtspsSession":
		return "v3/rtspssessions/kick", true
	case "rtmpConn":
		return "v3/rtmpconns/kick", true
	case "webRTCSession":
		return "v3/webrtcsessions/kick", true
	case "srtConn":
		return "v3/srtconns/kick", true
	case "hlsMuxer", "hlsSource", "udpSource", "rpiCameraSource":
		// These sources cannot be kicked via REST — they are static
		// configuration sources, not connected clients. Skip silently.
		return "", false
	default:
		return "", false
	}
}
