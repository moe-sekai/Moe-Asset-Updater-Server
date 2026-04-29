package gitwatch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/protocol"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestParseGitHubTreeURL(t *testing.T) {
	target, err := parseGitHubTreeURL("https://github.com/Team-Haruki/haruki-sekai-sc-master/tree/main/master")
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	if target.Owner != "Team-Haruki" || target.Repo != "haruki-sekai-sc-master" || target.Branch != "main" || target.Path != "master" {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestCheckOnceBaselinesThenCreatesJobOnHashChange(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	responses := []struct {
		status int
		hash   string
		etag   string
	}{
		{status: http.StatusOK, hash: "hash-a", etag: `"etag-a"`},
		{status: http.StatusNotModified, etag: `"etag-a"`},
		{status: http.StatusOK, hash: "hash-b", etag: `"etag-b"`},
	}
	requests := 0
	created := 0

	watcher := newTestWatcher(t, stateFile, func(req *http.Request) (*http.Response, error) {
		if requests >= len(responses) {
			t.Fatalf("unexpected request %d", requests+1)
		}
		resp := responses[requests]
		requests++
		if requests == 2 && req.Header.Get("If-None-Match") != `"etag-a"` {
			t.Fatalf("expected conditional request with etag-a, got %q", req.Header.Get("If-None-Match"))
		}
		if got := req.URL.Query().Get("path"); got != "master" {
			t.Fatalf("expected path query master, got %q", got)
		}
		return githubResponse(req, resp.status, resp.hash, resp.etag), nil
	}, func(ctx context.Context, region protocol.Region, oldHash string, newHash string) (protocol.JobSnapshot, error) {
		created++
		if region != protocol.RegionCN || oldHash != "hash-a" || newHash != "hash-b" {
			t.Fatalf("unexpected change callback region=%s old=%s new=%s", region, oldHash, newHash)
		}
		return protocol.JobSnapshot{ID: "job-1"}, nil
	})

	for i := 0; i < 3; i++ {
		if err := watcher.CheckOnce(context.Background()); err != nil {
			t.Fatalf("check %d: %v", i+1, err)
		}
	}
	if created != 1 {
		t.Fatalf("expected 1 created job, got %d", created)
	}
	state := watcher.targetState(watcher.targets[0])
	if state.LastHash != "hash-b" || state.ETag != `"etag-b"` || state.LastJobID != "job-1" {
		t.Fatalf("unexpected persisted state: %+v", state)
	}
}

func TestCheckOnceDoesNotAdvanceHashWhenJobCreationFails(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	requests := 0
	watcher := newTestWatcher(t, stateFile, func(req *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 1:
			return githubResponse(req, http.StatusOK, "hash-a", `"etag-a"`), nil
		case 2, 3:
			if got := req.Header.Get("If-None-Match"); got != `"etag-a"` {
				t.Fatalf("expected retry to keep old etag after failure, got %q", got)
			}
			return githubResponse(req, http.StatusOK, "hash-b", `"etag-b"`), nil
		default:
			t.Fatalf("unexpected request %d", requests)
			return nil, nil
		}
	}, func(ctx context.Context, region protocol.Region, oldHash string, newHash string) (protocol.JobSnapshot, error) {
		return protocol.JobSnapshot{}, errors.New("boom")
	})

	if err := watcher.CheckOnce(context.Background()); err != nil {
		t.Fatalf("baseline check: %v", err)
	}
	if err := watcher.CheckOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected job creation error, got %v", err)
	}
	state := watcher.targetState(watcher.targets[0])
	if state.LastHash != "hash-a" || state.ETag != `"etag-a"` {
		t.Fatalf("failed creation should keep old hash/etag, got %+v", state)
	}
	if err := watcher.CheckOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected retry error, got %v", err)
	}
}

func newTestWatcher(t *testing.T, stateFile string, rt roundTripFunc, handler ChangeHandler) *Watcher {
	t.Helper()
	watcher, err := New(config.AssetHashWatchConfig{
		IntervalSeconds: 60,
		StateFile:       stateFile,
		Targets: []config.AssetHashWatchTargetConfig{
			{
				Region: protocol.RegionCN,
				URL:    "https://github.com/Team-Haruki/haruki-sekai-sc-master/tree/main/master",
			},
		},
	}, nil, handler)
	if err != nil {
		t.Fatalf("new watcher: %v", err)
	}
	watcher.client = &http.Client{Transport: rt}
	watcher.apiBaseURL = "https://api.github.test"
	return watcher
}

func githubResponse(req *http.Request, status int, hash string, etag string) *http.Response {
	body := ""
	if status == http.StatusOK {
		body = `[{"sha":"` + hash + `"}]`
	}
	header := make(http.Header)
	if etag != "" {
		header.Set("ETag", etag)
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func TestCommitsURL(t *testing.T) {
	target, err := parseGitHubTreeURL("https://github.com/Team-Haruki/haruki-sekai-master/tree/main/master")
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	watcher := &Watcher{apiBaseURL: "https://api.github.com"}
	got, err := watcher.commitsURL(target)
	if err != nil {
		t.Fatalf("commits url: %v", err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse commits url: %v", err)
	}
	if parsed.Path != "/repos/Team-Haruki/haruki-sekai-master/commits" {
		t.Fatalf("unexpected path: %s", parsed.Path)
	}
	query := parsed.Query()
	if query.Get("sha") != "main" || query.Get("path") != "master" || query.Get("per_page") != "1" {
		t.Fatalf("unexpected query: %s", parsed.RawQuery)
	}
}
