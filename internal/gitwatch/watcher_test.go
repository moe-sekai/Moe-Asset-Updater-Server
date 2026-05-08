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
	"time"

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
		return protocol.JobSnapshot{ID: "job-1", Total: 1}, nil
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

func TestCheckOnceSchedulesRecheckWhenHashChangeCreatesEmptyJob(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	requests := 0
	created := 0
	watcher := newTestWatcher(t, stateFile, func(req *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 1:
			return githubResponse(req, http.StatusOK, "hash-a", `"etag-a"`), nil
		case 2:
			if got := req.Header.Get("If-None-Match"); got != `"etag-a"` {
				t.Fatalf("expected hash change request with etag-a, got %q", got)
			}
			return githubResponse(req, http.StatusOK, "hash-b", `"etag-b"`), nil
		default:
			t.Fatalf("unexpected request %d", requests)
			return nil, nil
		}
	}, func(ctx context.Context, region protocol.Region, oldHash string, newHash string) (protocol.JobSnapshot, error) {
		created++
		if oldHash != "hash-a" || newHash != "hash-b" {
			t.Fatalf("unexpected change callback old=%s new=%s", oldHash, newHash)
		}
		return protocol.JobSnapshot{ID: "job-empty", Total: 0}, nil
	})

	if err := watcher.CheckOnce(context.Background()); err != nil {
		t.Fatalf("baseline check: %v", err)
	}
	if err := watcher.CheckOnce(context.Background()); err != nil {
		t.Fatalf("hash change check: %v", err)
	}
	if created != 1 {
		t.Fatalf("expected 1 created empty job, got %d", created)
	}
	state := watcher.targetState(watcher.targets[0])
	if state.LastHash != "hash-a" {
		t.Fatalf("empty job should not advance last hash, got %s", state.LastHash)
	}
	if state.PendingHash != "hash-b" || state.PendingOldHash != "hash-a" {
		t.Fatalf("unexpected pending recheck state: %+v", state)
	}
	if state.RecheckStartedAt == nil || state.RecheckNextAt == nil || state.RecheckDeadlineAt == nil {
		t.Fatalf("expected recheck timestamps to be set: %+v", state)
	}
	if state.LastEmptyJobID != "job-empty" {
		t.Fatalf("expected empty job id to be recorded, got %q", state.LastEmptyJobID)
	}
}

func TestPendingRecheckKeepsPendingWhenJobStillEmpty(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	requests := 0
	created := 0
	watcher := newTestWatcher(t, stateFile, func(req *http.Request) (*http.Response, error) {
		requests++
		return githubResponse(req, http.StatusOK, "hash-b", `"etag-b"`), nil
	}, func(ctx context.Context, region protocol.Region, oldHash string, newHash string) (protocol.JobSnapshot, error) {
		created++
		if oldHash != "hash-a" || newHash != "hash-b" {
			t.Fatalf("unexpected recheck callback old=%s new=%s", oldHash, newHash)
		}
		return protocol.JobSnapshot{ID: "job-empty-recheck", Total: 0}, nil
	})

	now := time.Now()
	state := targetState{
		LastHash:          "hash-a",
		ETag:              `"etag-a"`,
		PendingHash:       "hash-b",
		PendingOldHash:    "hash-a",
		RecheckStartedAt:  ptrTime(now.Add(-10 * time.Minute)),
		RecheckNextAt:     ptrTime(now.Add(-time.Minute)),
		RecheckDeadlineAt: ptrTime(now.Add(time.Hour)),
		LastEmptyJobID:    "job-empty-first",
	}
	if err := watcher.storeTargetState(watcher.targets[0], state); err != nil {
		t.Fatalf("store pending state: %v", err)
	}

	if err := watcher.CheckOnce(context.Background()); err != nil {
		t.Fatalf("pending recheck: %v", err)
	}
	if requests != 1 || created != 1 {
		t.Fatalf("expected one fetch and one recheck, got requests=%d created=%d", requests, created)
	}
	state = watcher.targetState(watcher.targets[0])
	if state.LastHash != "hash-a" || state.PendingHash != "hash-b" {
		t.Fatalf("empty recheck should keep pending state: %+v", state)
	}
	if state.LastEmptyJobID != "job-empty-recheck" || state.LastJobID != "job-empty-recheck" {
		t.Fatalf("expected latest empty job id to be recorded: %+v", state)
	}
	if state.RecheckNextAt == nil || !state.RecheckNextAt.After(time.Now()) {
		t.Fatalf("expected next recheck to move forward, got %+v", state.RecheckNextAt)
	}
}

func TestPendingRecheckConfirmsHashWhenTasksAppear(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	created := 0
	watcher := newTestWatcher(t, stateFile, func(req *http.Request) (*http.Response, error) {
		return githubResponse(req, http.StatusOK, "hash-b", `"etag-b"`), nil
	}, func(ctx context.Context, region protocol.Region, oldHash string, newHash string) (protocol.JobSnapshot, error) {
		created++
		if oldHash != "hash-a" || newHash != "hash-b" {
			t.Fatalf("unexpected recheck callback old=%s new=%s", oldHash, newHash)
		}
		return protocol.JobSnapshot{ID: "job-ready", Total: 10}, nil
	})

	now := time.Now()
	state := targetState{
		LastHash:          "hash-a",
		ETag:              `"etag-a"`,
		PendingHash:       "hash-b",
		PendingOldHash:    "hash-a",
		RecheckStartedAt:  ptrTime(now.Add(-10 * time.Minute)),
		RecheckNextAt:     ptrTime(now.Add(-time.Minute)),
		RecheckDeadlineAt: ptrTime(now.Add(time.Hour)),
		LastEmptyJobID:    "job-empty-first",
	}
	if err := watcher.storeTargetState(watcher.targets[0], state); err != nil {
		t.Fatalf("store pending state: %v", err)
	}

	if err := watcher.CheckOnce(context.Background()); err != nil {
		t.Fatalf("pending recheck: %v", err)
	}
	if created != 1 {
		t.Fatalf("expected one recheck job, got %d", created)
	}
	state = watcher.targetState(watcher.targets[0])
	if state.LastHash != "hash-b" || state.ETag != `"etag-b"` || state.LastJobID != "job-ready" {
		t.Fatalf("expected hash to be confirmed, got %+v", state)
	}
	if state.PendingHash != "" || state.RecheckNextAt != nil || state.LastEmptyJobID != "" {
		t.Fatalf("expected recheck state to be cleared, got %+v", state)
	}
}

func TestPendingRecheckTimesOutAndConfirmsHash(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	created := 0
	watcher := newTestWatcher(t, stateFile, func(req *http.Request) (*http.Response, error) {
		return githubResponse(req, http.StatusOK, "hash-b", `"etag-b"`), nil
	}, func(ctx context.Context, region protocol.Region, oldHash string, newHash string) (protocol.JobSnapshot, error) {
		created++
		return protocol.JobSnapshot{ID: "unexpected", Total: 1}, nil
	})

	now := time.Now()
	state := targetState{
		LastHash:          "hash-a",
		ETag:              `"etag-a"`,
		LastJobID:         "job-empty-final",
		PendingHash:       "hash-b",
		PendingOldHash:    "hash-a",
		RecheckStartedAt:  ptrTime(now.Add(-2 * time.Hour)),
		RecheckNextAt:     ptrTime(now.Add(-time.Hour)),
		RecheckDeadlineAt: ptrTime(now.Add(-time.Minute)),
		LastEmptyJobID:    "job-empty-final",
	}
	if err := watcher.storeTargetState(watcher.targets[0], state); err != nil {
		t.Fatalf("store pending state: %v", err)
	}

	if err := watcher.CheckOnce(context.Background()); err != nil {
		t.Fatalf("pending timeout check: %v", err)
	}
	if created != 0 {
		t.Fatalf("expected timeout to confirm without creating another job, got %d", created)
	}
	state = watcher.targetState(watcher.targets[0])
	if state.LastHash != "hash-b" || state.LastJobID != "job-empty-final" {
		t.Fatalf("expected timed out pending hash to be confirmed, got %+v", state)
	}
	if state.PendingHash != "" || state.RecheckDeadlineAt != nil || state.LastEmptyJobID != "" {
		t.Fatalf("expected pending state to be cleared after timeout, got %+v", state)
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
		IntervalSeconds:        60,
		RecheckIntervalSeconds: 300,
		RecheckTimeoutSeconds:  3600,
		StateFile:              stateFile,
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
