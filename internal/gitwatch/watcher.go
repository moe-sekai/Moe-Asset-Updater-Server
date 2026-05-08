package gitwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/protocol"
)

const (
	defaultAPIBaseURL             = "https://api.github.com"
	defaultStateFile              = "./data/github_asset_hash_watch.json"
	defaultRecheckIntervalSeconds = 300
	defaultRecheckTimeoutSeconds  = 3600
	minimumRecheckIntervalSeconds = 1
	minimumRecheckTimeoutSeconds  = 1
)

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type ChangeHandler func(ctx context.Context, region protocol.Region, oldHash string, newHash string) (protocol.JobSnapshot, error)

type Watcher struct {
	cfg             config.AssetHashWatchConfig
	logger          Logger
	onChange        ChangeHandler
	client          *http.Client
	apiBaseURL      string
	interval        time.Duration
	recheckInterval time.Duration
	recheckTimeout  time.Duration
	targets         []target

	mu    sync.Mutex
	state watchState
}

type target struct {
	Region protocol.Region
	RawURL string
	Owner  string
	Repo   string
	Branch string
	Path   string
	Key    string
}

type watchState struct {
	Targets map[string]targetState `json:"targets"`
}

type targetState struct {
	Region            protocol.Region `json:"region"`
	URL               string          `json:"url"`
	LastHash          string          `json:"last_hash"`
	ETag              string          `json:"etag,omitempty"`
	LastCheckedAt     time.Time       `json:"last_checked_at"`
	LastJobID         string          `json:"last_job_id,omitempty"`
	LastError         string          `json:"last_error,omitempty"`
	PendingHash       string          `json:"pending_hash,omitempty"`
	PendingOldHash    string          `json:"pending_old_hash,omitempty"`
	RecheckStartedAt  *time.Time      `json:"recheck_started_at,omitempty"`
	RecheckNextAt     *time.Time      `json:"recheck_next_at,omitempty"`
	RecheckDeadlineAt *time.Time      `json:"recheck_deadline_at,omitempty"`
	LastEmptyJobID    string          `json:"last_empty_job_id,omitempty"`
}

type fetchResult struct {
	Hash        string
	ETag        string
	NotModified bool
}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...interface{}) {}
func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Warnf(string, ...interface{})  {}
func (noopLogger) Errorf(string, ...interface{}) {}

func New(cfg config.AssetHashWatchConfig, logger Logger, onChange ChangeHandler) (*Watcher, error) {
	if onChange == nil {
		return nil, errors.New("gitwatch change handler is required")
	}
	if logger == nil {
		logger = noopLogger{}
	}
	if cfg.IntervalSeconds <= 0 {
		cfg.IntervalSeconds = 60
	}
	if cfg.RecheckEnabled == nil {
		enabled := true
		cfg.RecheckEnabled = &enabled
	}
	if cfg.RecheckIntervalSeconds <= 0 {
		cfg.RecheckIntervalSeconds = defaultRecheckIntervalSeconds
	}
	if cfg.RecheckIntervalSeconds < minimumRecheckIntervalSeconds {
		cfg.RecheckIntervalSeconds = minimumRecheckIntervalSeconds
	}
	if cfg.RecheckTimeoutSeconds <= 0 {
		cfg.RecheckTimeoutSeconds = defaultRecheckTimeoutSeconds
	}
	if cfg.RecheckTimeoutSeconds < minimumRecheckTimeoutSeconds {
		cfg.RecheckTimeoutSeconds = minimumRecheckTimeoutSeconds
	}
	if cfg.StateFile == "" {
		cfg.StateFile = defaultStateFile
	}

	targets := make([]target, 0, len(cfg.Targets))
	seen := make(map[string]bool, len(cfg.Targets))
	for _, item := range cfg.Targets {
		parsed, err := parseGitHubTreeURL(item.URL)
		if err != nil {
			return nil, fmt.Errorf("parse github target %q: %w", item.URL, err)
		}
		parsed.Region = item.Region
		parsed.RawURL = item.URL
		parsed.Key = targetKey(parsed)
		if seen[parsed.Key] {
			continue
		}
		seen[parsed.Key] = true
		targets = append(targets, parsed)
	}

	w := &Watcher{
		cfg:             cfg,
		logger:          logger,
		onChange:        onChange,
		client:          &http.Client{Timeout: 30 * time.Second},
		apiBaseURL:      defaultAPIBaseURL,
		interval:        time.Duration(cfg.IntervalSeconds) * time.Second,
		recheckInterval: time.Duration(cfg.RecheckIntervalSeconds) * time.Second,
		recheckTimeout:  time.Duration(cfg.RecheckTimeoutSeconds) * time.Second,
		targets:         targets,
		state:           watchState{Targets: map[string]targetState{}},
	}
	if err := w.loadState(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Watcher) Start(ctx context.Context) {
	go w.Run(ctx)
}

func (w *Watcher) Run(ctx context.Context) {
	if len(w.targets) == 0 {
		w.logger.Warnf("GitHub哈希监控没有可用目标，已跳过")
		return
	}
	w.logger.Infof("GitHub哈希监控启动 interval=%s targets=%d", w.interval, len(w.targets))
	if err := w.CheckOnce(ctx); err != nil {
		w.logger.Warnf("GitHub哈希监控首次检查失败: %v", err)
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.logger.Infof("GitHub哈希监控停止: %v", ctx.Err())
			return
		case <-ticker.C:
			if err := w.CheckOnce(ctx); err != nil {
				w.logger.Warnf("GitHub哈希监控本轮检查失败: %v", err)
			}
		}
	}
}

func (w *Watcher) CheckOnce(ctx context.Context) error {
	var errs []error
	for _, target := range w.targets {
		if err := w.checkTarget(ctx, target); err != nil {
			wrapped := fmt.Errorf("%s: %w", target.Description(), err)
			errs = append(errs, wrapped)
			w.logger.Errorf("GitHub哈希监控目标检查失败 %s: %v", target.Description(), err)
		}
	}
	return errors.Join(errs...)
}

func (w *Watcher) checkTarget(ctx context.Context, target target) error {
	state := w.targetState(target)
	requestETag := state.ETag
	if state.LastHash == "" || state.PendingHash != "" {
		requestETag = ""
	}

	result, err := w.fetchLatestHash(ctx, target, requestETag)
	now := time.Now()
	if err != nil {
		state.LastCheckedAt = now
		state.LastError = err.Error()
		_ = w.storeTargetState(target, state)
		return err
	}

	if result.NotModified {
		state.LastCheckedAt = now
		state.LastError = ""
		if result.ETag != "" {
			state.ETag = result.ETag
		}
		if err := w.storeTargetState(target, state); err != nil {
			return err
		}
		w.logger.Debugf("GitHub哈希未变化 region=%s target=%s hash=%s", target.Region, target.Description(), state.LastHash)
		return nil
	}

	if result.Hash == "" {
		return errors.New("github commits response did not contain sha")
	}
	if state.PendingHash != "" && state.PendingHash == result.Hash {
		return w.handlePendingRecheck(ctx, target, state, result, now)
	}
	if state.LastHash == "" {
		state.LastHash = result.Hash
		state.ETag = result.ETag
		state.LastCheckedAt = now
		state.LastError = ""
		state.clearRecheck()
		if err := w.storeTargetState(target, state); err != nil {
			return err
		}
		w.logger.Infof("GitHub哈希监控 baseline region=%s target=%s hash=%s", target.Region, target.Description(), result.Hash)
		return nil
	}
	if state.LastHash == result.Hash {
		state.ETag = result.ETag
		state.LastCheckedAt = now
		state.LastError = ""
		state.clearRecheck()
		if err := w.storeTargetState(target, state); err != nil {
			return err
		}
		w.logger.Debugf("GitHub哈希未变化 region=%s target=%s hash=%s", target.Region, target.Description(), result.Hash)
		return nil
	}

	return w.handleHashChange(ctx, target, state, result, now)
}

func (w *Watcher) handleHashChange(ctx context.Context, target target, state targetState, result fetchResult, now time.Time) error {
	oldHash := state.LastHash
	w.logger.Infof("GitHub哈希变化 region=%s target=%s old=%s new=%s", target.Region, target.Description(), oldHash, result.Hash)
	job, err := w.onChange(ctx, target.Region, oldHash, result.Hash)
	if err != nil {
		state.LastCheckedAt = now
		state.LastError = err.Error()
		if saveErr := w.storeTargetState(target, state); saveErr != nil {
			return fmt.Errorf("create job after hash change: %w; additionally save state: %v", err, saveErr)
		}
		return fmt.Errorf("create job after hash change: %w", err)
	}
	if job.Total == 0 && w.recheckEnabled() {
		state = w.scheduleRecheck(state, oldHash, result.Hash, job.ID, now)
		state.LastCheckedAt = now
		state.LastError = ""
		if err := w.storeTargetState(target, state); err != nil {
			return err
		}
		w.logger.Warnf("GitHub哈希变化创建了空任务队列，资源可能未就绪，已安排重检查 region=%s job=%s old=%s new=%s next=%s deadline=%s", target.Region, job.ID, oldHash, result.Hash, formatTime(state.RecheckNextAt), formatTime(state.RecheckDeadlineAt))
		return nil
	}
	if err := w.confirmHashChange(target, state, result, now, job.ID); err != nil {
		return err
	}
	w.logger.Infof("GitHub哈希变化已创建任务队列 region=%s job=%s old=%s new=%s tasks=%d", target.Region, job.ID, oldHash, result.Hash, job.Total)
	return nil
}

func (w *Watcher) handlePendingRecheck(ctx context.Context, target target, state targetState, result fetchResult, now time.Time) error {
	if w.recheckDeadlineExceeded(state, now) {
		lastEmptyJobID := state.LastEmptyJobID
		pendingHash := state.PendingHash
		oldHash := state.PendingOldHash
		if err := w.confirmHashChange(target, state, result, now, state.LastJobID); err != nil {
			return err
		}
		w.logger.Warnf("GitHub哈希变化重检查超时，未在重检查窗口内发现新资源，已确认哈希 region=%s timeout=%s last_empty_job=%s old=%s new=%s", target.Region, w.recheckTimeout, lastEmptyJobID, oldHash, pendingHash)
		return nil
	}
	if state.RecheckNextAt != nil && now.Before(*state.RecheckNextAt) {
		state.LastCheckedAt = now
		state.LastError = ""
		if result.ETag != "" {
			state.ETag = result.ETag
		}
		if err := w.storeTargetState(target, state); err != nil {
			return err
		}
		w.logger.Debugf("GitHub哈希变化等待重检查 region=%s target=%s hash=%s next=%s", target.Region, target.Description(), state.PendingHash, state.RecheckNextAt.Format(time.RFC3339))
		return nil
	}

	oldHash := state.PendingOldHash
	if oldHash == "" {
		oldHash = state.LastHash
	}
	w.logger.Infof("GitHub哈希变化重检查 region=%s target=%s old=%s new=%s", target.Region, target.Description(), oldHash, state.PendingHash)
	job, err := w.onChange(ctx, target.Region, oldHash, state.PendingHash)
	if err != nil {
		state.LastCheckedAt = now
		state.LastError = err.Error()
		state.RecheckNextAt = ptrTime(nextRecheckAt(now, w.recheckInterval, state.RecheckDeadlineAt))
		if saveErr := w.storeTargetState(target, state); saveErr != nil {
			return fmt.Errorf("recheck job after hash change: %w; additionally save state: %v", err, saveErr)
		}
		return fmt.Errorf("recheck job after hash change: %w", err)
	}
	if job.Total == 0 {
		state.LastCheckedAt = now
		state.LastJobID = job.ID
		state.LastEmptyJobID = job.ID
		state.LastError = ""
		state.RecheckNextAt = ptrTime(nextRecheckAt(now, w.recheckInterval, state.RecheckDeadlineAt))
		if result.ETag != "" {
			state.ETag = result.ETag
		}
		if err := w.storeTargetState(target, state); err != nil {
			return err
		}
		w.logger.Warnf("GitHub哈希变化重检查仍未发现新资源，继续等待 region=%s job=%s old=%s new=%s next=%s deadline=%s", target.Region, job.ID, oldHash, state.PendingHash, formatTime(state.RecheckNextAt), formatTime(state.RecheckDeadlineAt))
		return nil
	}
	if err := w.confirmHashChange(target, state, result, now, job.ID); err != nil {
		return err
	}
	w.logger.Infof("GitHub哈希变化重检查已创建任务队列 region=%s job=%s old=%s new=%s tasks=%d", target.Region, job.ID, oldHash, state.PendingHash, job.Total)
	return nil
}

func (w *Watcher) scheduleRecheck(state targetState, oldHash string, pendingHash string, jobID string, now time.Time) targetState {
	if state.PendingHash != pendingHash || state.RecheckStartedAt == nil || state.RecheckDeadlineAt == nil {
		startedAt := now
		deadlineAt := now.Add(w.recheckTimeout)
		state.RecheckStartedAt = &startedAt
		state.RecheckDeadlineAt = &deadlineAt
	}
	nextAt := nextRecheckAt(now, w.recheckInterval, state.RecheckDeadlineAt)
	state.PendingHash = pendingHash
	state.PendingOldHash = oldHash
	state.RecheckNextAt = &nextAt
	state.LastJobID = jobID
	state.LastEmptyJobID = jobID
	return state
}

func (w *Watcher) confirmHashChange(target target, state targetState, result fetchResult, now time.Time, jobID string) error {
	state.LastHash = result.Hash
	state.ETag = result.ETag
	state.LastCheckedAt = now
	state.LastJobID = jobID
	state.LastError = ""
	state.clearRecheck()
	return w.storeTargetState(target, state)
}

func (w *Watcher) recheckEnabled() bool {
	return w.cfg.RecheckEnabled == nil || *w.cfg.RecheckEnabled
}

func (w *Watcher) recheckDeadlineExceeded(state targetState, now time.Time) bool {
	return state.RecheckDeadlineAt != nil && !now.Before(*state.RecheckDeadlineAt)
}

func (s *targetState) clearRecheck() {
	s.PendingHash = ""
	s.PendingOldHash = ""
	s.RecheckStartedAt = nil
	s.RecheckNextAt = nil
	s.RecheckDeadlineAt = nil
	s.LastEmptyJobID = ""
}

func nextRecheckAt(now time.Time, interval time.Duration, deadline *time.Time) time.Time {
	if interval <= 0 {
		interval = time.Duration(defaultRecheckIntervalSeconds) * time.Second
	}
	next := now.Add(interval)
	if deadline != nil && next.After(*deadline) {
		return *deadline
	}
	return next
}

func ptrTime(value time.Time) *time.Time {
	return &value
}

func formatTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.Format(time.RFC3339)
}

func (w *Watcher) fetchLatestHash(ctx context.Context, target target, etag string) (fetchResult, error) {
	apiURL, err := w.commitsURL(target)
	if err != nil {
		return fetchResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return fetchResult{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "Moe-Asset-Updater-Server")
	if token := strings.TrimSpace(w.cfg.GitHubToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return fetchResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	result := fetchResult{ETag: resp.Header.Get("ETag")}
	if resp.StatusCode == http.StatusNotModified {
		result.NotModified = true
		return result, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = resp.Status
		}
		return fetchResult{}, fmt.Errorf("github commits API returned %d: %s", resp.StatusCode, message)
	}

	var commits []struct {
		SHA string `json:"sha"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&commits); err != nil {
		return fetchResult{}, fmt.Errorf("decode github commits response: %w", err)
	}
	if len(commits) == 0 || strings.TrimSpace(commits[0].SHA) == "" {
		return fetchResult{}, errors.New("github commits response did not contain sha")
	}
	result.Hash = strings.TrimSpace(commits[0].SHA)
	return result, nil
}

func (w *Watcher) commitsURL(target target) (string, error) {
	base, err := url.Parse(w.apiBaseURL)
	if err != nil {
		return "", err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/repos/" + url.PathEscape(target.Owner) + "/" + url.PathEscape(target.Repo) + "/commits"
	query := base.Query()
	query.Set("sha", target.Branch)
	query.Set("per_page", "1")
	if target.Path != "" {
		query.Set("path", target.Path)
	}
	base.RawQuery = query.Encode()
	return base.String(), nil
}

func (w *Watcher) targetState(target target) targetState {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state.Targets == nil {
		w.state.Targets = map[string]targetState{}
	}
	return w.state.Targets[target.Key]
}

func (w *Watcher) storeTargetState(target target, state targetState) error {
	state.Region = target.Region
	state.URL = target.RawURL

	w.mu.Lock()
	if w.state.Targets == nil {
		w.state.Targets = map[string]targetState{}
	}
	w.state.Targets[target.Key] = state
	snapshot := cloneWatchState(w.state)
	w.mu.Unlock()

	return saveState(w.cfg.StateFile, snapshot)
}

func (w *Watcher) loadState() error {
	state, err := loadState(w.cfg.StateFile)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.state = state
	w.mu.Unlock()
	return nil
}

func loadState(path string) (watchState, error) {
	if path == "" {
		path = defaultStateFile
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return watchState{Targets: map[string]targetState{}}, nil
		}
		return watchState{}, fmt.Errorf("read github hash watch state %s: %w", path, err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return watchState{Targets: map[string]targetState{}}, nil
	}
	var state watchState
	if err := json.Unmarshal(data, &state); err != nil {
		return watchState{}, fmt.Errorf("parse github hash watch state %s: %w", path, err)
	}
	if state.Targets == nil {
		state.Targets = map[string]targetState{}
	}
	return state, nil
}

func saveState(path string, state watchState) error {
	if path == "" {
		path = defaultStateFile
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create github hash watch state directory: %w", err)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal github hash watch state: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write github hash watch state %s: %w", path, err)
	}
	return nil
}

func cloneWatchState(state watchState) watchState {
	out := watchState{Targets: make(map[string]targetState, len(state.Targets))}
	for key, value := range state.Targets {
		out.Targets[key] = value
	}
	return out
}

func parseGitHubTreeURL(raw string) (target, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return target{}, err
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return target{}, fmt.Errorf("unsupported url scheme %q", parsed.Scheme)
	}
	host := strings.ToLower(parsed.Hostname())
	if host != "github.com" && host != "www.github.com" {
		return target{}, fmt.Errorf("unsupported github host %q", parsed.Host)
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 4 || parts[2] != "tree" {
		return target{}, errors.New("expected github tree url: https://github.com/{owner}/{repo}/tree/{branch}/{path}")
	}
	owner := strings.TrimSpace(parts[0])
	repo := strings.TrimSpace(parts[1])
	branch := strings.TrimSpace(parts[3])
	path := strings.Join(parts[4:], "/")
	if owner == "" || repo == "" || branch == "" {
		return target{}, errors.New("github tree url is missing owner, repo or branch")
	}
	return target{
		RawURL: parsed.String(),
		Owner:  owner,
		Repo:   repo,
		Branch: branch,
		Path:   path,
	}, nil
}

func targetKey(target target) string {
	return fmt.Sprintf("%s|%s/%s@%s:%s", target.Region, target.Owner, target.Repo, target.Branch, target.Path)
}

func (t target) Description() string {
	if t.Path == "" {
		return fmt.Sprintf("%s/%s@%s", t.Owner, t.Repo, t.Branch)
	}
	return fmt.Sprintf("%s/%s@%s/%s", t.Owner, t.Repo, t.Branch, t.Path)
}
