package scheduler

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/protocol"
)

type Manager struct {
	mu       sync.Mutex
	cfg      *config.Config
	jobs     map[string]*jobState
	jobOrder []string
	tasks    map[string]*taskState
	clients  map[string]*clientState
}

type jobState struct {
	id        string
	request   protocol.JobRequest
	status    protocol.JobStatus
	taskIDs   []string
	createdAt time.Time
	updatedAt time.Time
}

type taskState struct {
	payload     protocol.TaskPayload
	status      protocol.TaskStatus
	clientID    string
	attempt     int
	maxAttempts int
	stage       protocol.ProgressStage
	progress    float64
	lastError   string
	leaseUntil  *time.Time
	createdAt   time.Time
	updatedAt   time.Time
	startedAt   *time.Time
	finishedAt  *time.Time
}

type clientState struct {
	id            string
	name          string
	version       string
	maxTasks      int
	tags          map[string]string
	activeTaskIDs []string
	lastSeen      time.Time
}

func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		cfg:      cfg,
		jobs:     make(map[string]*jobState),
		jobOrder: make([]string, 0),
		tasks:    make(map[string]*taskState),
		clients:  make(map[string]*clientState),
	}
}

func (m *Manager) CreateJob(req protocol.JobRequest, payloads []protocol.TaskPayload) protocol.JobSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	jobID := newID("job")
	job := &jobState{
		id:        jobID,
		request:   req,
		status:    protocol.JobStatusRunning,
		createdAt: now,
		updatedAt: now,
	}
	if len(payloads) == 0 {
		job.status = protocol.JobStatusSucceeded
	}
	m.jobs[jobID] = job
	m.jobOrder = append(m.jobOrder, jobID)

	maxAttempts := m.cfg.Execution.Retry.Attempts
	if maxAttempts <= 0 {
		maxAttempts = 4
	}
	for _, payload := range payloads {
		taskID := newID("task")
		payload.TaskID = taskID
		payload.JobID = jobID
		status := protocol.TaskStatusQueued
		if payload.Delayed {
			status = protocol.TaskStatusDelayed
		}
		task := &taskState{
			payload:     payload,
			status:      status,
			maxAttempts: maxAttempts,
			stage:       protocol.StageQueued,
			createdAt:   now,
			updatedAt:   now,
		}
		m.tasks[taskID] = task
		job.taskIDs = append(job.taskIDs, taskID)
	}
	return m.jobSnapshotLocked(job)
}

func (m *Manager) GetJob(jobID string) (protocol.JobSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLeasesLocked(time.Now())
	job, ok := m.jobs[jobID]
	if !ok {
		return protocol.JobSnapshot{}, false
	}
	return m.jobSnapshotLocked(job), true
}

func (m *Manager) ListTasks(jobID string) ([]protocol.TaskSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLeasesLocked(time.Now())
	job, ok := m.jobs[jobID]
	if !ok {
		return nil, false
	}
	out := make([]protocol.TaskSnapshot, 0, len(job.taskIDs))
	for _, taskID := range job.taskIDs {
		out = append(out, m.taskSnapshotLocked(m.tasks[taskID]))
	}
	return out, true
}

func (m *Manager) ListClients() []protocol.ClientSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]protocol.ClientSnapshot, 0, len(m.clients))
	for _, client := range m.clients {
		out = append(out, protocol.ClientSnapshot{
			ClientID:      client.id,
			Name:          client.name,
			Version:       client.version,
			MaxTasks:      client.maxTasks,
			Tags:          cloneMap(client.tags),
			ActiveTaskIDs: append([]string(nil), client.activeTaskIDs...),
			LastSeen:      client.lastSeen,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out
}

func (m *Manager) Overview() Overview {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLeasesLocked(time.Now())
	return m.overviewLocked()
}

func (m *Manager) Register(req protocol.ClientRegistrationRequest) protocol.ClientRegistrationResponse {
	m.mu.Lock()
	defer m.mu.Unlock()

	clientID := req.ClientID
	if clientID == "" {
		clientID = newID("client")
	}
	maxTasks := req.MaxTasks
	if maxTasks <= 0 {
		maxTasks = 1
	}
	m.clients[clientID] = &clientState{
		id:       clientID,
		name:     req.Name,
		version:  req.Version,
		maxTasks: maxTasks,
		tags:     cloneMap(req.Tags),
		lastSeen: time.Now(),
	}
	return protocol.ClientRegistrationResponse{
		ClientID:       clientID,
		HeartbeatAfter: m.cfg.Execution.HeartbeatSeconds,
		LeaseTTL:       m.cfg.Execution.LeaseTTLSeconds,
	}
}

func (m *Manager) Heartbeat(req protocol.HeartbeatRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, ok := m.clients[req.ClientID]
	if !ok {
		return fmt.Errorf("client %s is not registered", req.ClientID)
	}
	now := time.Now()
	client.lastSeen = now
	client.activeTaskIDs = append([]string(nil), req.ActiveTaskIDs...)
	leaseUntil := now.Add(m.cfg.LeaseTTL())
	for _, taskID := range req.ActiveTaskIDs {
		if task, ok := m.tasks[taskID]; ok && task.clientID == req.ClientID && !isTerminal(task.status) {
			task.leaseUntil = &leaseUntil
			task.updatedAt = now
		}
	}
	return nil
}

func (m *Manager) Lease(req protocol.LeaseRequest) ([]protocol.TaskPayload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.expireLeasesLocked(now)
	client, ok := m.clients[req.ClientID]
	if !ok {
		return nil, fmt.Errorf("client %s is not registered", req.ClientID)
	}
	client.lastSeen = now

	limit := req.MaxTasks
	if limit <= 0 || limit > client.maxTasks {
		limit = client.maxTasks
	}
	active := m.activeTaskCountLocked(req.ClientID)
	if active >= limit {
		return []protocol.TaskPayload{}, nil
	}
	remaining := limit - active
	leaseUntil := now.Add(m.cfg.LeaseTTL())

	candidates := m.leaseCandidatesLocked(false)
	if len(candidates) == 0 {
		if m.hasRegularWorkLocked() {
			return []protocol.TaskPayload{}, nil
		}
		remaining = m.delayedLeaseCapacityLocked(req.ClientID, remaining)
		if remaining <= 0 {
			return []protocol.TaskPayload{}, nil
		}
		candidates = m.leaseCandidatesLocked(true)
	}

	leased := make([]protocol.TaskPayload, 0, remaining)
	for _, task := range candidates {
		if len(leased) >= remaining {
			break
		}
		task.status = protocol.TaskStatusLeased
		task.clientID = req.ClientID
		task.attempt++
		task.stage = protocol.StageQueued
		task.progress = 0
		task.lastError = ""
		task.leaseUntil = &leaseUntil
		if task.startedAt == nil {
			startedAt := now
			task.startedAt = &startedAt
		}
		task.updatedAt = now
		if job := m.jobs[task.payload.JobID]; job != nil {
			job.updatedAt = now
		}
		leased = append(leased, task.payload)
	}
	return leased, nil
}

func (m *Manager) Progress(taskID string, req protocol.ProgressRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if task.clientID != req.ClientID {
		return fmt.Errorf("task %s is not leased by client %s", taskID, req.ClientID)
	}
	if isTerminal(task.status) {
		return nil
	}
	now := time.Now()
	if req.Stage == protocol.StageUploadResult {
		task.status = protocol.TaskStatusUploadingResult
	} else {
		task.status = protocol.TaskStatusRunning
	}
	task.stage = req.Stage
	task.progress = clampProgress(req.Progress)
	task.updatedAt = now
	leaseUntil := now.Add(m.cfg.LeaseTTL())
	task.leaseUntil = &leaseUntil
	if job := m.jobs[task.payload.JobID]; job != nil {
		job.updatedAt = now
	}
	return nil
}

func (m *Manager) MarkUploadingS3(taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	now := time.Now()
	task.status = protocol.TaskStatusUploadingS3
	task.stage = protocol.StageUploadS3
	task.progress = 0.95
	task.updatedAt = now
	if job := m.jobs[task.payload.JobID]; job != nil {
		job.updatedAt = now
	}
	return nil
}

func (m *Manager) Complete(taskID string) (protocol.TaskPayload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[taskID]
	if !ok {
		return protocol.TaskPayload{}, fmt.Errorf("task %s not found", taskID)
	}
	now := time.Now()
	task.status = protocol.TaskStatusSucceeded
	task.stage = protocol.StageDone
	task.progress = 1
	task.leaseUntil = nil
	task.updatedAt = now
	finishedAt := now
	task.finishedAt = &finishedAt
	if job := m.jobs[task.payload.JobID]; job != nil {
		m.refreshJobStatusLocked(job)
	}
	return task.payload, nil
}

func (m *Manager) Fail(taskID string, clientID string, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if clientID != "" && task.clientID != "" && task.clientID != clientID {
		return fmt.Errorf("task %s is not leased by client %s", taskID, clientID)
	}
	m.failTaskLocked(task, message, time.Now())
	return nil
}

func (m *Manager) CancelJob(jobID string) (protocol.JobSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[jobID]
	if !ok {
		return protocol.JobSnapshot{}, false
	}
	now := time.Now()
	job.status = protocol.JobStatusCancelled
	job.updatedAt = now
	for _, taskID := range job.taskIDs {
		task := m.tasks[taskID]
		if task != nil && !isTerminal(task.status) {
			task.status = protocol.TaskStatusCancelled
			task.leaseUntil = nil
			task.updatedAt = now
			finishedAt := now
			task.finishedAt = &finishedAt
		}
	}
	return m.jobSnapshotLocked(job), true
}

func (m *Manager) TaskPayload(taskID string) (protocol.TaskPayload, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[taskID]
	if !ok {
		return protocol.TaskPayload{}, false
	}
	return task.payload, true
}

// TaskClientID returns the client ID currently leasing the given task.
func (m *Manager) TaskClientID(taskID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[taskID]
	if !ok {
		return ""
	}
	return task.clientID
}

func (m *Manager) failTaskLocked(task *taskState, message string, now time.Time) {
	task.lastError = message
	task.clientID = ""
	task.leaseUntil = nil
	task.updatedAt = now
	if task.attempt < task.maxAttempts {
		if task.payload.Delayed {
			task.status = protocol.TaskStatusDelayed
		} else {
			task.status = protocol.TaskStatusQueued
		}
		task.stage = protocol.StageQueued
		task.progress = 0
	} else {
		task.status = protocol.TaskStatusFailed
		finishedAt := now
		task.finishedAt = &finishedAt
	}
	if job := m.jobs[task.payload.JobID]; job != nil {
		m.refreshJobStatusLocked(job)
	}
}

func (m *Manager) expireLeasesLocked(now time.Time) {
	for _, task := range m.tasks {
		if task.leaseUntil == nil || isTerminal(task.status) {
			continue
		}
		if now.After(*task.leaseUntil) {
			m.failTaskLocked(task, "lease expired", now)
		}
	}
}

func (m *Manager) activeTaskCountLocked(clientID string) int {
	count := 0
	for _, task := range m.tasks {
		if task.clientID == clientID && !isTerminal(task.status) {
			count++
		}
	}
	return count
}

func (m *Manager) leaseCandidatesLocked(delayed bool) []*taskState {
	wantedStatus := protocol.TaskStatusQueued
	if delayed {
		wantedStatus = protocol.TaskStatusDelayed
	}
	candidates := make([]*taskState, 0)
	for _, jobID := range m.jobOrder {
		job := m.jobs[jobID]
		if job == nil || job.status == protocol.JobStatusCancelled || isTerminal(jobTaskStatusToTaskStatus(job.status)) {
			continue
		}
		for _, taskID := range job.taskIDs {
			task := m.tasks[taskID]
			if task != nil && task.status == wantedStatus && task.payload.Delayed == delayed {
				candidates = append(candidates, task)
			}
		}
	}
	return candidates
}

func (m *Manager) hasRegularWorkLocked() bool {
	for _, task := range m.tasks {
		if task != nil && !task.payload.Delayed && !isTerminal(task.status) {
			return true
		}
	}
	return false
}

func (m *Manager) delayedLeaseCapacityLocked(clientID string, requested int) int {
	if requested <= 0 {
		return 0
	}
	maxRunning := m.cfg.Execution.Delayed.MaxRunning
	if maxRunning <= 0 {
		maxRunning = 1
	}
	maxPerClient := m.cfg.Execution.Delayed.MaxPerClient
	if maxPerClient <= 0 {
		maxPerClient = 1
	}
	globalAvailable := maxRunning - m.delayedActiveCountLocked("")
	clientAvailable := maxPerClient - m.delayedActiveCountLocked(clientID)
	if globalAvailable <= 0 || clientAvailable <= 0 {
		return 0
	}
	if requested > globalAvailable {
		requested = globalAvailable
	}
	if requested > clientAvailable {
		requested = clientAvailable
	}
	return requested
}

func (m *Manager) delayedActiveCountLocked(clientID string) int {
	count := 0
	for _, task := range m.tasks {
		if task == nil || !task.payload.Delayed || task.status == protocol.TaskStatusDelayed || isTerminal(task.status) {
			continue
		}
		if clientID == "" || task.clientID == clientID {
			count++
		}
	}
	return count
}

func (m *Manager) refreshJobStatusLocked(job *jobState) {
	if job.status == protocol.JobStatusCancelled {
		return
	}
	counts := m.jobCountsLocked(job)
	job.updatedAt = time.Now()
	if counts.total == 0 || counts.succeeded == counts.total {
		job.status = protocol.JobStatusSucceeded
		return
	}
	finished := counts.succeeded + counts.failed + counts.cancelled
	if finished == counts.total {
		if counts.succeeded > 0 && counts.failed > 0 {
			job.status = protocol.JobStatusPartialFailed
		} else if counts.failed > 0 {
			job.status = protocol.JobStatusFailed
		} else {
			job.status = protocol.JobStatusCancelled
		}
		return
	}
	job.status = protocol.JobStatusRunning
}

func (m *Manager) jobSnapshotLocked(job *jobState) protocol.JobSnapshot {
	m.refreshJobStatusLocked(job)
	counts := m.jobCountsLocked(job)
	progress := 1.0
	if counts.total > 0 {
		progress = float64(counts.succeeded) / float64(counts.total)
	}
	return protocol.JobSnapshot{
		ID:           job.id,
		Status:       job.status,
		Region:       job.request.Server,
		AssetVersion: job.request.AssetVersion,
		AssetHash:    job.request.AssetHash,
		Total:        counts.total,
		Queued:       counts.queued,
		Delayed:      counts.delayed,
		Running:      counts.running,
		Succeeded:    counts.succeeded,
		Failed:       counts.failed,
		Cancelled:    counts.cancelled,
		Progress:     progress,
		CreatedAt:    job.createdAt,
		UpdatedAt:    job.updatedAt,
	}
}

func (m *Manager) taskSnapshotLocked(task *taskState) protocol.TaskSnapshot {
	var leaseUntil *time.Time
	if task.leaseUntil != nil {
		v := *task.leaseUntil
		leaseUntil = &v
	}
	var startedAt *time.Time
	if task.startedAt != nil {
		v := *task.startedAt
		startedAt = &v
	}
	var finishedAt *time.Time
	if task.finishedAt != nil {
		v := *task.finishedAt
		finishedAt = &v
	}
	return protocol.TaskSnapshot{
		TaskID:             task.payload.TaskID,
		JobID:              task.payload.JobID,
		Region:             task.payload.Region,
		BundlePath:         task.payload.BundlePath,
		BundleHash:         task.payload.BundleHash,
		EstimatedSizeBytes: task.payload.EstimatedSizeBytes,
		Delayed:            task.payload.Delayed,
		Status:             task.status,
		ClientID:           task.clientID,
		Attempt:            task.attempt,
		MaxAttempts:        task.maxAttempts,
		Stage:              task.stage,
		Progress:           task.progress,
		LastError:          task.lastError,
		LeaseUntil:         leaseUntil,
		CreatedAt:          task.createdAt,
		UpdatedAt:          task.updatedAt,
		StartedAt:          startedAt,
		FinishedAt:         finishedAt,
	}
}

type jobCounts struct {
	total     int
	queued    int
	delayed   int
	running   int
	succeeded int
	failed    int
	cancelled int
}

type Overview struct {
	Jobs      int
	Total     int
	Queued    int
	Delayed   int
	Running   int
	Succeeded int
	Failed    int
	Cancelled int
}

func (m *Manager) jobCountsLocked(job *jobState) jobCounts {
	counts := jobCounts{total: len(job.taskIDs)}
	for _, taskID := range job.taskIDs {
		task := m.tasks[taskID]
		if task == nil {
			continue
		}
		accumulateTaskCounts(&counts, task.status)
	}
	return counts
}

func (m *Manager) overviewLocked() Overview {
	overview := Overview{Jobs: len(m.jobs)}
	for _, task := range m.tasks {
		overview.Total++
		switch task.status {
		case protocol.TaskStatusQueued:
			overview.Queued++
		case protocol.TaskStatusDelayed:
			overview.Delayed++
		case protocol.TaskStatusLeased, protocol.TaskStatusRunning, protocol.TaskStatusUploadingResult, protocol.TaskStatusUploadingS3:
			overview.Running++
		case protocol.TaskStatusSucceeded:
			overview.Succeeded++
		case protocol.TaskStatusFailed:
			overview.Failed++
		case protocol.TaskStatusCancelled:
			overview.Cancelled++
		}
	}
	return overview
}

func accumulateTaskCounts(counts *jobCounts, status protocol.TaskStatus) {
	switch status {
	case protocol.TaskStatusQueued:
		counts.queued++
	case protocol.TaskStatusDelayed:
		counts.delayed++
	case protocol.TaskStatusLeased, protocol.TaskStatusRunning, protocol.TaskStatusUploadingResult, protocol.TaskStatusUploadingS3:
		counts.running++
	case protocol.TaskStatusSucceeded:
		counts.succeeded++
	case protocol.TaskStatusFailed:
		counts.failed++
	case protocol.TaskStatusCancelled:
		counts.cancelled++
	}
}

func isTerminal(status protocol.TaskStatus) bool {
	return status == protocol.TaskStatusSucceeded || status == protocol.TaskStatusFailed || status == protocol.TaskStatusCancelled
}

func jobTaskStatusToTaskStatus(status protocol.JobStatus) protocol.TaskStatus {
	switch status {
	case protocol.JobStatusSucceeded:
		return protocol.TaskStatusSucceeded
	case protocol.JobStatusFailed, protocol.JobStatusPartialFailed:
		return protocol.TaskStatusFailed
	case protocol.JobStatusCancelled:
		return protocol.TaskStatusCancelled
	default:
		return protocol.TaskStatusRunning
	}
}

func clampProgress(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func newID(prefix string) string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(buf)
}
