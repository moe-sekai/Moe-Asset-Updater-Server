package scheduler

import (
	"testing"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/protocol"
)

func TestLeaseDelaysLargeTasksUntilRegularWorkFinishes(t *testing.T) {
	cfg := delayedTestConfig()
	manager := NewManager(&cfg)

	job := manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		testTaskPayload("small.bundle", false),
		testTaskPayload("big.bundle", true),
	})
	if job.Queued != 1 || job.Delayed != 1 {
		t.Fatalf("unexpected initial counts: queued=%d delayed=%d", job.Queued, job.Delayed)
	}

	clientID := manager.Register(protocol.ClientRegistrationRequest{MaxTasks: 4}).ClientID
	regular, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease regular task: %v", err)
	}
	if len(regular) != 1 || regular[0].Delayed {
		t.Fatalf("expected exactly one regular task, got %#v", regular)
	}

	blocked, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease while regular task is still active: %v", err)
	}
	if len(blocked) != 0 {
		t.Fatalf("delayed task should not lease while regular work is active, got %#v", blocked)
	}

	if _, err := manager.Complete(regular[0].TaskID); err != nil {
		t.Fatalf("complete regular task: %v", err)
	}
	delayed, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease delayed task: %v", err)
	}
	if len(delayed) != 1 || !delayed[0].Delayed {
		t.Fatalf("expected exactly one delayed task after regular work finishes, got %#v", delayed)
	}
}

func TestDelayedLeaseHonorsGlobalAndPerClientLimits(t *testing.T) {
	cfg := delayedTestConfig()
	cfg.Execution.Delayed.MaxRunning = 3
	cfg.Execution.Delayed.MaxPerClient = 1
	manager := NewManager(&cfg)

	manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		testTaskPayload("big-a.bundle", true),
		testTaskPayload("big-b.bundle", true),
		testTaskPayload("big-c.bundle", true),
	})

	clientA := manager.Register(protocol.ClientRegistrationRequest{ClientID: "client-a", MaxTasks: 4}).ClientID
	clientB := manager.Register(protocol.ClientRegistrationRequest{ClientID: "client-b", MaxTasks: 4}).ClientID

	first, err := manager.Lease(protocol.LeaseRequest{ClientID: clientA, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease first delayed task: %v", err)
	}
	if len(first) != 1 || !first[0].Delayed {
		t.Fatalf("expected client A to receive one delayed task, got %#v", first)
	}

	secondFromSameClient, err := manager.Lease(protocol.LeaseRequest{ClientID: clientA, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease second delayed task from same client: %v", err)
	}
	if len(secondFromSameClient) != 0 {
		t.Fatalf("max_per_client=1 should block another delayed task for same client, got %#v", secondFromSameClient)
	}

	fromOtherClient, err := manager.Lease(protocol.LeaseRequest{ClientID: clientB, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease delayed task from other client: %v", err)
	}
	if len(fromOtherClient) != 1 || !fromOtherClient[0].Delayed {
		t.Fatalf("expected client B to receive one delayed task, got %#v", fromOtherClient)
	}
}

func TestDelayedLeaseHonorsGlobalLimit(t *testing.T) {
	cfg := delayedTestConfig()
	cfg.Execution.Delayed.MaxRunning = 1
	cfg.Execution.Delayed.MaxPerClient = 1
	manager := NewManager(&cfg)

	manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		testTaskPayload("big-a.bundle", true),
		testTaskPayload("big-b.bundle", true),
	})

	clientA := manager.Register(protocol.ClientRegistrationRequest{ClientID: "client-a", MaxTasks: 4}).ClientID
	clientB := manager.Register(protocol.ClientRegistrationRequest{ClientID: "client-b", MaxTasks: 4}).ClientID

	leased, err := manager.Lease(protocol.LeaseRequest{ClientID: clientA, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease delayed task: %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected one delayed task under global limit, got %#v", leased)
	}

	blocked, err := manager.Lease(protocol.LeaseRequest{ClientID: clientB, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease delayed task over global limit: %v", err)
	}
	if len(blocked) != 0 {
		t.Fatalf("max_running=1 should block other delayed tasks, got %#v", blocked)
	}
}

func TestDelayedTaskFailureRequeuesToDelayedState(t *testing.T) {
	cfg := delayedTestConfig()
	cfg.Execution.Retry.Attempts = 2
	manager := NewManager(&cfg)

	job := manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		testTaskPayload("big.bundle", true),
	})
	clientID := manager.Register(protocol.ClientRegistrationRequest{MaxTasks: 1}).ClientID

	leased, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 1})
	if err != nil {
		t.Fatalf("lease delayed task: %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected delayed task, got %#v", leased)
	}
	if err := manager.Fail(leased[0].TaskID, clientID, "boom"); err != nil {
		t.Fatalf("fail delayed task: %v", err)
	}

	tasks, ok := manager.ListTasks(job.ID)
	if !ok {
		t.Fatalf("job %s not found", job.ID)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one task snapshot, got %d", len(tasks))
	}
	if tasks[0].Status != protocol.TaskStatusDelayed {
		t.Fatalf("failed delayed task should requeue as delayed, got %s", tasks[0].Status)
	}
	if tasks[0].Attempt != 1 {
		t.Fatalf("unexpected attempt count: got %d want 1", tasks[0].Attempt)
	}
}

func TestLeasePrefersPriorityTasksBeforeRegularTasks(t *testing.T) {
	cfg := delayedTestConfig()
	manager := NewManager(&cfg)

	manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		priorityTaskPayload("normal-a.bundle", false, false),
		priorityTaskPayload("normal-b.bundle", false, false),
		priorityTaskPayload("priority-a.bundle", true, false),
	})

	clientID := manager.Register(protocol.ClientRegistrationRequest{MaxTasks: 1}).ClientID

	first, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 1})
	if err != nil {
		t.Fatalf("lease first task: %v", err)
	}
	if len(first) != 1 || !first[0].Priority {
		t.Fatalf("expected priority task to be leased first, got %#v", first)
	}
}

func TestLeasePriorityBeatsDelayedAndNormal(t *testing.T) {
	cfg := delayedTestConfig()
	manager := NewManager(&cfg)

	manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		priorityTaskPayload("big.bundle", false, true),
		priorityTaskPayload("normal.bundle", false, false),
		priorityTaskPayload("priority.bundle", true, false),
	})

	clientID := manager.Register(protocol.ClientRegistrationRequest{MaxTasks: 1}).ClientID

	leased, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 1})
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if len(leased) != 1 || leased[0].BundlePath != "priority.bundle" {
		t.Fatalf("expected priority bundle first, got %#v", leased)
	}
	if _, err := manager.Complete(leased[0].TaskID); err != nil {
		t.Fatalf("complete priority task: %v", err)
	}

	leased, err = manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 1})
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if len(leased) != 1 || leased[0].BundlePath != "normal.bundle" {
		t.Fatalf("expected normal bundle second, got %#v", leased)
	}
	if _, err := manager.Complete(leased[0].TaskID); err != nil {
		t.Fatalf("complete normal task: %v", err)
	}

	leased, err = manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 1})
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if len(leased) != 1 || !leased[0].Delayed {
		t.Fatalf("expected delayed bundle last, got %#v", leased)
	}
}

func TestLowPriorityWaitsUntilRegularWorkFinishesAndAllowsMultipleLeases(t *testing.T) {
	cfg := delayedTestConfig()
	cfg.Execution.Delayed.MaxRunning = 1
	cfg.Execution.LowPriority.MaxRunning = 0
	cfg.Execution.LowPriority.MaxPerClient = 0
	manager := NewManager(&cfg)

	job := manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		testTaskPayload("normal.bundle", false),
		lowPriorityTaskPayload("model3d/a.bundle"),
		lowPriorityTaskPayload("model3d/b.bundle"),
	})
	if job.Queued != 1 || job.LowPriority != 2 || job.Delayed != 0 {
		t.Fatalf("unexpected initial counts: queued=%d low_priority=%d delayed=%d", job.Queued, job.LowPriority, job.Delayed)
	}

	clientID := manager.Register(protocol.ClientRegistrationRequest{MaxTasks: 4}).ClientID
	regular, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease regular task: %v", err)
	}
	if len(regular) != 1 || regular[0].Queue != protocol.TaskQueueNormal {
		t.Fatalf("expected exactly one normal task first, got %#v", regular)
	}

	blocked, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease while regular task is active: %v", err)
	}
	if len(blocked) != 0 {
		t.Fatalf("low priority tasks should not lease while regular work is active, got %#v", blocked)
	}
	if _, err := manager.Complete(regular[0].TaskID); err != nil {
		t.Fatalf("complete regular task: %v", err)
	}

	lowPriority, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease low priority tasks: %v", err)
	}
	if len(lowPriority) != 2 {
		t.Fatalf("expected two low priority tasks to lease together, got %#v", lowPriority)
	}
	for _, task := range lowPriority {
		if task.Queue != protocol.TaskQueueLowPriority || task.Delayed {
			t.Fatalf("expected low priority task without delayed marker, got %#v", task)
		}
	}
}

func TestLowPriorityHonorsOwnLimitsWithoutUsingDelayedLimit(t *testing.T) {
	cfg := delayedTestConfig()
	cfg.Execution.Delayed.MaxRunning = 1
	cfg.Execution.Delayed.MaxPerClient = 1
	cfg.Execution.LowPriority.MaxRunning = 2
	cfg.Execution.LowPriority.MaxPerClient = 2
	manager := NewManager(&cfg)

	manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		lowPriorityTaskPayload("model3d/a.bundle"),
		lowPriorityTaskPayload("model3d/b.bundle"),
		lowPriorityTaskPayload("model3d/c.bundle"),
	})

	clientA := manager.Register(protocol.ClientRegistrationRequest{ClientID: "client-a", MaxTasks: 4}).ClientID
	clientB := manager.Register(protocol.ClientRegistrationRequest{ClientID: "client-b", MaxTasks: 4}).ClientID

	leased, err := manager.Lease(protocol.LeaseRequest{ClientID: clientA, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease low priority tasks: %v", err)
	}
	if len(leased) != 2 {
		t.Fatalf("low_priority.max_running=2 should allow two tasks despite delayed.max_running=1, got %#v", leased)
	}
	blocked, err := manager.Lease(protocol.LeaseRequest{ClientID: clientB, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease low priority task over global limit: %v", err)
	}
	if len(blocked) != 0 {
		t.Fatalf("low_priority.max_running=2 should block the third active task, got %#v", blocked)
	}
}

func TestLowPriorityTaskFailureRequeuesToLowPriorityState(t *testing.T) {
	cfg := delayedTestConfig()
	cfg.Execution.Retry.Attempts = 2
	manager := NewManager(&cfg)

	job := manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		lowPriorityTaskPayload("model3d/a.bundle"),
	})
	clientID := manager.Register(protocol.ClientRegistrationRequest{MaxTasks: 1}).ClientID

	leased, err := manager.Lease(protocol.LeaseRequest{ClientID: clientID, MaxTasks: 1})
	if err != nil {
		t.Fatalf("lease low priority task: %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected low priority task, got %#v", leased)
	}
	if err := manager.Fail(leased[0].TaskID, clientID, "boom"); err != nil {
		t.Fatalf("fail low priority task: %v", err)
	}

	tasks, ok := manager.ListTasks(job.ID)
	if !ok {
		t.Fatalf("job %s not found", job.ID)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one task snapshot, got %d", len(tasks))
	}
	if tasks[0].Status != protocol.TaskStatusLowPriority || tasks[0].Queue != protocol.TaskQueueLowPriority {
		t.Fatalf("failed low priority task should requeue as low priority, got status=%s queue=%s", tasks[0].Status, tasks[0].Queue)
	}
}

func TestDelayedWaitsForLowPriorityWork(t *testing.T) {
	cfg := delayedTestConfig()
	cfg.Execution.LowPriority.MaxRunning = 4
	cfg.Execution.LowPriority.MaxPerClient = 4
	manager := NewManager(&cfg)

	manager.CreateJob(protocol.JobRequest{Server: protocol.RegionJP}, []protocol.TaskPayload{
		lowPriorityTaskPayload("model3d/a.bundle"),
		testTaskPayload("big.bundle", true),
	})
	clientA := manager.Register(protocol.ClientRegistrationRequest{ClientID: "client-a", MaxTasks: 4}).ClientID
	clientB := manager.Register(protocol.ClientRegistrationRequest{ClientID: "client-b", MaxTasks: 4}).ClientID

	lowPriority, err := manager.Lease(protocol.LeaseRequest{ClientID: clientA, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease low priority task: %v", err)
	}
	if len(lowPriority) != 1 || lowPriority[0].Queue != protocol.TaskQueueLowPriority {
		t.Fatalf("expected low priority task before delayed, got %#v", lowPriority)
	}
	blocked, err := manager.Lease(protocol.LeaseRequest{ClientID: clientB, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease delayed while low priority active: %v", err)
	}
	if len(blocked) != 0 {
		t.Fatalf("delayed queue should wait for low priority work, got %#v", blocked)
	}
	if _, err := manager.Complete(lowPriority[0].TaskID); err != nil {
		t.Fatalf("complete low priority task: %v", err)
	}

	delayed, err := manager.Lease(protocol.LeaseRequest{ClientID: clientB, MaxTasks: 4})
	if err != nil {
		t.Fatalf("lease delayed task: %v", err)
	}
	if len(delayed) != 1 || delayed[0].Queue != protocol.TaskQueueDelayed || !delayed[0].Delayed {
		t.Fatalf("expected delayed task after low priority completes, got %#v", delayed)
	}
}

func priorityTaskPayload(bundlePath string, priority bool, delayed bool) protocol.TaskPayload {
	estimatedSize := int64(4 * 1024 * 1024)
	if delayed {
		estimatedSize = 64 * 1024 * 1024
	}
	queue := protocol.TaskQueueNormal
	if priority {
		queue = protocol.TaskQueuePriority
	}
	if delayed {
		queue = protocol.TaskQueueDelayed
	}
	return protocol.TaskPayload{
		Region:             protocol.RegionJP,
		BundlePath:         bundlePath,
		DownloadPath:       bundlePath,
		BundleHash:         bundlePath + "-hash",
		Category:           protocol.AssetCategoryOnDemand,
		DownloadURL:        "https://example.invalid/" + bundlePath,
		EstimatedSizeBytes: estimatedSize,
		Queue:              queue,
		Priority:           priority,
		Delayed:            delayed,
	}
}

func lowPriorityTaskPayload(bundlePath string) protocol.TaskPayload {
	return protocol.TaskPayload{
		Region:             protocol.RegionJP,
		BundlePath:         bundlePath,
		DownloadPath:       bundlePath,
		BundleHash:         bundlePath + "-hash",
		Category:           protocol.AssetCategoryOnDemand,
		DownloadURL:        "https://example.invalid/" + bundlePath,
		EstimatedSizeBytes: 4 * 1024 * 1024,
		Queue:              protocol.TaskQueueLowPriority,
	}
}

func delayedTestConfig() config.Config {
	cfg := config.Default()
	cfg.Execution.Delayed.Enabled = true
	cfg.Execution.Delayed.ThresholdMB = 50
	cfg.Execution.Delayed.MaxRunning = 1
	cfg.Execution.Delayed.MaxPerClient = 1
	cfg.Execution.Retry.Attempts = 4
	return cfg
}

func testTaskPayload(bundlePath string, delayed bool) protocol.TaskPayload {
	estimatedSize := int64(4 * 1024 * 1024)
	if delayed {
		estimatedSize = 64 * 1024 * 1024
	}
	queue := protocol.TaskQueueNormal
	if delayed {
		queue = protocol.TaskQueueDelayed
	}
	return protocol.TaskPayload{
		Region:             protocol.RegionJP,
		BundlePath:         bundlePath,
		DownloadPath:       bundlePath,
		BundleHash:         bundlePath + "-hash",
		Category:           protocol.AssetCategoryOnDemand,
		DownloadURL:        "https://example.invalid/" + bundlePath,
		EstimatedSizeBytes: estimatedSize,
		Queue:              queue,
		Delayed:            delayed,
	}
}
