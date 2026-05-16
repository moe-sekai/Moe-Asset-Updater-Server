package catalog

import (
	"testing"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/protocol"
)

func TestBuildDownloadListMarksDelayedByEstimatedSize(t *testing.T) {
	cfg := delayedCatalogConfig()
	builder := NewBuilder(&cfg)

	tasks := builder.buildDownloadList(protocol.RegionJP, delayedRegionConfig(), assetInfoWithBundle("large/path", 64*1024*1024), nil, "asset-version", "asset-hash", "")
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if !tasks[0].Delayed {
		t.Fatalf("expected large bundle to be marked delayed")
	}
	if tasks[0].EstimatedSizeBytes != 64*1024*1024 {
		t.Fatalf("unexpected estimated size: got %d", tasks[0].EstimatedSizeBytes)
	}
}

func TestBuildDownloadListMarksDelayedByPathPattern(t *testing.T) {
	cfg := delayedCatalogConfig()
	cfg.Execution.Delayed.PathPatterns = []string{`^huge/`}
	builder := NewBuilder(&cfg)

	tasks := builder.buildDownloadList(protocol.RegionJP, delayedRegionConfig(), assetInfoWithBundle("huge/explodes-after-export", 1024), nil, "asset-version", "asset-hash", "")
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if !tasks[0].Delayed {
		t.Fatalf("expected path pattern match to mark task delayed")
	}
}

func TestBuildDownloadListDoesNotMarkDelayedWhenDisabled(t *testing.T) {
	cfg := delayedCatalogConfig()
	cfg.Execution.Delayed.Enabled = false
	builder := NewBuilder(&cfg)

	tasks := builder.buildDownloadList(protocol.RegionJP, delayedRegionConfig(), assetInfoWithBundle("large/path", 64*1024*1024), nil, "asset-version", "asset-hash", "")
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if tasks[0].Delayed || tasks[0].Queue == protocol.TaskQueueDelayed {
		t.Fatalf("did not expect delayed marker when delayed queue is disabled")
	}
}

func TestBuildDownloadListMarksLowPriorityByPathPattern(t *testing.T) {
	cfg := delayedCatalogConfig()
	cfg.Execution.LowPriority.PathPatterns = []string{`^model3d(?:/|$)`}
	builder := NewBuilder(&cfg)

	tasks := builder.buildDownloadList(protocol.RegionJP, delayedRegionConfig(), assetInfoWithBundle("model3d/chara/body", 1024), nil, "asset-version", "asset-hash", "")
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if tasks[0].Queue != protocol.TaskQueueLowPriority {
		t.Fatalf("expected low priority queue, got %q", tasks[0].Queue)
	}
	if tasks[0].Delayed || tasks[0].Priority {
		t.Fatalf("low priority task should not use delayed/priority compatibility flags, got %#v", tasks[0])
	}
}

func TestBuildDownloadListDelayedBeatsLowPriority(t *testing.T) {
	cfg := delayedCatalogConfig()
	cfg.Execution.Delayed.PathPatterns = []string{`^model3d(?:/|$)`}
	cfg.Execution.LowPriority.PathPatterns = []string{`^model3d(?:/|$)`}
	builder := NewBuilder(&cfg)

	tasks := builder.buildDownloadList(protocol.RegionJP, delayedRegionConfig(), assetInfoWithBundle("model3d/chara/body", 1024), nil, "asset-version", "asset-hash", "")
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if tasks[0].Queue != protocol.TaskQueueDelayed || !tasks[0].Delayed {
		t.Fatalf("expected delayed queue to beat low priority, got %#v", tasks[0])
	}
}

func TestBuildDownloadListLowPriorityBeatsPriority(t *testing.T) {
	cfg := delayedCatalogConfig()
	cfg.Execution.Priority.PathPatterns = []string{`^model3d(?:/|$)`}
	cfg.Execution.LowPriority.PathPatterns = []string{`^model3d(?:/|$)`}
	builder := NewBuilder(&cfg)

	tasks := builder.buildDownloadList(protocol.RegionJP, delayedRegionConfig(), assetInfoWithBundle("model3d/chara/body", 1024), nil, "asset-version", "asset-hash", "")
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if tasks[0].Queue != protocol.TaskQueueLowPriority || tasks[0].Priority {
		t.Fatalf("expected low priority to beat priority, got %#v", tasks[0])
	}
}

func delayedCatalogConfig() config.Config {
	cfg := config.Default()
	cfg.Execution.Delayed.Enabled = true
	cfg.Execution.Delayed.ThresholdMB = 50
	cfg.Execution.Delayed.MaxRunning = 1
	cfg.Execution.Delayed.MaxPerClient = 1
	return cfg
}

func delayedRegionConfig() config.RegionConfig {
	return config.RegionConfig{
		Enabled: true,
		Provider: config.ProviderConfig{
			Kind:                   "colorful_palette",
			AssetBundleURLTemplate: "https://example.invalid/{asset_version}/{asset_hash}/{bundle_path}",
			Profile:                "production",
			ProfileHashes:          map[string]string{"production": "profile-hash"},
		},
		Filters: config.FiltersConfig{
			StartApp: []string{".*"},
			OnDemand: []string{".*"},
		},
	}
}

func assetInfoWithBundle(bundleName string, fileSize int64) *assetBundleInfo {
	return &assetBundleInfo{Bundles: map[string]assetBundleDetail{
		bundleName: {
			BundleName: bundleName,
			Hash:       "bundle-hash",
			Category:   protocol.AssetCategoryOnDemand,
			FileSize:   fileSize,
		},
	}}
}
