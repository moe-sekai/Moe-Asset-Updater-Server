package record

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/protocol"

	"github.com/bytedance/sonic"
)

type Manager struct {
	cfg *config.Config
	mu  sync.Mutex
}

func NewManager(cfg *config.Config) *Manager {
	return &Manager{cfg: cfg}
}

func (m *Manager) Load(region protocol.Region) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadUnlocked(region)
}

func (m *Manager) Mark(region protocol.Region, bundlePath string, bundleHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	records, err := m.loadUnlocked(region)
	if err != nil {
		return err
	}
	records[bundlePath] = bundleHash
	return m.saveUnlocked(region, records)
}

func (m *Manager) loadUnlocked(region protocol.Region) (map[string]string, error) {
	path, err := m.recordPath(region)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("read downloaded asset record %s: %w", path, err)
	}
	if len(data) == 0 {
		return map[string]string{}, nil
	}
	var records map[string]string
	if err := sonic.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("parse downloaded asset record %s: %w", path, err)
	}
	if records == nil {
		records = map[string]string{}
	}
	return records, nil
}

func (m *Manager) saveUnlocked(region protocol.Region, records map[string]string) error {
	path, err := m.recordPath(region)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create record directory: %w", err)
	}
	data, err := sonic.ConfigStd.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal downloaded asset record: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write downloaded asset record %s: %w", path, err)
	}
	return nil
}

func (m *Manager) recordPath(region protocol.Region) (string, error) {
	regionCfg, ok := m.cfg.Regions[region]
	if !ok {
		return "", fmt.Errorf("region %s not found in config", region)
	}
	if regionCfg.Paths.DownloadedAssetRecordFile != "" {
		return regionCfg.Paths.DownloadedAssetRecordFile, nil
	}
	return filepath.Join("data", string(region), "downloaded_assets.json"), nil
}
