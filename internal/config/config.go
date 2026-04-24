package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"moe-asset-server/internal/protocol"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    ServerConfig                     `yaml:"server"`
	Logging   LoggingConfig                    `yaml:"logging"`
	Execution ExecutionConfig                  `yaml:"execution"`
	Storage   StorageConfig                    `yaml:"storage"`
	GitSync   GitSyncConfig                    `yaml:"git_sync"`
	Regions   map[protocol.Region]RegionConfig `yaml:"regions"`
}

type ServerConfig struct {
	Host            string     `yaml:"host"`
	Port            int        `yaml:"port"`
	Proxy           string     `yaml:"proxy"`
	BodyLimitMB     int        `yaml:"body_limit_mb"`
	Auth            AuthConfig `yaml:"auth"`
	TLS             TLSConfig  `yaml:"tls"`
	StagingDir      string     `yaml:"staging_dir"`
	ResultRetention bool       `yaml:"result_retention"`
}

type AuthConfig struct {
	Enabled         bool   `yaml:"enabled"`
	UserAgentPrefix string `yaml:"user_agent_prefix"`
	BearerToken     string `yaml:"bearer_token"`
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type LoggingConfig struct {
	Level  string          `yaml:"level"`
	Format string          `yaml:"format"`
	File   string          `yaml:"file"`
	Access AccessLogConfig `yaml:"access"`
}

type AccessLogConfig struct {
	Enabled bool   `yaml:"enabled"`
	Format  string `yaml:"format"`
	File    string `yaml:"file"`
}

type ExecutionConfig struct {
	TimeoutSeconds   int         `yaml:"timeout_seconds"`
	AllowCancel      bool        `yaml:"allow_cancel"`
	BatchSaveSize    int         `yaml:"batch_save_size"`
	LeaseTTLSeconds  int         `yaml:"lease_ttl_seconds"`
	HeartbeatSeconds int         `yaml:"heartbeat_seconds"`
	Retry            RetryConfig `yaml:"retry"`
}

type RetryConfig struct {
	Attempts         int `yaml:"attempts"`
	InitialBackoffMS int `yaml:"initial_backoff_ms"`
	MaxBackoffMS     int `yaml:"max_backoff_ms"`
}

type StorageConfig struct {
	Providers         []StorageProviderConfig `yaml:"providers"`
	UploadConcurrency int                     `yaml:"upload_concurrency"`
}

type StorageProviderConfig struct {
	Kind       string               `yaml:"kind"`
	Endpoint   string               `yaml:"endpoint"`
	TLS        bool                 `yaml:"tls"`
	Bucket     string               `yaml:"bucket"`
	Prefix     string               `yaml:"prefix"`
	PathStyle  bool                 `yaml:"path_style"`
	Region     string               `yaml:"region"`
	PublicRead bool                 `yaml:"public_read"`
	Timeout    StorageTimeoutConfig `yaml:"timeout"`
	AccessKey  string               `yaml:"access_key"`
	SecretKey  string               `yaml:"secret_key"`
	Dedupe     StorageDedupeConfig  `yaml:"dedupe"`
}

type StorageTimeoutConfig struct {
	ConnectSeconds            int `yaml:"connect_seconds"`
	ReadSeconds               int `yaml:"read_seconds"`
	OperationAttemptSeconds   int `yaml:"operation_attempt_seconds"`
	OperationSeconds          int `yaml:"operation_seconds"`
	StalledStreamGraceSeconds int `yaml:"stalled_stream_grace_seconds"`
}

type StorageDedupeConfig struct {
	Enabled         bool   `yaml:"enabled"`
	IndexKey        string `yaml:"index_key"`
	AliasDuplicates bool   `yaml:"alias_duplicates"`
	VerifyRemote    bool   `yaml:"verify_remote"`
	FlushBatchSize  int    `yaml:"flush_batch_size"`
}

type GitSyncConfig struct {
	ChartHashes ChartHashesConfig `yaml:"chart_hashes"`
}

type ChartHashesConfig struct {
	Enabled       bool   `yaml:"enabled"`
	RepositoryDir string `yaml:"repository_dir"`
	Username      string `yaml:"username"`
	Email         string `yaml:"email"`
	Password      string `yaml:"password"`
}

type RegionConfig struct {
	Enabled  bool           `yaml:"enabled"`
	Provider ProviderConfig `yaml:"provider"`
	Crypto   CryptoConfig   `yaml:"crypto"`
	Runtime  RuntimeConfig  `yaml:"runtime"`
	Paths    PathsConfig    `yaml:"paths"`
	Filters  FiltersConfig  `yaml:"filters"`
	Export   ExportConfig   `yaml:"export"`
	Upload   UploadConfig   `yaml:"upload"`
}

type ProviderConfig struct {
	Kind                   string            `yaml:"kind"`
	CurrentVersionURL      string            `yaml:"current_version_url"`
	AssetVersionURL        string            `yaml:"asset_version_url"`
	AppVersion             string            `yaml:"app_version"`
	AssetInfoURLTemplate   string            `yaml:"asset_info_url_template"`
	AssetBundleURLTemplate string            `yaml:"asset_bundle_url_template"`
	Profile                string            `yaml:"profile"`
	ProfileHashes          map[string]string `yaml:"profile_hashes"`
	RequiredCookies        bool              `yaml:"required_cookies"`
}

type CryptoConfig struct {
	AESKeyHex string `yaml:"aes_key_hex"`
	AESIVHex  string `yaml:"aes_iv_hex"`
}

type RuntimeConfig struct {
	UnityVersion string `yaml:"unity_version"`
}

type PathsConfig struct {
	AssetSaveDir              string `yaml:"asset_save_dir"`
	DownloadedAssetRecordFile string `yaml:"downloaded_asset_record_file"`
}

type FiltersConfig struct {
	StartApp []string `yaml:"start_app"`
	OnDemand []string `yaml:"on_demand"`
	Skip     []string `yaml:"skip"`
	Priority []string `yaml:"priority"`
}

type ExportConfig struct {
	ByCategory          bool              `yaml:"by_category"`
	KeepTypes           []string          `yaml:"keep_types"`
	KeepExtensions      []string          `yaml:"keep_extensions"`
	RemoveFilteredFiles bool              `yaml:"remove_filtered_files"`
	USM                 USMExportConfig   `yaml:"usm"`
	ACB                 ACBExportConfig   `yaml:"acb"`
	HCA                 HCAExportConfig   `yaml:"hca"`
	Images              ImageExportConfig `yaml:"images"`
	Video               VideoExportConfig `yaml:"video"`
	Audio               AudioExportConfig `yaml:"audio"`
}

type USMExportConfig struct {
	Export bool `yaml:"export"`
	Decode bool `yaml:"decode"`
}

type ACBExportConfig struct {
	Export bool `yaml:"export"`
	Decode bool `yaml:"decode"`
}

type HCAExportConfig struct {
	Decode bool `yaml:"decode"`
}

type ImageExportConfig struct {
	ConvertToWebP bool `yaml:"convert_to_webp"`
	RemovePNG     bool `yaml:"remove_png"`
}

type VideoExportConfig struct {
	ConvertToMP4             bool `yaml:"convert_to_mp4"`
	DirectUSMToMP4WithFFmpeg bool `yaml:"direct_usm_to_mp4_with_ffmpeg"`
	RemoveM2V                bool `yaml:"remove_m2v"`
}

type AudioExportConfig struct {
	ConvertToMP3  bool `yaml:"convert_to_mp3"`
	ConvertToFLAC bool `yaml:"convert_to_flac"`
	RemoveWav     bool `yaml:"remove_wav"`
}

type UploadConfig struct {
	Enabled                    bool `yaml:"enabled"`
	RemoveLocalAfterUpload     bool `yaml:"remove_local_after_upload"`
	RemoveEmptyDirsAfterUpload bool `yaml:"remove_empty_dirs_after_upload"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		path = os.Getenv("MOE_ASSET_SERVER_CONFIG")
	}
	if path == "" {
		path = "config.yaml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	data = []byte(expandEnvPreservingTemplates(string(data)))

	cfg := Default()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.applyDefaults()
	return &cfg, nil
}

func Default() Config {
	cfg := Config{}
	cfg.applyDefaults()
	return cfg
}

func expandEnvPreservingTemplates(value string) string {
	return os.Expand(value, func(name string) string {
		if name == strings.ToUpper(name) {
			return os.Getenv(name)
		}
		return "${" + name + "}"
	})
}

func (c *Config) applyDefaults() {
	if c.Server.Host == "" {
		c.Server.Host = "0.0.0.0"
	}
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if c.Server.BodyLimitMB == 0 {
		c.Server.BodyLimitMB = 2048
	}
	if c.Server.StagingDir == "" {
		c.Server.StagingDir = "./data/staging"
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "INFO"
	}
	if c.Logging.Access.Format == "" {
		c.Logging.Access.Format = "[${time}] ${status} - ${method} ${path} ${latency}\n"
	}
	if c.Execution.TimeoutSeconds == 0 {
		c.Execution.TimeoutSeconds = 21600
	}
	if c.Execution.BatchSaveSize == 0 {
		c.Execution.BatchSaveSize = 100
	}
	if c.Execution.LeaseTTLSeconds == 0 {
		c.Execution.LeaseTTLSeconds = 300
	}
	if c.Execution.HeartbeatSeconds == 0 {
		c.Execution.HeartbeatSeconds = 30
	}
	if c.Execution.Retry.Attempts == 0 {
		c.Execution.Retry.Attempts = 4
	}
	if c.Storage.UploadConcurrency == 0 {
		c.Storage.UploadConcurrency = 16
	}
	if c.Regions == nil {
		c.Regions = make(map[protocol.Region]RegionConfig)
	}
}

func (c Config) ListenAddress() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

func (c Config) LeaseTTL() time.Duration {
	return time.Duration(c.Execution.LeaseTTLSeconds) * time.Second
}

func (c Config) HeartbeatInterval() time.Duration {
	return time.Duration(c.Execution.HeartbeatSeconds) * time.Second
}

func (r RegionConfig) ExportOptions() protocol.ExportOptions {
	return protocol.ExportOptions{
		ExportByCategory:         r.Export.ByCategory,
		UnityVersion:             r.Runtime.UnityVersion,
		ExportUSMFiles:           r.Export.USM.Export,
		DecodeUSMFiles:           r.Export.USM.Decode,
		ExportACBFiles:           r.Export.ACB.Export,
		DecodeACBFiles:           r.Export.ACB.Decode,
		DecodeHCAFiles:           r.Export.HCA.Decode,
		ConvertPhotoToWebP:       r.Export.Images.ConvertToWebP,
		RemovePNG:                r.Export.Images.RemovePNG,
		ConvertVideoToMP4:        r.Export.Video.ConvertToMP4,
		DirectUSMToMP4WithFFmpeg: r.Export.Video.DirectUSMToMP4WithFFmpeg,
		RemoveM2V:                r.Export.Video.RemoveM2V,
		ConvertAudioToMP3:        r.Export.Audio.ConvertToMP3,
		ConvertWavToFLAC:         r.Export.Audio.ConvertToFLAC,
		RemoveWav:                r.Export.Audio.RemoveWav,
	}
}
