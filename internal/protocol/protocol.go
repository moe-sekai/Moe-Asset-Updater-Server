package protocol

import "time"

type Region string

const (
	RegionJP Region = "jp"
	RegionEN Region = "en"
	RegionTW Region = "tw"
	RegionKR Region = "kr"
	RegionCN Region = "cn"
)

type AssetCategory string

const (
	AssetCategoryStartApp AssetCategory = "StartApp"
	AssetCategoryOnDemand AssetCategory = "OnDemand"
)

type JobStatus string

const (
	JobStatusPending       JobStatus = "pending"
	JobStatusRunning       JobStatus = "running"
	JobStatusSucceeded     JobStatus = "succeeded"
	JobStatusPartialFailed JobStatus = "partial_failed"
	JobStatusFailed        JobStatus = "failed"
	JobStatusCancelled     JobStatus = "cancelled"
)

type TaskStatus string

const (
	TaskStatusQueued          TaskStatus = "queued"
	TaskStatusLeased          TaskStatus = "leased"
	TaskStatusRunning         TaskStatus = "running"
	TaskStatusUploadingResult TaskStatus = "uploading_result"
	TaskStatusUploadingS3     TaskStatus = "uploading_s3"
	TaskStatusSucceeded       TaskStatus = "succeeded"
	TaskStatusFailed          TaskStatus = "failed"
	TaskStatusCancelled       TaskStatus = "cancelled"
)

type ProgressStage string

const (
	StageQueued            ProgressStage = "queued"
	StageDownload          ProgressStage = "download"
	StageDeobfuscate       ProgressStage = "deobfuscate"
	StageAssetStudioExport ProgressStage = "assetstudio_export"
	StagePostProcess       ProgressStage = "postprocess"
	StageArchive           ProgressStage = "archive"
	StageUploadResult      ProgressStage = "upload_result"
	StageUploadS3          ProgressStage = "upload_s3"
	StageDone              ProgressStage = "done"
)

type JobRequest struct {
	Server       Region `json:"server"`
	AssetVersion string `json:"assetVersion,omitempty"`
	AssetHash    string `json:"assetHash,omitempty"`
}

type CreateJobResponse struct {
	Message string      `json:"message"`
	Job     JobSnapshot `json:"job"`
}

type ExportOptions struct {
	ExportByCategory         bool   `json:"export_by_category"`
	UnityVersion             string `json:"unity_version,omitempty"`
	ExportUSMFiles           bool   `json:"export_usm_files"`
	DecodeUSMFiles           bool   `json:"decode_usm_files"`
	ExportACBFiles           bool   `json:"export_acb_files"`
	DecodeACBFiles           bool   `json:"decode_acb_files"`
	DecodeHCAFiles           bool   `json:"decode_hca_files"`
	ConvertPhotoToWebP       bool   `json:"convert_photo_to_webp"`
	RemovePNG                bool   `json:"remove_png"`
	ConvertVideoToMP4        bool   `json:"convert_video_to_mp4"`
	DirectUSMToMP4WithFFmpeg bool   `json:"direct_usm_to_mp4_with_ffmpeg"`
	RemoveM2V                bool   `json:"remove_m2v"`
	ConvertAudioToMP3        bool   `json:"convert_audio_to_mp3"`
	ConvertWavToFLAC         bool   `json:"convert_wav_to_flac"`
	RemoveWav                bool   `json:"remove_wav"`
}

type TaskPayload struct {
	TaskID       string            `json:"task_id"`
	JobID        string            `json:"job_id"`
	Region       Region            `json:"region"`
	BundlePath   string            `json:"bundle_path"`
	DownloadPath string            `json:"download_path"`
	BundleHash   string            `json:"bundle_hash"`
	Category     AssetCategory     `json:"category"`
	DownloadURL  string            `json:"download_url"`
	Headers      map[string]string `json:"headers,omitempty"`
	Export       ExportOptions     `json:"export"`
}

type ClientRegistrationRequest struct {
	ClientID string            `json:"client_id,omitempty"`
	Name     string            `json:"name,omitempty"`
	Version  string            `json:"version,omitempty"`
	MaxTasks int               `json:"max_tasks,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
}

type ClientRegistrationResponse struct {
	ClientID       string `json:"client_id"`
	HeartbeatAfter int    `json:"heartbeat_after_seconds"`
	LeaseTTL       int    `json:"lease_ttl_seconds"`
}

type HeartbeatRequest struct {
	ClientID      string   `json:"client_id"`
	ActiveTaskIDs []string `json:"active_task_ids,omitempty"`
}

type LeaseRequest struct {
	ClientID    string `json:"client_id"`
	MaxTasks    int    `json:"max_tasks,omitempty"`
	WaitSeconds int    `json:"wait_seconds,omitempty"`
}

type LeaseResponse struct {
	Tasks []TaskPayload `json:"tasks"`
}

type ProgressRequest struct {
	ClientID string        `json:"client_id"`
	Stage    ProgressStage `json:"stage"`
	Progress float64       `json:"progress"`
	Message  string        `json:"message,omitempty"`
}

type FailRequest struct {
	ClientID string `json:"client_id"`
	Error    string `json:"error"`
}

type ResultFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type TaskResultManifest struct {
	TaskID     string       `json:"task_id"`
	JobID      string       `json:"job_id"`
	Region     Region       `json:"region"`
	BundlePath string       `json:"bundle_path"`
	BundleHash string       `json:"bundle_hash"`
	Files      []ResultFile `json:"files"`
}

type JobSnapshot struct {
	ID           string    `json:"id"`
	Status       JobStatus `json:"status"`
	Region       Region    `json:"region"`
	AssetVersion string    `json:"assetVersion,omitempty"`
	AssetHash    string    `json:"assetHash,omitempty"`
	Total        int       `json:"total"`
	Queued       int       `json:"queued"`
	Running      int       `json:"running"`
	Succeeded    int       `json:"succeeded"`
	Failed       int       `json:"failed"`
	Cancelled    int       `json:"cancelled"`
	Progress     float64   `json:"progress"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type TaskSnapshot struct {
	TaskID      string        `json:"task_id"`
	JobID       string        `json:"job_id"`
	Region      Region        `json:"region"`
	BundlePath  string        `json:"bundle_path"`
	BundleHash  string        `json:"bundle_hash"`
	Status      TaskStatus    `json:"status"`
	ClientID    string        `json:"client_id,omitempty"`
	Attempt     int           `json:"attempt"`
	MaxAttempts int           `json:"max_attempts"`
	Stage       ProgressStage `json:"stage,omitempty"`
	Progress    float64       `json:"progress"`
	LastError   string        `json:"last_error,omitempty"`
	LeaseUntil  *time.Time    `json:"lease_until,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
	StartedAt   *time.Time    `json:"started_at,omitempty"`
	FinishedAt  *time.Time    `json:"finished_at,omitempty"`
}

type ClientSnapshot struct {
	ClientID      string            `json:"client_id"`
	Name          string            `json:"name,omitempty"`
	Version       string            `json:"version,omitempty"`
	MaxTasks      int               `json:"max_tasks"`
	Tags          map[string]string `json:"tags,omitempty"`
	ActiveTaskIDs []string          `json:"active_task_ids,omitempty"`
	LastSeen      time.Time         `json:"last_seen"`
}
