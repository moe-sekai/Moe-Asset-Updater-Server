package api

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"moe-asset-server/internal/catalog"
	"moe-asset-server/internal/config"
	"moe-asset-server/internal/gitwatch"
	harukiLogger "moe-asset-server/internal/logger"
	"moe-asset-server/internal/protocol"
	"moe-asset-server/internal/record"
	"moe-asset-server/internal/scheduler"
	"moe-asset-server/internal/storage"
	"moe-asset-server/internal/tcpserver"

	"github.com/gofiber/fiber/v3"
)

type Server struct {
	cfg          *config.Config
	logger       *harukiLogger.Logger
	builder      *catalog.Builder
	scheduler    *scheduler.Manager
	records      *record.Manager
	uploader     *storage.Uploader
	taskNotifier func()
}

var (
	errLoadDownloadedAssetRecord = errors.New("load downloaded asset record")
	errBuildAssetTasks           = errors.New("build asset tasks")
)

func New(cfg *config.Config, logger *harukiLogger.Logger) *Server {
	if logger == nil {
		logger = harukiLogger.NewLogger("ServerAPI", cfg.Logging.Level, nil)
	}
	return &Server{
		cfg:       cfg,
		logger:    logger,
		builder:   catalog.NewBuilder(cfg),
		scheduler: scheduler.NewManager(cfg),
		records:   record.NewManager(cfg),
		uploader:  storage.NewUploader(cfg),
	}
}

func (s *Server) StartTCPServer(ctx context.Context) {
	if !s.cfg.Server.TCP.Enabled {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	server := tcpserver.New(s.cfg, s.logger, s.scheduler)
	s.taskNotifier = server.Notify
	go func() {
		if err := server.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Errorf("TCP task server stopped: %v", err)
		}
	}()
}

func (s *Server) notifyTasksAvailable() {
	if s.taskNotifier != nil {
		s.taskNotifier()
	}
}

func (s *Server) StartGitHashWatcher(ctx context.Context) {
	watchCfg := s.cfg.GitSync.AssetHashWatch
	if !watchCfg.Enabled {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	targets := make([]config.AssetHashWatchTargetConfig, 0, len(watchCfg.Targets))
	for _, target := range watchCfg.Targets {
		if target.Region == "" || target.URL == "" {
			s.logger.Warnf("GitHub哈希监控目标配置不完整，已跳过 region=%s url=%s", target.Region, target.URL)
			continue
		}
		regionCfg, ok := s.cfg.Regions[target.Region]
		if !ok {
			s.logger.Warnf("GitHub哈希监控目标 region=%s 未在配置中找到，已跳过", target.Region)
			continue
		}
		if !regionCfg.Enabled {
			s.logger.Warnf("GitHub哈希监控目标 region=%s 已禁用，已跳过", target.Region)
			continue
		}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		s.logger.Warnf("GitHub哈希监控已启用但没有可用目标")
		return
	}
	watchCfg.Targets = targets

	watcher, err := gitwatch.New(watchCfg, s.logger, func(ctx context.Context, region protocol.Region, oldHash string, newHash string) (protocol.JobSnapshot, error) {
		return s.createAssetJob(ctx, protocol.JobRequest{Server: region}, "GitHub哈希变化创建任务")
	})
	if err != nil {
		s.logger.Errorf("GitHub哈希监控启动失败: %v", err)
		return
	}
	watcher.Start(ctx)
}

func (s *Server) logJobProgress(event string, job protocol.JobSnapshot) {
	overview := s.scheduler.Overview()
	jobCompleted := job.Succeeded + job.Failed + job.Cancelled
	allCompleted := overview.Succeeded + overview.Failed + overview.Cancelled
	s.logger.Infof(
		"[%s] job=%s region=%s 总任务=%d 已完成=%d 成功=%d 失败=%d 取消=%d 运行中=%d 排队=%d 延迟=%d | 全局任务=%d 全局已完成=%d 全局成功=%d 全局失败=%d 全局取消=%d 全局运行中=%d 全局排队=%d 全局延迟=%d",
		event,
		job.ID,
		job.Region,
		job.Total,
		jobCompleted,
		job.Succeeded,
		job.Failed,
		job.Cancelled,
		job.Running,
		job.Queued,
		job.Delayed,
		overview.Total,
		allCompleted,
		overview.Succeeded,
		overview.Failed,
		overview.Cancelled,
		overview.Running,
		overview.Queued,
		overview.Delayed,
	)
}

func (s *Server) logJobProgressByID(event string, jobID string) {
	job, ok := s.scheduler.GetJob(jobID)
	if !ok {
		return
	}
	s.logJobProgress(event, job)
}

func (s *Server) failTaskAndLog(taskID string, clientID string, message string, event string) error {
	payload, _ := s.scheduler.TaskPayload(taskID)
	if err := s.scheduler.Fail(taskID, clientID, message); err != nil {
		return err
	}
	s.notifyTasksAvailable()
	if payload.JobID != "" {
		s.logJobProgressByID(event, payload.JobID)
	}
	return nil
}

func (s *Server) RegisterRoutes(app *fiber.App) {
	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"ok": true})
	})

	api := app.Group("/api/v1", s.authMiddleware)
	api.Post("/jobs", s.createJobHandler)
	api.Get("/jobs/:job_id", s.getJobHandler)
	api.Get("/jobs/:job_id/tasks", s.listTasksHandler)
	api.Post("/jobs/:job_id/cancel", s.cancelJobHandler)
	api.Get("/clients", s.listClientsHandler)

	api.Post("/clients/register", s.registerClientHandler)
	api.Post("/clients/:client_id/heartbeat", s.heartbeatHandler)
	api.Post("/tasks/lease", s.leaseTaskHandler)
	api.Post("/tasks/:task_id/progress", s.progressHandler)
	api.Post("/tasks/:task_id/fail", s.failHandler)
	api.Post("/tasks/:task_id/result", s.resultHandler)

	// Admin endpoints for auditing and requeuing.
	admin := api.Group("/admin")
	admin.Post("/s3/audit", s.auditS3Handler)
	admin.Post("/requeue", s.requeueHandler)

	// Backward-compatible endpoint used by the old monolith.
	app.Post("/update_asset", s.authMiddleware, s.createJobHandler)
}

func (s *Server) authMiddleware(c fiber.Ctx) error {
	if !s.cfg.Server.Auth.Enabled {
		return c.Next()
	}
	if prefix := s.cfg.Server.Auth.UserAgentPrefix; prefix != "" {
		if !strings.HasPrefix(c.Get("User-Agent"), prefix) {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"message": "Invalid User-Agent"})
		}
	}
	if token := s.cfg.Server.Auth.BearerToken; token != "" {
		if c.Get("Authorization") != "Bearer "+token {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"message": "Invalid authorization token"})
		}
	}
	return c.Next()
}

func (s *Server) createAssetJob(ctx context.Context, req protocol.JobRequest, event string) (protocol.JobSnapshot, error) {
	downloaded, err := s.records.Load(req.Server)
	if err != nil {
		return protocol.JobSnapshot{}, fmt.Errorf("%w: %v", errLoadDownloadedAssetRecord, err)
	}
	resolvedReq, tasks, err := s.builder.BuildTasks(ctx, req, downloaded)
	if err != nil {
		return protocol.JobSnapshot{}, fmt.Errorf("%w: %v", errBuildAssetTasks, err)
	}
	job := s.scheduler.CreateJob(resolvedReq, tasks)
	s.notifyTasksAvailable()
	s.logJobProgress(event, job)
	return job, nil
}

func (s *Server) createJobHandler(c fiber.Ctx) error {
	var req protocol.JobRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	if req.Server == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "server is required"})
	}

	job, err := s.createAssetJob(context.Background(), req, "创建任务")
	if err != nil {
		if errors.Is(err, errLoadDownloadedAssetRecord) {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "failed to load downloaded asset record", "error": err.Error()})
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "failed to create asset job", "error": err.Error()})
	}
	return c.Status(fiber.StatusOK).JSON(protocol.CreateJobResponse{Message: "Asset updater started running", Job: job})
}

func (s *Server) getJobHandler(c fiber.Ctx) error {
	job, ok := s.scheduler.GetJob(c.Params("job_id"))
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"message": "job not found"})
	}
	return c.JSON(job)
}

func (s *Server) listTasksHandler(c fiber.Ctx) error {
	tasks, ok := s.scheduler.ListTasks(c.Params("job_id"))
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"message": "job not found"})
	}
	return c.JSON(fiber.Map{"tasks": tasks})
}

func (s *Server) cancelJobHandler(c fiber.Ctx) error {
	if !s.cfg.Execution.AllowCancel {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"message": "cancel is disabled"})
	}
	job, ok := s.scheduler.CancelJob(c.Params("job_id"))
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"message": "job not found"})
	}
	s.logJobProgress("取消任务", job)
	return c.JSON(job)
}

func (s *Server) listClientsHandler(c fiber.Ctx) error {
	return c.JSON(fiber.Map{"clients": s.scheduler.ListClients()})
}

func (s *Server) registerClientHandler(c fiber.Ctx) error {
	var req protocol.ClientRegistrationRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	return c.JSON(s.scheduler.Register(req))
}

func (s *Server) heartbeatHandler(c fiber.Ctx) error {
	var req protocol.HeartbeatRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	if req.ClientID == "" {
		req.ClientID = c.Params("client_id")
	}
	if err := s.scheduler.Heartbeat(req); err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"message": err.Error()})
	}
	return c.JSON(fiber.Map{"ok": true})
}

func (s *Server) leaseTaskHandler(c fiber.Ctx) error {
	var req protocol.LeaseRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	deadline := time.Now().Add(time.Duration(req.WaitSeconds) * time.Second)
	for {
		tasks, err := s.scheduler.Lease(req)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
		}
		if len(tasks) > 0 || req.WaitSeconds <= 0 || time.Now().After(deadline) {
			return c.JSON(protocol.LeaseResponse{Tasks: tasks})
		}
		time.Sleep(time.Second)
	}
}

func (s *Server) progressHandler(c fiber.Ctx) error {
	var req protocol.ProgressRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	if err := s.scheduler.Progress(c.Params("task_id"), req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
	return c.JSON(fiber.Map{"ok": true})
}

func (s *Server) failHandler(c fiber.Ctx) error {
	var req protocol.FailRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	if err := s.failTaskAndLog(c.Params("task_id"), req.ClientID, req.Error, "任务失败"); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
	return c.JSON(fiber.Map{"ok": true})
}

func (s *Server) resultHandler(c fiber.Ctx) error {
	taskID := c.Params("task_id")
	payload, ok := s.scheduler.TaskPayload(taskID)
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"message": "task not found"})
	}
	if payload.Delayed {
		s.logger.Infof("接收延迟任务结果 task=%s bundle=%s estimated_size=%d", taskID, payload.BundlePath, payload.EstimatedSizeBytes)
	}
	manifest, archivePath, stagingDir, err := s.saveResultUpload(c, taskID)
	if err != nil {
		_ = s.failTaskAndLog(taskID, "", err.Error(), "结果上传失败")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "invalid result upload", "error": err.Error()})
	}
	defer s.cleanupStaging(stagingDir)

	if err := validateManifestIdentity(manifest, payload); err != nil {
		_ = s.failTaskAndLog(taskID, "", err.Error(), "结果校验失败")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "invalid manifest", "error": err.Error()})
	}

	extractDir := filepath.Join(stagingDir, "extract")
	if err := extractArchive(archivePath, extractDir); err != nil {
		_ = s.failTaskAndLog(taskID, "", err.Error(), "结果解压失败")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "failed to extract result", "error": err.Error()})
	}
	if err := validateManifestFiles(extractDir, manifest.Files); err != nil {
		_ = s.failTaskAndLog(taskID, "", err.Error(), "结果清单校验失败")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "manifest verification failed", "error": err.Error()})
	}
	if err := validateExpectedOutputs(manifest, payload.Export); err != nil {
		_ = s.failTaskAndLog(taskID, "", err.Error(), "期望产物校验失败")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "expected output validation failed", "error": err.Error()})
	}
	if err := s.scheduler.MarkUploadingS3(taskID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
	if err := s.uploader.UploadManifestFiles(context.Background(), extractDir, manifest.Region, manifest.Files, storage.UploadMeta{
		TaskID:     manifest.TaskID,
		JobID:      manifest.JobID,
		ClientID:   s.scheduler.TaskClientID(taskID),
		BundlePath: manifest.BundlePath,
		BundleHash: manifest.BundleHash,
	}); err != nil {
		_ = s.failTaskAndLog(taskID, "", err.Error(), "S3上传失败")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "failed to upload result to storage", "error": err.Error()})
	}
	if err := s.records.Mark(manifest.Region, manifest.BundlePath, manifest.BundleHash); err != nil {
		_ = s.failTaskAndLog(taskID, "", err.Error(), "记录更新失败")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "failed to update downloaded asset record", "error": err.Error()})
	}
	completed, err := s.scheduler.Complete(taskID)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
	s.notifyTasksAvailable()
	s.logJobProgressByID("任务完成", completed.JobID)
	return c.JSON(fiber.Map{"ok": true, "task_id": completed.TaskID})
}

func (s *Server) saveResultUpload(c fiber.Ctx, taskID string) (protocol.TaskResultManifest, string, string, error) {
	stagingDir := filepath.Join(s.cfg.Server.StagingDir, taskID, fmt.Sprintf("%d", time.Now().UnixNano()))
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return protocol.TaskResultManifest{}, "", "", err
	}

	manifest, err := readManifestFromRequest(c, stagingDir)
	if err != nil {
		return protocol.TaskResultManifest{}, "", stagingDir, err
	}
	archiveHeader, err := c.FormFile("archive")
	if err != nil {
		return protocol.TaskResultManifest{}, "", stagingDir, fmt.Errorf("archive file is required: %w", err)
	}
	archivePath := filepath.Join(stagingDir, "result.tar.gz")
	if err := c.SaveFile(archiveHeader, archivePath); err != nil {
		return protocol.TaskResultManifest{}, "", stagingDir, fmt.Errorf("save archive: %w", err)
	}
	return manifest, archivePath, stagingDir, nil
}

func readManifestFromRequest(c fiber.Ctx, stagingDir string) (protocol.TaskResultManifest, error) {
	if raw := c.FormValue("manifest"); raw != "" && strings.HasPrefix(strings.TrimSpace(raw), "{") {
		var manifest protocol.TaskResultManifest
		if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
			return protocol.TaskResultManifest{}, fmt.Errorf("parse manifest field: %w", err)
		}
		return manifest, nil
	}
	manifestHeader, err := c.FormFile("manifest")
	if err != nil {
		return protocol.TaskResultManifest{}, fmt.Errorf("manifest file is required: %w", err)
	}
	manifestPath := filepath.Join(stagingDir, "manifest.json")
	if err := c.SaveFile(manifestHeader, manifestPath); err != nil {
		return protocol.TaskResultManifest{}, fmt.Errorf("save manifest: %w", err)
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return protocol.TaskResultManifest{}, err
	}
	var manifest protocol.TaskResultManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return protocol.TaskResultManifest{}, fmt.Errorf("parse manifest file: %w", err)
	}
	return manifest, nil
}

func validateManifestIdentity(manifest protocol.TaskResultManifest, payload protocol.TaskPayload) error {
	if manifest.TaskID != payload.TaskID || manifest.JobID != payload.JobID {
		return fmt.Errorf("manifest task/job does not match leased task")
	}
	if manifest.Region != payload.Region || manifest.BundlePath != payload.BundlePath || manifest.BundleHash != payload.BundleHash {
		return fmt.Errorf("manifest bundle identity does not match leased task")
	}
	return nil
}

func extractArchive(archivePath string, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if header.FileInfo().IsDir() {
			continue
		}
		name := filepath.ToSlash(header.Name)
		if !isSafeRelativePath(name) {
			return fmt.Errorf("unsafe archive path %q", header.Name)
		}
		target := filepath.Join(destDir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, tr)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func validateManifestFiles(baseDir string, files []protocol.ResultFile) error {
	seen := make(map[string]bool, len(files))
	for _, file := range files {
		if !isSafeRelativePath(file.Path) {
			return fmt.Errorf("unsafe manifest path %q", file.Path)
		}
		if seen[file.Path] {
			return fmt.Errorf("duplicate manifest path %q", file.Path)
		}
		seen[file.Path] = true
		path := filepath.Join(baseDir, filepath.FromSlash(file.Path))
		stat, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("stat %s: %w", file.Path, err)
		}
		if stat.IsDir() {
			return fmt.Errorf("manifest path %s is a directory", file.Path)
		}
		if stat.Size() != file.Size {
			return fmt.Errorf("size mismatch for %s", file.Path)
		}
		sha, err := fileSHA256(path)
		if err != nil {
			return err
		}
		if !strings.EqualFold(sha, file.SHA256) {
			return fmt.Errorf("sha256 mismatch for %s", file.Path)
		}
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func isSafeRelativePath(path string) bool {
	if path == "" || strings.Contains(path, "\\") || strings.Contains(path, ":") {
		return false
	}
	cleaned := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	return cleaned == path && !strings.HasPrefix(cleaned, "../") && cleaned != ".." && !strings.HasPrefix(cleaned, "/")
}

func (s *Server) cleanupStaging(dir string) {
	if s.cfg.Server.ResultRetention || dir == "" {
		return
	}
	_ = os.RemoveAll(dir)
}

// validateExpectedOutputs checks that the manifest contains expected output
// files based on the export options. This catches cases where ffmpeg silently
// fails and the client uploads results without the expected converted files.
func validateExpectedOutputs(manifest protocol.TaskResultManifest, export protocol.ExportOptions) error {
	counts := make(map[string]int)
	samples := make(map[string][]string)
	for _, f := range manifest.Files {
		ext := strings.ToLower(filepath.Ext(f.Path))
		switch ext {
		case ".wav", ".mp3", ".flac", ".m2v", ".mp4", ".png", ".webp":
			counts[ext]++
			if len(samples[ext]) < 3 {
				samples[ext] = append(samples[ext], f.Path)
			}
		}
	}
	has := func(ext string) bool { return counts[ext] > 0 }

	// Audio: if WAV→MP3 conversion is enabled but we see .wav without .mp3, something went wrong.
	if export.ConvertAudioToMP3 && export.ExportACBFiles && export.DecodeACBFiles {
		if has(".wav") && !has(".mp3") {
			return fmt.Errorf("export requires mp3 conversion but result contains .wav without any .mp3 files (%s)", outputExtSummary(counts, samples, ".wav", ".mp3"))
		}
	}
	// Audio: if WAV→FLAC conversion is enabled but we see .wav without .flac.
	if export.ConvertWavToFLAC && export.ExportACBFiles && export.DecodeACBFiles {
		if has(".wav") && !has(".flac") {
			return fmt.Errorf("export requires flac conversion but result contains .wav without any .flac files (%s)", outputExtSummary(counts, samples, ".wav", ".flac"))
		}
	}
	// Audio: if remove_wav is enabled but .wav files are still present (and conversion was requested).
	if export.RemoveWav && (export.ConvertAudioToMP3 || export.ConvertWavToFLAC) {
		if has(".wav") {
			return fmt.Errorf("export requires wav removal but result still contains .wav files (%s)", outputExtSummary(counts, samples, ".wav", ".mp3", ".flac"))
		}
	}
	// Video: if M2V→MP4 conversion is enabled but we see .m2v without .mp4.
	if export.ConvertVideoToMP4 {
		if has(".m2v") && !has(".mp4") {
			return fmt.Errorf("export requires mp4 conversion but result contains .m2v without any .mp4 files (%s)", outputExtSummary(counts, samples, ".m2v", ".mp4"))
		}
	}
	// Images: if PNG→WebP conversion is enabled with remove_png, but .png files remain without .webp.
	if export.ConvertPhotoToWebP && export.RemovePNG {
		if has(".png") && !has(".webp") {
			return fmt.Errorf("export requires webp conversion with png removal but result contains .png without any .webp files (%s)", outputExtSummary(counts, samples, ".png", ".webp"))
		}
	}
	return nil
}

func outputExtSummary(counts map[string]int, samples map[string][]string, exts ...string) string {
	parts := make([]string, 0, len(exts)*2)
	for _, ext := range exts {
		label := strings.TrimPrefix(ext, ".")
		parts = append(parts, fmt.Sprintf("%s=%d", label, counts[ext]))
		if len(samples[ext]) > 0 {
			parts = append(parts, fmt.Sprintf("%s_samples=%v", label, samples[ext]))
		}
	}
	return strings.Join(parts, " ")
}

// --- Admin handlers ---

func (s *Server) auditS3Handler(c fiber.Ctx) error {
	var req protocol.AuditS3Request
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	if req.Region == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "region is required"})
	}
	resp, err := s.uploader.AuditS3(context.Background(), req)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "S3 audit failed", "error": err.Error()})
	}
	return c.JSON(resp)
}

func (s *Server) requeueHandler(c fiber.Ctx) error {
	var req protocol.RequeueRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	if req.Region == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "region is required"})
	}
	if len(req.BundlePaths) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "bundle_paths is required"})
	}

	resp := protocol.RequeueResponse{}

	// Step 1: Clear downloaded asset records for the specified bundles.
	if req.ClearRecord {
		removed, err := s.records.RemoveBundles(req.Region, req.BundlePaths)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "failed to remove records", "error": err.Error()})
		}
		resp.RecordsRemoved = removed
	}

	// Step 2: Optionally delete S3 objects.
	if req.DeleteS3Objects && len(req.S3KeysToDelete) > 0 {
		deleted, err := s.uploader.DeleteS3Objects(context.Background(), req.Region, req.S3KeysToDelete)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message":         "partial S3 deletion",
				"error":           err.Error(),
				"records_removed": resp.RecordsRemoved,
				"s3_deleted":      deleted,
			})
		}
		resp.S3Deleted = deleted
	}

	resp.Message = fmt.Sprintf("removed %d records, deleted %d S3 objects; create a new job for region %s to re-process these bundles", resp.RecordsRemoved, resp.S3Deleted, req.Region)
	return c.JSON(resp)
}
