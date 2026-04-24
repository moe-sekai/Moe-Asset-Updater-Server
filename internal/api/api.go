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
	"moe-asset-server/internal/protocol"
	"moe-asset-server/internal/record"
	"moe-asset-server/internal/scheduler"
	"moe-asset-server/internal/storage"

	"github.com/gofiber/fiber/v3"
)

type Server struct {
	cfg       *config.Config
	builder   *catalog.Builder
	scheduler *scheduler.Manager
	records   *record.Manager
	uploader  *storage.Uploader
}

func New(cfg *config.Config) *Server {
	return &Server{
		cfg:       cfg,
		builder:   catalog.NewBuilder(cfg),
		scheduler: scheduler.NewManager(cfg),
		records:   record.NewManager(cfg),
		uploader:  storage.NewUploader(cfg),
	}
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

func (s *Server) createJobHandler(c fiber.Ctx) error {
	var req protocol.JobRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "Invalid request payload", "error": err.Error()})
	}
	if req.Server == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "server is required"})
	}

	downloaded, err := s.records.Load(req.Server)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "failed to load downloaded asset record", "error": err.Error()})
	}
	resolvedReq, tasks, err := s.builder.BuildTasks(context.Background(), req, downloaded)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "failed to create asset job", "error": err.Error()})
	}
	job := s.scheduler.CreateJob(resolvedReq, tasks)
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
	if err := s.scheduler.Fail(c.Params("task_id"), req.ClientID, req.Error); err != nil {
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
	manifest, archivePath, stagingDir, err := s.saveResultUpload(c, taskID)
	if err != nil {
		_ = s.scheduler.Fail(taskID, "", err.Error())
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "invalid result upload", "error": err.Error()})
	}
	defer s.cleanupStaging(stagingDir)

	if err := validateManifestIdentity(manifest, payload); err != nil {
		_ = s.scheduler.Fail(taskID, "", err.Error())
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "invalid manifest", "error": err.Error()})
	}

	extractDir := filepath.Join(stagingDir, "extract")
	if err := extractArchive(archivePath, extractDir); err != nil {
		_ = s.scheduler.Fail(taskID, "", err.Error())
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "failed to extract result", "error": err.Error()})
	}
	if err := validateManifestFiles(extractDir, manifest.Files); err != nil {
		_ = s.scheduler.Fail(taskID, "", err.Error())
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "manifest verification failed", "error": err.Error()})
	}
	if err := s.scheduler.MarkUploadingS3(taskID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
	if err := s.uploader.UploadManifestFiles(context.Background(), extractDir, manifest.Region, manifest.Files); err != nil {
		_ = s.scheduler.Fail(taskID, "", err.Error())
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "failed to upload result to storage", "error": err.Error()})
	}
	if err := s.records.Mark(manifest.Region, manifest.BundlePath, manifest.BundleHash); err != nil {
		_ = s.scheduler.Fail(taskID, "", err.Error())
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "failed to update downloaded asset record", "error": err.Error()})
	}
	completed, err := s.scheduler.Complete(taskID)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
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
