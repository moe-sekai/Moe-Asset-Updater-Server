package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"moe-asset-server/internal/api"
	"moe-asset-server/internal/config"
	harukiLogger "moe-asset-server/internal/logger"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/logger"
)

var Version = "dev"

func main() {
	configPath := flag.String("config", "", "path to server config yaml")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	logWriter := io.Writer(os.Stdout)
	if cfg.Logging.File != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.Logging.File), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create log directory: %v\n", err)
			os.Exit(1)
		}
		file, err := os.OpenFile(cfg.Logging.File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open log file: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = file.Close() }()
		logWriter = io.MultiWriter(os.Stdout, file)
	}
	mainLogger := harukiLogger.NewLogger("Server", cfg.Logging.Level, logWriter)
	mainLogger.Infof("========================= Moe Asset Server %s =========================", Version)

	app := fiber.New(fiber.Config{BodyLimit: cfg.Server.BodyLimitMB * 1024 * 1024})
	if cfg.Logging.Access.Enabled {
		logCfg := logger.Config{Format: cfg.Logging.Access.Format}
		if cfg.Logging.Access.File != "" {
			if err := os.MkdirAll(filepath.Dir(cfg.Logging.Access.File), 0o755); err != nil {
				mainLogger.Errorf("failed to create access log directory: %v", err)
				os.Exit(1)
			}
			accessFile, err := os.OpenFile(cfg.Logging.Access.File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				mainLogger.Errorf("failed to open access log file: %v", err)
				os.Exit(1)
			}
			defer func() { _ = accessFile.Close() }()
			logCfg.Stream = accessFile
		}
		app.Use(logger.New(logCfg))
	}

	server := api.New(cfg, mainLogger)
	server.RegisterRoutes(app)
	server.StartGitHashWatcher(context.Background())

	listenCfg := fiber.ListenConfig{DisableStartupMessage: true}
	serverType := "HTTP"
	if cfg.Server.TLS.Enabled {
		serverType = "HTTPS"
		listenCfg.CertFile = cfg.Server.TLS.CertFile
		listenCfg.CertKeyFile = cfg.Server.TLS.KeyFile
	}
	mainLogger.Infof("Start listening %s server on %s...", serverType, cfg.ListenAddress())
	if err := app.Listen(cfg.ListenAddress(), listenCfg); err != nil {
		mainLogger.Errorf("failed to start server: %v", err)
		os.Exit(1)
	}
}
