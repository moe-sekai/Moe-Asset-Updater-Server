package tcpserver

import (
	"context"
	"errors"
	"net"
	"sync"

	"moe-asset-server/internal/config"
	harukiLogger "moe-asset-server/internal/logger"
	"moe-asset-server/internal/scheduler"
)

type Server struct {
	cfg       *config.Config
	logger    *harukiLogger.Logger
	scheduler *scheduler.Manager

	sem   chan struct{}
	mu    sync.Mutex
	conns map[string]*clientConn
}

func New(cfg *config.Config, logger *harukiLogger.Logger, manager *scheduler.Manager) *Server {
	maxConnections := cfg.Server.TCP.MaxConnections
	if maxConnections <= 0 {
		maxConnections = 100
	}
	return &Server{
		cfg:       cfg,
		logger:    logger,
		scheduler: manager,
		sem:       make(chan struct{}, maxConnections),
		conns:     make(map[string]*clientConn),
	}
}

func (s *Server) Start(ctx context.Context) error {
	addr := s.cfg.Server.TCP.ListenAddress()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	if s.logger != nil {
		s.logger.Infof("TCP task server listening on %s", addr)
	}
	if done := ctx.Done(); done != nil {
		go func() {
			<-done
			_ = ln.Close()
		}()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return ctx.Err()
			}
			if s.logger != nil {
				s.logger.Warnf("TCP accept failed: %v", err)
			}
			continue
		}
		select {
		case s.sem <- struct{}{}:
			client := newClientConn(s, conn)
			go func() {
				defer func() { <-s.sem }()
				client.run(ctx)
			}()
		default:
			if s.logger != nil {
				s.logger.Warnf("TCP connection limit reached; rejecting %s", conn.RemoteAddr())
			}
			_ = conn.Close()
		}
	}
}

func (s *Server) Notify() {
	s.mu.Lock()
	conns := make([]*clientConn, 0, len(s.conns))
	for _, conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()
	for _, conn := range conns {
		conn.wake()
	}
}

func (s *Server) addConn(conn *clientConn) {
	var old *clientConn
	s.mu.Lock()
	if existing := s.conns[conn.clientID]; existing != nil && existing != conn {
		old = existing
	}
	s.conns[conn.clientID] = conn
	s.mu.Unlock()
	if old != nil {
		old.close()
	}
}

func (s *Server) removeConn(conn *clientConn) {
	s.mu.Lock()
	if existing := s.conns[conn.clientID]; existing == conn {
		delete(s.conns, conn.clientID)
	}
	s.mu.Unlock()
}
