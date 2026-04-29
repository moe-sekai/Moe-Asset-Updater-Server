package tcpserver

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"moe-asset-server/internal/protocol"
)

type clientConn struct {
	server   *Server
	conn     net.Conn
	clientID string
	maxTasks int

	sendMu    sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
	wakeCh    chan struct{}
}

func newClientConn(server *Server, conn net.Conn) *clientConn {
	return &clientConn{
		server: server,
		conn:   conn,
		done:   make(chan struct{}),
		wakeCh: make(chan struct{}, 1),
	}
}

func (c *clientConn) run(parent context.Context) {
	if done := parent.Done(); done != nil {
		go func() {
			<-done
			c.close()
		}()
	}
	defer c.close()

	if c.server.logger != nil {
		c.server.logger.Infof("TCP client connected from %s", c.conn.RemoteAddr())
	}

	first, err := c.read()
	if err != nil {
		if c.server.logger != nil {
			c.server.logger.Warnf("TCP client %s failed before register: %v", c.conn.RemoteAddr(), err)
		}
		return
	}
	if first.Type != MessageRegister {
		_ = c.send(Message{Type: MessageError, Error: "first tcp message must be register"})
		return
	}
	if err := c.authenticate(first); err != nil {
		_ = c.send(Message{Type: MessageError, Error: err.Error()})
		if c.server.logger != nil {
			c.server.logger.Warnf("TCP client %s auth failed: %v", c.conn.RemoteAddr(), err)
		}
		return
	}

	maxTasks := first.MaxTasks
	if maxTasks <= 0 {
		maxTasks = 1
	}
	registered := c.server.scheduler.Register(protocol.ClientRegistrationRequest{
		ClientID: first.ClientID,
		Name:     first.Name,
		Version:  first.Version,
		MaxTasks: maxTasks,
		Tags:     first.Tags,
	})
	c.clientID = registered.ClientID
	c.maxTasks = maxTasks
	c.server.addConn(c)
	defer c.server.removeConn(c)

	if len(first.ActiveTaskIDs) > 0 {
		_ = c.server.scheduler.Heartbeat(protocol.HeartbeatRequest{ClientID: c.clientID, ActiveTaskIDs: first.ActiveTaskIDs})
	}
	if err := c.send(Message{Type: MessageRegistered, ClientID: c.clientID, HeartbeatAfter: registered.HeartbeatAfter, LeaseTTL: registered.LeaseTTL}); err != nil {
		return
	}
	if c.server.logger != nil {
		c.server.logger.Infof("TCP client %s registered from %s max_tasks=%d", c.clientID, c.conn.RemoteAddr(), c.maxTasks)
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() {
		select {
		case <-c.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	go c.leaseLoop(ctx)
	c.wake()

	for {
		msg, err := c.read()
		if err != nil {
			if parent.Err() == nil && c.server.logger != nil {
				c.server.logger.Warnf("TCP client %s disconnected: %v", c.clientID, err)
			}
			return
		}
		if err := c.handleMessage(msg); err != nil {
			if c.server.logger != nil {
				c.server.logger.Warnf("TCP client %s message %s failed: %v", c.clientID, msg.Type, err)
			}
			_ = c.send(Message{Type: MessageError, Error: err.Error()})
		}
	}
}

func (c *clientConn) authenticate(msg Message) error {
	auth := c.server.cfg.Server.Auth
	if !auth.Enabled {
		return nil
	}
	if auth.UserAgentPrefix != "" && !strings.HasPrefix(msg.UserAgent, auth.UserAgentPrefix) {
		return fmt.Errorf("invalid user agent")
	}
	if auth.BearerToken != "" && msg.BearerToken != auth.BearerToken {
		return fmt.Errorf("invalid bearer token")
	}
	return nil
}

func (c *clientConn) read() (Message, error) {
	if timeout := c.server.cfg.Server.TCP.ReadTimeoutSeconds; timeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
	}
	return ReadMessage(c.conn)
}

func (c *clientConn) send(msg Message) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if timeout := c.server.cfg.Server.TCP.WriteTimeoutSeconds; timeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
	}
	return WriteMessage(c.conn, msg)
}

func (c *clientConn) handleMessage(msg Message) error {
	switch msg.Type {
	case MessageHeartbeat:
		return c.server.scheduler.Heartbeat(protocol.HeartbeatRequest{ClientID: c.clientID, ActiveTaskIDs: msg.ActiveTaskIDs})
	case MessageProgress:
		if msg.TaskID == "" {
			return fmt.Errorf("task_id is required for progress")
		}
		return c.server.scheduler.Progress(msg.TaskID, protocol.ProgressRequest{ClientID: c.clientID, Stage: msg.Stage, Progress: msg.Progress, Message: msg.Message})
	case MessageFail:
		if msg.TaskID == "" {
			return fmt.Errorf("task_id is required for fail")
		}
		if err := c.server.scheduler.Fail(msg.TaskID, c.clientID, msg.Error); err != nil {
			return err
		}
		c.server.Notify()
		return nil
	default:
		return fmt.Errorf("unsupported tcp message type %q", msg.Type)
	}
}

func (c *clientConn) leaseLoop(ctx context.Context) {
	interval := c.server.cfg.Server.TCP.LeasePollInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := c.leaseOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			if c.server.logger != nil {
				c.server.logger.Warnf("TCP lease for client %s failed: %v", c.clientID, err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-c.wakeCh:
		case <-ticker.C:
		}
	}
}

func (c *clientConn) leaseOnce(ctx context.Context) error {
	if c.clientID == "" {
		return nil
	}
	tasks, err := c.server.scheduler.Lease(protocol.LeaseRequest{ClientID: c.clientID, MaxTasks: c.maxTasks})
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	if err := c.send(Message{Type: MessageTaskPush, Tasks: tasks}); err != nil {
		c.close()
		return err
	}
	if c.server.logger != nil {
		c.server.logger.Infof("pushed %d task(s) to TCP client %s", len(tasks), c.clientID)
	}
	return nil
}

func (c *clientConn) wake() {
	select {
	case c.wakeCh <- struct{}{}:
	default:
	}
}

func (c *clientConn) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}
