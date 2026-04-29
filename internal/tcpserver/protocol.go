package tcpserver

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"moe-asset-server/internal/protocol"
)

const (
	MessageRegister   = "register"
	MessageRegistered = "registered"
	MessageHeartbeat  = "heartbeat"
	MessageProgress   = "progress"
	MessageFail       = "fail"
	MessageTaskPush   = "task_push"
	MessageError      = "error"

	maxFrameSize = 32 * 1024 * 1024
)

type Message struct {
	Type           string                 `json:"type"`
	ClientID       string                 `json:"client_id,omitempty"`
	Name           string                 `json:"name,omitempty"`
	Version        string                 `json:"version,omitempty"`
	MaxTasks       int                    `json:"max_tasks,omitempty"`
	Tags           map[string]string      `json:"tags,omitempty"`
	UserAgent      string                 `json:"user_agent,omitempty"`
	BearerToken    string                 `json:"bearer_token,omitempty"`
	ActiveTaskIDs  []string               `json:"active_task_ids,omitempty"`
	TaskID         string                 `json:"task_id,omitempty"`
	Stage          protocol.ProgressStage `json:"stage,omitempty"`
	Progress       float64                `json:"progress,omitempty"`
	Message        string                 `json:"message,omitempty"`
	Error          string                 `json:"error,omitempty"`
	Tasks          []protocol.TaskPayload `json:"tasks,omitempty"`
	HeartbeatAfter int                    `json:"heartbeat_after_seconds,omitempty"`
	LeaseTTL       int                    `json:"lease_ttl_seconds,omitempty"`
}

func ReadMessage(r io.Reader) (Message, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Message{}, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		return Message{}, fmt.Errorf("empty tcp message")
	}
	if length > maxFrameSize {
		return Message{}, fmt.Errorf("tcp message too large: %d > %d", length, maxFrameSize)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Message{}, err
	}
	var msg Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		return Message{}, err
	}
	if msg.Type == "" {
		return Message{}, fmt.Errorf("tcp message type is required")
	}
	return msg, nil
}

func WriteMessage(w io.Writer, msg Message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > maxFrameSize {
		return fmt.Errorf("tcp message size out of range: %d", len(payload))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
