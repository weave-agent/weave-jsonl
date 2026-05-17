package jsonlstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/weave-agent/weave/sdk"
)

var logger = sdk.Logger("jsonl")

// EventTypeMessage is the event type for message entries.
const EventTypeMessage = "message"

// compactionSummaryPrefix matches the prefix used by the agent extension
// when creating compaction summary messages.
const compactionSummaryPrefix = "[Compaction Summary]\n"

type SessionHeader struct {
	Type      string    `json:"type"`
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	CWD       string    `json:"cwd"`
}

type Entry struct {
	ID       string          `json:"id"`
	ParentID string          `json:"parent_id,omitempty"`
	Type     string          `json:"type"`
	Turn     int             `json:"turn"`
	Data     json.RawMessage `json:"data"`
	Created  time.Time       `json:"created"`
}

type Session struct {
	Header  SessionHeader
	Entries []Entry
}

type SessionInfo struct {
	ID         string    `json:"id"`
	CWD        string    `json:"cwd"`
	EntryCount int       `json:"entry_count"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type JSONLOpts struct {
	Dir string `json:"dir" default:"" env:"DIR" description:"Session directory (default: ~/.weave/sessions)"`
}

type Store struct {
	cfg JSONLOpts

	mu        sync.Mutex
	sessionID string
	lastEntry string
	turn      int
	resumed   bool // true after session.resume until first agent.prompt
	cancel    context.CancelFunc
	done      chan struct{}
}

func init() {
	sdk.RegisterExtensionWithScope("jsonl", "jsonl", func(_ sdk.Config, _ sdk.PreferenceReader, opts JSONLOpts) (sdk.Extension, error) {
		return NewStore(opts)
	})
}

func NewStore(opts JSONLOpts) (*Store, error) {
	return &Store{cfg: opts}, nil
}

func (s *Store) Name() string { return "jsonl" }

func (s *Store) Subscribe(bus sdk.Bus) error {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return errors.New("jsonl: Subscribe called twice without Close")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()

	ch := make(chan sdk.Event, 256)

	bus.OnAll(func(ev sdk.Event) error {
		switch ev.Topic {
		case "agent.prompt", "agent.followup", "agent.steer",
			"agent.turn_start", "agent.message_end",
			"agent.tool_result", "agent.end", "session.resume":
			select {
			case ch <- ev:
			case <-ctx.Done():
			}
		}

		return nil
	})

	go func() {
		defer close(s.done)

		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					return
				}

				s.dispatch(ev)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if done != nil {
		<-done
	}

	return nil
}

func (s *Store) dispatch(ev sdk.Event) {
	switch ev.Topic {
	case "agent.prompt":
		s.handlePrompt(ev)
	case "agent.followup":
		s.handleFollowup(ev)
	case "agent.steer":
		s.handleSteer(ev)
	case "agent.turn_start":
		s.handleTurnStart(ev)
	case "agent.message_end":
		s.handleMsgEnd(ev)
	case "agent.tool_result":
		s.handleToolResult(ev)
	case "session.resume":
		s.handleSessionResume(ev)
	case "agent.end":
		s.cancel()
	}
}

func (s *Store) handlePrompt(evt sdk.Event) {
	s.mu.Lock()
	sessionID := s.sessionID
	lastEntry := s.lastEntry
	turn := s.turn
	resumed := s.resumed
	s.mu.Unlock()

	// Session was resumed; treat the first prompt after resume as a follow-up
	// instead of creating a new session. After that, agent.prompt starts a new
	// conversation (e.g. /new).
	if sessionID != "" && resumed {
		s.appendUserEntry(sessionID, turn+1, lastEntry, evt)
		s.mu.Lock()
		s.turn = turn + 1
		s.resumed = false
		s.mu.Unlock()

		return
	}

	cwd, _ := os.Getwd()
	if cwd == "" {
		cwd = "/"
	}

	sess, err := s.Create(cwd)
	if err != nil {
		logger.Error("jsonl: create session", "error", err)
		return
	}

	data, _ := json.Marshal(map[string]any{
		"role":    sdk.RoleUser,
		"content": evt.Payload,
	})

	entry := Entry{
		Type: EventTypeMessage,
		Turn: 1,
		Data: data,
	}

	entryID, err := s.Append(sess.Header.ID, entry)
	if err != nil {
		logger.Error("jsonl: append entry", "error", err)
		return
	}

	s.mu.Lock()
	s.sessionID = sess.Header.ID
	s.lastEntry = entryID
	s.turn = 1
	s.mu.Unlock()
}

func (s *Store) handleSessionResume(evt sdk.Event) {
	payload, ok := evt.Payload.(sdk.SessionResumePayload)
	if !ok {
		logger.Error("jsonl: invalid session.resume payload type")
		return
	}

	if payload.SessionID == "" {
		return
	}

	var lastEntryID string

	sess, err := s.Load(payload.SessionID)
	if err != nil {
		logger.Warn("jsonl: load session for resume failed, continuing with empty state", "session", payload.SessionID, "error", err)
	} else if len(sess.Entries) > 0 {
		last := sess.Entries[len(sess.Entries)-1]
		lastEntryID = last.ID
	}

	s.mu.Lock()
	s.sessionID = payload.SessionID
	s.lastEntry = lastEntryID
	s.resumed = true
	s.mu.Unlock()
}

func (s *Store) handleFollowup(evt sdk.Event) {
	s.mu.Lock()
	sessionID := s.sessionID
	lastEntry := s.lastEntry
	turn := s.turn + 1
	resumed := s.resumed
	s.mu.Unlock()

	s.appendUserEntry(sessionID, turn, lastEntry, evt)

	if resumed {
		s.mu.Lock()
		s.resumed = false
		s.mu.Unlock()
	}
}

func (s *Store) handleSteer(evt sdk.Event) {
	s.mu.Lock()
	sessionID := s.sessionID
	lastEntry := s.lastEntry
	turn := s.turn
	s.mu.Unlock()

	s.appendUserEntry(sessionID, turn, lastEntry, evt)
}

func (s *Store) appendUserEntry(sessionID string, turn int, parentID string, evt sdk.Event) {
	if sessionID == "" {
		return
	}

	data, _ := json.Marshal(map[string]any{
		"role":    sdk.RoleUser,
		"content": evt.Payload,
	})

	entry := Entry{
		ParentID: parentID,
		Type:     EventTypeMessage,
		Turn:     turn,
		Data:     data,
	}

	entryID, err := s.Append(sessionID, entry)
	if err != nil {
		logger.Error("jsonl: append user input", "error", err)
		return
	}

	s.mu.Lock()
	s.lastEntry = entryID
	s.mu.Unlock()
}

func (s *Store) handleTurnStart(evt sdk.Event) {
	turn, ok := evt.Payload.(int)
	if !ok {
		return
	}

	s.mu.Lock()
	s.turn = turn
	s.mu.Unlock()
}

func (s *Store) handleMsgEnd(evt sdk.Event) {
	s.mu.Lock()
	sessionID := s.sessionID
	lastEntry := s.lastEntry
	turn := s.turn
	s.mu.Unlock()

	if sessionID == "" {
		return
	}

	id, err := generateID()
	if err != nil {
		logger.Error("jsonl: generate id", "error", err)
		return
	}

	payload := map[string]any{
		"role": sdk.RoleAssistant,
	}

	switch p := evt.Payload.(type) {
	case map[string]any:
		if c, ok := p["content"]; ok {
			payload["content"] = c
		}

		if tc, ok := p["tool_calls"]; ok {
			payload["tool_calls"] = tc
		}

		if th, ok := p["thinking"]; ok {
			payload["thinking"] = th
		}

		if rt, ok := p["redacted_thinking"]; ok {
			payload["redacted_thinking"] = rt
		}
	case string:
		payload["content"] = p
	}

	data, _ := json.Marshal(payload)

	entry := Entry{
		ID:       id,
		ParentID: lastEntry,
		Type:     EventTypeMessage,
		Turn:     turn,
		Data:     data,
	}

	if _, err := s.Append(sessionID, entry); err != nil {
		logger.Error("jsonl: append entry", "error", err)
		return
	}

	s.mu.Lock()
	s.lastEntry = id
	s.mu.Unlock()
}

func (s *Store) handleToolResult(evt sdk.Event) {
	s.mu.Lock()
	sessionID := s.sessionID
	lastEntry := s.lastEntry
	turn := s.turn
	s.mu.Unlock()

	if sessionID == "" {
		return
	}

	id, err := generateID()
	if err != nil {
		logger.Error("jsonl: generate id", "error", err)
		return
	}

	data, _ := json.Marshal(map[string]any{
		"role": sdk.RoleToolResult,
		"tool": evt.Payload,
	})

	entry := Entry{
		ID:       id,
		ParentID: lastEntry,
		Type:     EventTypeMessage,
		Turn:     turn,
		Data:     data,
	}

	if _, err := s.Append(sessionID, entry); err != nil {
		logger.Error("jsonl: append entry", "error", err)
		return
	}

	s.mu.Lock()
	s.lastEntry = id
	s.mu.Unlock()
}

func generateID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}

	return hex.EncodeToString(b), nil
}

func (s *Store) sessionDir() (string, error) {
	if s.cfg.Dir != "" {
		return s.cfg.Dir, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("session dir: %w", err)
	}

	return filepath.Join(home, ".weave", "sessions"), nil
}

func (s *Store) sessionPath(sessionID string) (string, error) {
	if !isValidID(sessionID) {
		return "", fmt.Errorf("invalid session ID: %q", sessionID)
	}

	dir, err := s.sessionDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, sessionID+".jsonl"), nil
}

func isValidID(id string) bool {
	if id == "" {
		return false
	}

	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}

func (s *Store) Create(cwd string) (*Session, error) {
	id, err := generateID()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	sess := &Session{
		Header: SessionHeader{
			Type:      "session",
			ID:        id,
			Timestamp: now,
			CWD:       cwd,
		},
	}

	dir, err := s.sessionDir()
	if err != nil {
		return nil, err
	}

	if err = os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create session dir: %w", err)
	}

	line, err := json.Marshal(sess.Header)
	if err != nil {
		return nil, fmt.Errorf("marshal header: %w", err)
	}

	path := filepath.Join(dir, id+".jsonl")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create session file: %w", err)
	}
	defer f.Close()

	if _, err := fmt.Fprintf(f, "%s\n", line); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	return sess, nil
}

func (s *Store) Append(sessionID string, entry Entry) (string, error) {
	if entry.ID == "" {
		id, err := generateID()
		if err != nil {
			return "", err
		}

		entry.ID = id
	}

	if entry.Created.IsZero() {
		entry.Created = time.Now().UTC()
	}

	line, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("marshal entry: %w", err)
	}

	path, err := s.sessionPath(sessionID)
	if err != nil {
		return "", err
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return "", fmt.Errorf("open session file: %w", err)
	}
	defer f.Close()

	if _, err := fmt.Fprintf(f, "%s\n", line); err != nil {
		return "", fmt.Errorf("append entry: %w", err)
	}

	return entry.ID, nil
}

func loadFromFile(path string) (*Session, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read session file: %w", err)
	}

	lines := splitLines(data)
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty session file: %s", path)
	}

	var header SessionHeader
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	entries := make([]Entry, 0, len(lines)-1)
	for _, line := range lines[1:] {
		var entry Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("parse entry: %w", err)
		}

		entries = append(entries, entry)
	}

	return &Session{Header: header, Entries: entries}, nil
}

func (s *Store) Load(sessionID string) (*Session, error) {
	path, err := s.sessionPath(sessionID)
	if err != nil {
		return nil, err
	}

	return loadFromFile(path)
}

func (s *Store) History(sessionID string) ([]Entry, error) {
	sess, err := s.Load(sessionID)
	if err != nil {
		return nil, err
	}

	return sess.Entries, nil
}

func (s *Store) List() ([]SessionInfo, error) {
	dir, err := s.sessionDir()
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("read session dir: %w", err)
	}

	var infos []SessionInfo

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
			continue
		}

		path := filepath.Join(dir, e.Name())

		sess, err := loadFromFile(path)
		if err != nil {
			continue
		}

		fi, err := e.Info()
		if err != nil {
			continue
		}

		infos = append(infos, SessionInfo{
			ID:         sess.Header.ID,
			CWD:        sess.Header.CWD,
			EntryCount: len(sess.Entries),
			CreatedAt:  sess.Header.Timestamp,
			UpdatedAt:  fi.ModTime().UTC(),
		})
	}

	return infos, nil
}

// ListSessions implements sdk.SessionStore.
func (s *Store) ListSessions() ([]sdk.SessionInfo, error) {
	infos, err := s.List()
	if err != nil {
		return nil, err
	}

	if len(infos) == 0 {
		return []sdk.SessionInfo{}, nil
	}

	result := make([]sdk.SessionInfo, len(infos))
	for i, info := range infos {
		result[i] = sdk.SessionInfo{
			ID:        info.ID,
			CWD:       info.CWD,
			CreatedAt: info.CreatedAt,
			UpdatedAt: info.UpdatedAt,
		}
	}

	return result, nil
}

// LoadHistory implements sdk.SessionStore.
func (s *Store) LoadHistory(sessionID string) ([]sdk.Message, error) {
	path, err := s.sessionPath(sessionID)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read session file: %w", err)
	}

	lines := splitLines(data)
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty session file: %s", path)
	}

	messages := make([]sdk.Message, 0, len(lines)-1)
	for _, line := range lines[1:] {
		var entry Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			logger.Warn("jsonl: skip unparseable entry", "session", sessionID, "error", err)
			continue
		}

		if entry.Type != EventTypeMessage {
			continue
		}

		msg, err := entryToMessage(entry)
		if err != nil {
			logger.Warn("jsonl: skip corrupt entry", "session", sessionID, "entry", entry.ID, "error", err)
			continue
		}

		messages = append(messages, msg)
	}

	return filterCompactedMessages(messages), nil
}

// filterCompactedMessages scans for the last compaction summary message
// (content starting with "[Compaction Summary]\n") and returns only that
// message and all messages after it. If no compaction summary is found,
// all messages are returned unchanged.
func filterCompactedMessages(messages []sdk.Message) []sdk.Message {
	lastSummaryIdx := -1

	for i, msg := range messages {
		if content, ok := msg.Content.(string); ok && strings.HasPrefix(content, compactionSummaryPrefix) {
			lastSummaryIdx = i
		}
	}

	if lastSummaryIdx < 0 {
		return messages
	}

	return messages[lastSummaryIdx:]
}

type rawMessage struct {
	Role             string          `json:"role"`
	Content          any             `json:"content"`
	ToolCalls        json.RawMessage `json:"tool_calls"`
	Tool             json.RawMessage `json:"tool"`
	Thinking         string          `json:"thinking"`
	RedactedThinking json.RawMessage `json:"redacted_thinking"`
}

func entryToMessage(entry Entry) (sdk.Message, error) {
	var raw rawMessage
	if err := json.Unmarshal(entry.Data, &raw); err != nil {
		return sdk.Message{}, fmt.Errorf("unmarshal entry data: %w", err)
	}

	msg := sdk.Message{
		Role:      raw.Role,
		Content:   raw.Content,
		Timestamp: entry.Created,
	}

	switch raw.Role {
	case sdk.RoleAssistant:
		if len(raw.ToolCalls) > 0 {
			var tcs []sdk.ToolCall
			if err := json.Unmarshal(raw.ToolCalls, &tcs); err != nil {
				return sdk.Message{}, fmt.Errorf("unmarshal tool_calls: %w", err)
			}

			msg.ToolCalls = tcs
		}

		if raw.Thinking != "" {
			msg.Thinking = []sdk.SignedThinking{{Thinking: raw.Thinking}}
		}

		if len(raw.RedactedThinking) > 0 {
			var rt []sdk.RedactedThinking
			if err := json.Unmarshal(raw.RedactedThinking, &rt); err != nil {
				return sdk.Message{}, fmt.Errorf("unmarshal redacted_thinking: %w", err)
			}

			msg.RedactedThinking = rt
		}
	case sdk.RoleToolResult:
		if err := applyToolResult(&msg, raw.Tool); err != nil {
			return sdk.Message{}, err
		}
	}

	return msg, nil
}

func applyToolResult(msg *sdk.Message, toolRaw json.RawMessage) error {
	if len(toolRaw) == 0 {
		return nil
	}

	var toolData map[string]any
	if err := json.Unmarshal(toolRaw, &toolData); err != nil {
		return fmt.Errorf("unmarshal tool data: %w", err)
	}

	if id, ok := toolData["id"].(string); ok {
		msg.ToolCallID = id
	}

	if name, ok := toolData["tool"].(string); ok {
		msg.ToolName = name
	}

	if result, ok := toolData["result"]; ok {
		applyToolResultContent(msg, result)
	}

	return nil
}

func applyToolResultContent(msg *sdk.Message, result any) {
	r, ok := result.(map[string]any)
	if !ok {
		msg.Content = result

		return
	}

	if content, ok := r["content"]; ok {
		msg.Content = content
	}

	if isErr, ok := r["is_error"].(bool); ok {
		msg.IsError = isErr
	}
}

func (s *Store) Compact(sessionID string, keepLast int) error {
	sess, err := s.Load(sessionID)
	if err != nil {
		return fmt.Errorf("compact load: %w", err)
	}

	if keepLast >= len(sess.Entries) {
		return nil
	}

	removed := sess.Entries[:len(sess.Entries)-keepLast]
	kept := sess.Entries[len(sess.Entries)-keepLast:]

	summaryID, err := generateID()
	if err != nil {
		return err
	}

	summary := Entry{
		ID:      summaryID,
		Type:    "summary",
		Turn:    removed[len(removed)-1].Turn,
		Data:    json.RawMessage(fmt.Sprintf(`{"removed_count":%d,"first_turn":%d,"last_turn":%d}`, len(removed), removed[0].Turn, removed[len(removed)-1].Turn)),
		Created: time.Now().UTC(),
	}

	if len(kept) > 0 {
		kept[0].ParentID = summary.ID
	}

	newEntries := make([]Entry, 0, 1+len(kept))
	newEntries = append(newEntries, summary)
	newEntries = append(newEntries, kept...)

	return s.rewriteFile(sessionID, sess.Header, newEntries)
}

func (s *Store) rewriteFile(sessionID string, header SessionHeader, entries []Entry) error {
	path, err := s.sessionPath(sessionID)
	if err != nil {
		return err
	}

	headerLine, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	}

	var lines []string

	lines = append(lines, string(headerLine))

	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal entry: %w", err)
		}

		lines = append(lines, string(line))
	}

	var buf []byte
	for _, l := range lines {
		buf = append(buf, l...)
		buf = append(buf, '\n')
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0o600); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp file: %w", err)
	}

	return nil
}

func splitLines(data []byte) []string {
	var lines []string

	for line := range bytes.SplitSeq(data, []byte{'\n'}) {
		if len(line) > 0 {
			lines = append(lines, string(line))
		}
	}

	return lines
}
