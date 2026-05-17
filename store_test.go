package jsonlstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	eventbus "github.com/weave-agent/weave/bus"

	"github.com/weave-agent/weave/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()

	return &Store{cfg: JSONLOpts{Dir: dir}}
}

func TestGenerateID(t *testing.T) {
	id, err := generateID()
	require.NoError(t, err)
	assert.Len(t, id, 32)

	id2, err := generateID()
	require.NoError(t, err)
	assert.NotEqual(t, id, id2)
}

func TestCreate(t *testing.T) {
	s := newTestStore(t)

	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	assert.Equal(t, "session", sess.Header.Type)
	assert.Len(t, sess.Header.ID, 32)
	assert.Equal(t, "/tmp/project", sess.Header.CWD)
	assert.False(t, sess.Header.Timestamp.IsZero())

	path := filepath.Join(s.cfg.Dir, sess.Header.ID+".jsonl")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var header SessionHeader
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &header))
	assert.Equal(t, sess.Header.ID, header.ID)
	assert.Equal(t, "/tmp/project", header.CWD)
}

func TestAppend(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entry := Entry{
		Type: "message",
		Turn: 1,
		Data: json.RawMessage(`{"role":"user","content":"hello"}`),
	}
	_, err = s.Append(sess.Header.ID, entry)
	require.NoError(t, err)

	entry2 := Entry{
		Type: "message",
		Turn: 2,
		Data: json.RawMessage(`{"role":"assistant","content":"hi"}`),
	}
	_, err = s.Append(sess.Header.ID, entry2)
	require.NoError(t, err)

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 2)

	assert.Len(t, loaded.Entries[0].ID, 32)
	assert.False(t, loaded.Entries[0].Created.IsZero())
	assert.Equal(t, 1, loaded.Entries[0].Turn)
	assert.JSONEq(t, `{"role":"user","content":"hello"}`, string(loaded.Entries[0].Data))

	assert.Len(t, loaded.Entries[1].ID, 32)
	assert.Equal(t, 2, loaded.Entries[1].Turn)
}

func TestAppend_PreservesIDAndTimestamp(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	entry := Entry{
		ID:      "custom-id",
		Type:    "message",
		Turn:    1,
		Data:    json.RawMessage(`{}`),
		Created: ts,
	}
	_, err = s.Append(sess.Header.ID, entry)
	require.NoError(t, err)

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	assert.Equal(t, "custom-id", loaded.Entries[0].ID)
	assert.Equal(t, ts, loaded.Entries[0].Created)
}

func TestLoad_Roundtrip(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entries := []Entry{
		{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"hello"}`)},
		{Type: "message", Turn: 2, Data: json.RawMessage(`{"role":"assistant","content":"world"}`)},
	}
	for _, e := range entries {
		_, err = s.Append(sess.Header.ID, e)
		require.NoError(t, err)
	}

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)

	assert.Equal(t, sess.Header.ID, loaded.Header.ID)
	assert.Equal(t, "/tmp/project", loaded.Header.CWD)
	assert.Equal(t, sess.Header.Timestamp, loaded.Header.Timestamp)
	require.Len(t, loaded.Entries, 2)
}

func TestLoad_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Load("nonexistent")
	require.Error(t, err)
}

func TestHistory(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	for i := range 3 {
		_, err = s.Append(sess.Header.ID, Entry{
			Type: "message",
			Turn: i + 1,
			Data: json.RawMessage(`{}`),
		})
		require.NoError(t, err)
	}

	history, err := s.History(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, history, 3)

	for i, entry := range history {
		assert.Equal(t, i+1, entry.Turn)
	}
}

func TestList(t *testing.T) {
	s := newTestStore(t)

	sess1, err := s.Create("/tmp/proj1")
	require.NoError(t, err)

	sess2, err := s.Create("/tmp/proj2")
	require.NoError(t, err)

	for i := range 3 {
		_, err = s.Append(sess1.Header.ID, Entry{Type: "message", Turn: i + 1, Data: json.RawMessage(`{}`)})
		require.NoError(t, err)
	}

	_, err = s.Append(sess2.Header.ID, Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{}`)})
	require.NoError(t, err)

	infos, err := s.List()
	require.NoError(t, err)
	require.Len(t, infos, 2)

	byID := map[string]SessionInfo{}
	for _, info := range infos {
		byID[info.ID] = info
	}

	info1 := byID[sess1.Header.ID]
	assert.Equal(t, "/tmp/proj1", info1.CWD)
	assert.Equal(t, 3, info1.EntryCount)
	assert.False(t, info1.CreatedAt.IsZero())
	assert.False(t, info1.UpdatedAt.IsZero())

	info2 := byID[sess2.Header.ID]
	assert.Equal(t, "/tmp/proj2", info2.CWD)
	assert.Equal(t, 1, info2.EntryCount)
}

func TestList_EmptyDir(t *testing.T) {
	s := newTestStore(t)
	infos, err := s.List()
	require.NoError(t, err)
	assert.Nil(t, infos)
}

func TestCompact_Truncation(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	for i := range 5 {
		_, err = s.Append(sess.Header.ID, Entry{
			Type: "message",
			Turn: i + 1,
			Data: json.RawMessage(`{"role":"user","content":"msg"}`),
		})
		require.NoError(t, err)
	}

	require.NoError(t, s.Compact(sess.Header.ID, 2))

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 3)

	assert.Equal(t, "summary", loaded.Entries[0].Type)
	assert.Equal(t, 4, loaded.Entries[1].Turn)
	assert.Equal(t, 5, loaded.Entries[2].Turn)

	assert.Equal(t, loaded.Entries[0].ID, loaded.Entries[1].ParentID,
		"first kept entry should reference summary as parent")
}

func TestCompact_SummaryEntry(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	for i := range 4 {
		_, err = s.Append(sess.Header.ID, Entry{
			Type: "message",
			Turn: i + 1,
			Data: json.RawMessage(`{}`),
		})
		require.NoError(t, err)
	}

	require.NoError(t, s.Compact(sess.Header.ID, 1))

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 2)

	summary := loaded.Entries[0]
	assert.Equal(t, "summary", summary.Type)
	assert.NotEmpty(t, summary.ID)
	assert.False(t, summary.Created.IsZero())
	assert.Contains(t, string(summary.Data), `"removed_count":3`)
	assert.Contains(t, string(summary.Data), `"first_turn":1`)
	assert.Contains(t, string(summary.Data), `"last_turn":3`)
}

func TestCompact_KeepLastExceedsTotal(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	for i := range 3 {
		_, err = s.Append(sess.Header.ID, Entry{
			Type: "message",
			Turn: i + 1,
			Data: json.RawMessage(`{}`),
		})
		require.NoError(t, err)
	}

	require.NoError(t, s.Compact(sess.Header.ID, 10))

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	assert.Len(t, loaded.Entries, 3, "should not modify file when keepLast >= total")
}

func TestCompact_PreservesHeader(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/my-project")
	require.NoError(t, err)

	for i := range 3 {
		_, err = s.Append(sess.Header.ID, Entry{Type: "message", Turn: i + 1, Data: json.RawMessage(`{}`)})
		require.NoError(t, err)
	}

	require.NoError(t, s.Compact(sess.Header.ID, 1))

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	assert.Equal(t, sess.Header.ID, loaded.Header.ID)
	assert.Equal(t, "/tmp/my-project", loaded.Header.CWD)
}

func TestList_IgnoresNonJSONL(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, os.WriteFile(filepath.Join(s.cfg.Dir, "other.txt"), []byte("data"), 0o644))

	infos, err := s.List()
	require.NoError(t, err)
	assert.Nil(t, infos)
}

func TestSessionPath_InvalidID(t *testing.T) {
	s := newTestStore(t)
	_, err := s.sessionPath("../../etc/passwd")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid session ID")
}

func TestLoad_EmptyFile(t *testing.T) {
	s := newTestStore(t)
	path := filepath.Join(s.cfg.Dir, "empty.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(""), 0o644))
	_, err := loadFromFile(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty session file")
}

func TestList_SkipsCorruptedFile(t *testing.T) {
	s := newTestStore(t)

	sess, err := s.Create("/tmp/valid")
	require.NoError(t, err)
	_, err = s.Append(sess.Header.ID, Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{}`)})
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(s.cfg.Dir, "corrupt.jsonl"), []byte("not json\n"), 0o644))

	infos, err := s.List()
	require.NoError(t, err)
	require.Len(t, infos, 1)
	assert.Equal(t, sess.Header.ID, infos[0].ID)
}

func TestSubscribe_CreatesSessionOnPrompt(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	b.Publish(sdk.NewEvent("agent.prompt", "hello world"))
	time.Sleep(50 * time.Millisecond)

	s.mu.Lock()
	sessionID := s.sessionID
	s.mu.Unlock()

	require.NotEmpty(t, sessionID)

	sess, err := s.Load(sessionID)
	require.NoError(t, err)
	require.Len(t, sess.Entries, 1)

	assert.Equal(t, "message", sess.Entries[0].Type)
	assert.Equal(t, 1, sess.Entries[0].Turn)

	var data map[string]any
	require.NoError(t, json.Unmarshal(sess.Entries[0].Data, &data))
	assert.Equal(t, "user", data["role"])
	assert.Equal(t, "hello world", data["content"])
}

func TestSubscribe_AppendsAssistantOnMsgEnd(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	b.Publish(sdk.NewEvent("agent.prompt", "hello"))
	time.Sleep(50 * time.Millisecond)

	b.Publish(sdk.NewEvent("agent.turn_start", 1))
	b.Publish(sdk.NewEvent("agent.message_end", "hi there"))
	time.Sleep(50 * time.Millisecond)

	s.mu.Lock()
	sessionID := s.sessionID
	s.mu.Unlock()

	sess, err := s.Load(sessionID)
	require.NoError(t, err)
	require.Len(t, sess.Entries, 2)

	assert.Equal(t, "message", sess.Entries[1].Type)

	var data map[string]any
	require.NoError(t, json.Unmarshal(sess.Entries[1].Data, &data))
	assert.Equal(t, "assistant", data["role"])
	assert.Equal(t, "hi there", data["content"])

	assert.Equal(t, sess.Entries[0].ID, sess.Entries[1].ParentID)
}

func TestSubscribe_AppendsToolResult(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	b.Publish(sdk.NewEvent("agent.prompt", "list files"))
	time.Sleep(50 * time.Millisecond)

	b.Publish(sdk.NewEvent("agent.turn_start", 1))
	b.Publish(sdk.NewEvent("agent.message_end", "let me check"))
	time.Sleep(50 * time.Millisecond)

	b.Publish(sdk.NewEvent("agent.tool_result", map[string]any{
		"id":     "call-123",
		"tool":   "bash",
		"result": "file1.txt\nfile2.txt",
	}))
	time.Sleep(50 * time.Millisecond)

	s.mu.Lock()
	sessionID := s.sessionID
	s.mu.Unlock()

	sess, err := s.Load(sessionID)
	require.NoError(t, err)
	require.Len(t, sess.Entries, 3)

	assert.Equal(t, "message", sess.Entries[2].Type)

	var data map[string]any
	require.NoError(t, json.Unmarshal(sess.Entries[2].Data, &data))
	assert.Equal(t, "tool_result", data["role"])

	assert.Equal(t, sess.Entries[1].ID, sess.Entries[2].ParentID)
}

func TestSubscribe_EndStopsGoroutine(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))

	b.Publish(sdk.NewEvent("agent.prompt", "hello"))
	time.Sleep(50 * time.Millisecond)

	b.Publish(sdk.NewEvent("agent.end", nil))

	require.NoError(t, s.Close())
}

func TestSubscribe_CloseCancelsGoroutine(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))

	b.Publish(sdk.NewEvent("agent.prompt", "test"))
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, s.Close())

	s.mu.Lock()
	sessionID := s.sessionID
	s.mu.Unlock()
	require.NotEmpty(t, sessionID)
}

func TestNewStore_LoadsNestedDir(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, ".weave", "settings.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(configPath), 0o755))

	sessionDir := filepath.Join(dir, "sessions")
	configContent := `{"exclude_extensions":["jsonl"],"jsonl":{"dir":"` + sessionDir + `"}}`
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0o644))

	s, err := NewStore(JSONLOpts{Dir: sessionDir})
	require.NoError(t, err)

	got, err := s.sessionDir()
	require.NoError(t, err)
	assert.Equal(t, sessionDir, got)
}

func TestNewStore_DefaultDir(t *testing.T) {
	s, err := NewStore(JSONLOpts{})
	require.NoError(t, err)

	got, err := s.sessionDir()
	require.NoError(t, err)

	home, err := os.UserHomeDir()
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(home, ".weave", "sessions"), got)
}

func TestListSessions_Conversion(t *testing.T) {
	s := newTestStore(t)

	sess1, err := s.Create("/tmp/proj1")
	require.NoError(t, err)

	sess2, err := s.Create("/tmp/proj2")
	require.NoError(t, err)

	_, err = s.Append(sess1.Header.ID, Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{}`)})
	require.NoError(t, err)

	infos, err := s.ListSessions()
	require.NoError(t, err)
	require.Len(t, infos, 2)

	byID := map[string]sdk.SessionInfo{}
	for _, info := range infos {
		byID[info.ID] = info
	}

	info1 := byID[sess1.Header.ID]
	assert.Equal(t, "/tmp/proj1", info1.CWD)
	assert.False(t, info1.CreatedAt.IsZero())
	assert.False(t, info1.UpdatedAt.IsZero())

	info2 := byID[sess2.Header.ID]
	assert.Equal(t, "/tmp/proj2", info2.CWD)
}

func TestListSessions_EmptyDir(t *testing.T) {
	s := newTestStore(t)
	infos, err := s.ListSessions()
	require.NoError(t, err)
	assert.Empty(t, infos)
}

func TestListSessions_SkipsCorruptedHeader(t *testing.T) {
	s := newTestStore(t)

	sess, err := s.Create("/tmp/valid")
	require.NoError(t, err)
	_, err = s.Append(sess.Header.ID, Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{}`)})
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(s.cfg.Dir, "corrupt.jsonl"), []byte("not json\n"), 0o644))

	infos, err := s.ListSessions()
	require.NoError(t, err)
	require.Len(t, infos, 1)
	assert.Equal(t, sess.Header.ID, infos[0].ID)
}

func TestLoadHistory_FullHistory(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entries := []Entry{
		{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"hello"}`), Created: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Type: "message", Turn: 2, Data: json.RawMessage(`{"role":"assistant","content":"hi there"}`), Created: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)},
	}
	for _, e := range entries {
		_, err = s.Append(sess.Header.ID, e)
		require.NoError(t, err)
	}

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, messages, 2)

	assert.Equal(t, sdk.RoleUser, messages[0].Role)
	assert.Equal(t, "hello", messages[0].Content)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), messages[0].Timestamp)

	assert.Equal(t, sdk.RoleAssistant, messages[1].Role)
	assert.Equal(t, "hi there", messages[1].Content)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC), messages[1].Timestamp)
}

func TestLoadHistory_WithToolCalls(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	data := json.RawMessage(`{"role":"assistant","content":"","tool_calls":[{"id":"call-1","name":"bash","arguments":{"command":"ls"}}]}`)
	_, err = s.Append(sess.Header.ID, Entry{Type: "message", Turn: 1, Data: data})
	require.NoError(t, err)

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, messages, 1)

	assert.Equal(t, sdk.RoleAssistant, messages[0].Role)
	require.Len(t, messages[0].ToolCalls, 1)
	assert.Equal(t, "call-1", messages[0].ToolCalls[0].ID)
	assert.Equal(t, "bash", messages[0].ToolCalls[0].Name)
	assert.Equal(t, map[string]any{"command": "ls"}, messages[0].ToolCalls[0].Arguments)
}

func TestLoadHistory_WithToolResult(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	data := json.RawMessage(`{"role":"tool_result","tool":{"id":"call-1","tool":"bash","result":{"content":"file1.txt\nfile2.txt","is_error":false}}}`)
	_, err = s.Append(sess.Header.ID, Entry{Type: "message", Turn: 1, Data: data})
	require.NoError(t, err)

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, messages, 1)

	assert.Equal(t, sdk.RoleToolResult, messages[0].Role)
	assert.Equal(t, "call-1", messages[0].ToolCallID)
	assert.Equal(t, "bash", messages[0].ToolName)
	assert.Equal(t, "file1.txt\nfile2.txt", messages[0].Content)
	assert.False(t, messages[0].IsError)
}

func TestLoadHistory_WithToolResultError(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	data := json.RawMessage(`{"role":"tool_result","tool":{"id":"call-1","tool":"bash","result":{"content":"command not found","is_error":true}}}`)
	_, err = s.Append(sess.Header.ID, Entry{Type: "message", Turn: 1, Data: data})
	require.NoError(t, err)

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, messages, 1)

	assert.Equal(t, sdk.RoleToolResult, messages[0].Role)
	assert.Equal(t, "command not found", messages[0].Content)
	assert.True(t, messages[0].IsError)
}

func TestLoadHistory_PartialCorruption(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	_, err = s.Append(sess.Header.ID, Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"hello"}`)})
	require.NoError(t, err)

	// Write a corrupt entry directly to the file (Append would reject invalid JSON)
	path := filepath.Join(s.cfg.Dir, sess.Header.ID+".jsonl")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString(`{"type":"message","turn":2,"data":{invalid json}}
`)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = s.Append(sess.Header.ID, Entry{Type: "message", Turn: 3, Data: json.RawMessage(`{"role":"assistant","content":"hi"}`)})
	require.NoError(t, err)

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, messages, 2)

	assert.Equal(t, sdk.RoleUser, messages[0].Role)
	assert.Equal(t, "hello", messages[0].Content)
	assert.Equal(t, sdk.RoleAssistant, messages[1].Role)
	assert.Equal(t, "hi", messages[1].Content)
}

func TestLoadHistory_EmptySession(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	assert.Empty(t, messages)
}

func TestLoadHistory_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.LoadHistory("nonexistent")
	require.Error(t, err)
}

func TestLoadHistory_InvalidSessionID(t *testing.T) {
	s := newTestStore(t)
	_, err := s.LoadHistory("../../etc/passwd")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid session ID")
}

func TestListSessions_ReadDirError(t *testing.T) {
	s := newTestStore(t)

	// Create a file with the same name as the session dir to cause ReadDir to fail
	err := os.WriteFile(s.cfg.Dir+".file", []byte("x"), 0o644)
	require.NoError(t, err)

	// Swap cfg.Dir to point to the file path (not a directory) to trigger a ReadDir error
	s.cfg.Dir += ".file"
	_, err = s.ListSessions()
	require.Error(t, err)
}

func TestEntryToMessage_InvalidJSON(t *testing.T) {
	entry := Entry{Data: json.RawMessage(`{invalid}`)}
	_, err := entryToMessage(entry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestEntryToMessage_UnknownRole(t *testing.T) {
	entry := Entry{Data: json.RawMessage(`{"role":"system","content":"hello"}`)}
	msg, err := entryToMessage(entry)
	require.NoError(t, err)
	assert.Equal(t, "system", msg.Role)
	assert.Equal(t, "hello", msg.Content)
}

func TestSubscribe_ResumesSession(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	// Create a session with some entries
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entry1 := Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"hello"}`)}
	_, err = s.Append(sess.Header.ID, entry1)
	require.NoError(t, err)

	entry2 := Entry{Type: "message", Turn: 2, Data: json.RawMessage(`{"role":"assistant","content":"hi there"}`)}
	id2, err := s.Append(sess.Header.ID, entry2)
	require.NoError(t, err)

	// Publish session.resume
	b.Publish(sdk.NewEvent("session.resume", sdk.SessionResumePayload{
		SessionID: sess.Header.ID,
		Messages:  []sdk.Message{},
	}))
	time.Sleep(50 * time.Millisecond)

	// Verify internal state — turn is NOT advanced to maxTurn; it stays at 0
	// so the first prompt after resume appends at turn 1, matching the agent loop.
	s.mu.Lock()
	assert.Equal(t, sess.Header.ID, s.sessionID)
	assert.Equal(t, id2, s.lastEntry)
	assert.Equal(t, 0, s.turn)
	s.mu.Unlock()

	// Now publish agent.prompt - should append to resumed session, not create new
	b.Publish(sdk.NewEvent("agent.prompt", "follow up"))
	time.Sleep(50 * time.Millisecond)

	// Load and verify
	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 3)

	assert.Equal(t, 1, loaded.Entries[2].Turn)

	var data map[string]any
	require.NoError(t, json.Unmarshal(loaded.Entries[2].Data, &data))
	assert.Equal(t, "user", data["role"])
	assert.Equal(t, "follow up", data["content"])
	assert.Equal(t, id2, loaded.Entries[2].ParentID)

	// Verify no new session was created
	infos, err := s.List()
	require.NoError(t, err)
	require.Len(t, infos, 1)
}

func TestSubscribe_ResumesSession_Followup(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entry1 := Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"hello"}`)}
	id1, err := s.Append(sess.Header.ID, entry1)
	require.NoError(t, err)

	b.Publish(sdk.NewEvent("session.resume", sdk.SessionResumePayload{
		SessionID: sess.Header.ID,
		Messages:  []sdk.Message{},
	}))
	time.Sleep(50 * time.Millisecond)

	b.Publish(sdk.NewEvent("agent.followup", "follow up"))
	time.Sleep(50 * time.Millisecond)

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 2)

	assert.Equal(t, 1, loaded.Entries[1].Turn)

	var data map[string]any
	require.NoError(t, json.Unmarshal(loaded.Entries[1].Data, &data))
	assert.Equal(t, "user", data["role"])
	assert.Equal(t, "follow up", data["content"])
	assert.Equal(t, id1, loaded.Entries[1].ParentID)
}

func TestSubscribe_ResumesSession_FollowupThenNewPrompt(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entry1 := Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"hello"}`)}
	_, err = s.Append(sess.Header.ID, entry1)
	require.NoError(t, err)

	b.Publish(sdk.NewEvent("session.resume", sdk.SessionResumePayload{
		SessionID: sess.Header.ID,
		Messages:  []sdk.Message{},
	}))
	time.Sleep(50 * time.Millisecond)

	// First input after resume is a followup — should append to resumed session
	b.Publish(sdk.NewEvent("agent.followup", "follow up"))
	time.Sleep(50 * time.Millisecond)

	// Verify resumed flag is cleared after the first followup
	s.mu.Lock()
	assert.False(t, s.resumed, "resumed should be cleared after first followup")
	s.mu.Unlock()

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 2)

	// Now a prompt (e.g. /new) should create a new session, not append to the old one
	b.Publish(sdk.NewEvent("agent.prompt", "new conversation"))
	time.Sleep(50 * time.Millisecond)

	infos, err := s.List()
	require.NoError(t, err)
	require.Len(t, infos, 2, "expected a new session to be created")

	// Find the new session (the one with 1 entry)
	var newSessID string

	for _, info := range infos {
		if info.EntryCount == 1 {
			newSessID = info.ID
			break
		}
	}

	require.NotEmpty(t, newSessID, "expected one session with a single entry")

	newSess, err := s.Load(newSessID)
	require.NoError(t, err)
	require.Len(t, newSess.Entries, 1)

	var data map[string]any
	require.NoError(t, json.Unmarshal(newSess.Entries[0].Data, &data))
	assert.Equal(t, "user", data["role"])
	assert.Equal(t, "new conversation", data["content"])
}

func TestSubscribe_ResumesSession_MsgEndAndToolResult(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entry1 := Entry{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"list files"}`)}
	_, err = s.Append(sess.Header.ID, entry1)
	require.NoError(t, err)

	b.Publish(sdk.NewEvent("session.resume", sdk.SessionResumePayload{
		SessionID: sess.Header.ID,
		Messages:  []sdk.Message{},
	}))
	time.Sleep(50 * time.Millisecond)

	b.Publish(sdk.NewEvent("agent.prompt", "show me"))
	time.Sleep(50 * time.Millisecond)

	b.Publish(sdk.NewEvent("agent.turn_start", 1))
	b.Publish(sdk.NewEvent("agent.message_end", "here are the files"))
	time.Sleep(50 * time.Millisecond)

	b.Publish(sdk.NewEvent("agent.tool_result", map[string]any{
		"id":     "call-123",
		"tool":   "bash",
		"result": "file1.txt\nfile2.txt",
	}))
	time.Sleep(50 * time.Millisecond)

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 4)

	// Entry 1: original user message
	// Entry 2: resumed prompt "show me"
	// Entry 3: assistant message "here are the files"
	// Entry 4: tool result

	assert.Equal(t, 1, loaded.Entries[1].Turn)
	assert.Equal(t, 1, loaded.Entries[2].Turn)
	assert.Equal(t, 1, loaded.Entries[3].Turn)

	var data3 map[string]any
	require.NoError(t, json.Unmarshal(loaded.Entries[2].Data, &data3))
	assert.Equal(t, "assistant", data3["role"])
	assert.Equal(t, "here are the files", data3["content"])

	var data4 map[string]any
	require.NoError(t, json.Unmarshal(loaded.Entries[3].Data, &data4))
	assert.Equal(t, "tool_result", data4["role"])
}

func TestSubscribe_ResumeInvalidPayload(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	b.Publish(sdk.NewEvent("session.resume", "not a payload"))
	time.Sleep(50 * time.Millisecond)

	s.mu.Lock()
	assert.Empty(t, s.sessionID)
	s.mu.Unlock()
}

func TestSubscribe_ResumeEmptySession(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	b.Publish(sdk.NewEvent("session.resume", sdk.SessionResumePayload{
		SessionID: sess.Header.ID,
		Messages:  []sdk.Message{},
	}))
	time.Sleep(50 * time.Millisecond)

	s.mu.Lock()
	assert.Equal(t, sess.Header.ID, s.sessionID)
	assert.Empty(t, s.lastEntry)
	assert.Equal(t, 0, s.turn)
	s.mu.Unlock()

	// Prompt after resuming empty session should append with turn 1
	b.Publish(sdk.NewEvent("agent.prompt", "hello"))
	time.Sleep(50 * time.Millisecond)

	loaded, err := s.Load(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 1)
	assert.Equal(t, 1, loaded.Entries[0].Turn)
}

func TestSubscribe_PromptCreatesNewSessionWhenNotResumed(t *testing.T) {
	s := newTestStore(t)
	b := eventbus.New()

	require.NoError(t, s.Subscribe(b))
	defer s.Close()

	b.Publish(sdk.NewEvent("agent.prompt", "hello world"))
	time.Sleep(50 * time.Millisecond)

	s.mu.Lock()
	sessionID := s.sessionID
	s.mu.Unlock()

	require.NotEmpty(t, sessionID)

	sess, err := s.Load(sessionID)
	require.NoError(t, err)
	require.Len(t, sess.Entries, 1)

	var data map[string]any
	require.NoError(t, json.Unmarshal(sess.Entries[0].Data, &data))
	assert.Equal(t, "user", data["role"])
	assert.Equal(t, "hello world", data["content"])
}

func TestLoadHistory_WithCompactionSummary(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entries := []Entry{
		{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"first message"}`), Created: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Type: "message", Turn: 2, Data: json.RawMessage(`{"role":"assistant","content":"first response"}`), Created: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)},
		{Type: "message", Turn: 3, Data: json.RawMessage(`{"role":"assistant","content":"[Compaction Summary]\nsummary of earlier conversation"}`), Created: time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)},
		{Type: "message", Turn: 4, Data: json.RawMessage(`{"role":"user","content":"after compaction"}`), Created: time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC)},
		{Type: "message", Turn: 5, Data: json.RawMessage(`{"role":"assistant","content":"response after compaction"}`), Created: time.Date(2026, 1, 1, 0, 0, 4, 0, time.UTC)},
	}
	for _, e := range entries {
		_, err = s.Append(sess.Header.ID, e)
		require.NoError(t, err)
	}

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, messages, 3, "expected only compaction summary and messages after it")

	assert.Equal(t, sdk.RoleAssistant, messages[0].Role)
	assert.Equal(t, "[Compaction Summary]\nsummary of earlier conversation", messages[0].Content)

	assert.Equal(t, sdk.RoleUser, messages[1].Role)
	assert.Equal(t, "after compaction", messages[1].Content)

	assert.Equal(t, sdk.RoleAssistant, messages[2].Role)
	assert.Equal(t, "response after compaction", messages[2].Content)
}

func TestLoadHistory_MultipleCompactions(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entries := []Entry{
		{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"msg 1"}`)},
		{Type: "message", Turn: 2, Data: json.RawMessage(`{"role":"assistant","content":"[Compaction Summary]\nfirst summary"}`)},
		{Type: "message", Turn: 3, Data: json.RawMessage(`{"role":"user","content":"msg 3"}`)},
		{Type: "message", Turn: 4, Data: json.RawMessage(`{"role":"assistant","content":"[Compaction Summary]\nsecond summary"}`)},
		{Type: "message", Turn: 5, Data: json.RawMessage(`{"role":"user","content":"msg 5"}`)},
		{Type: "message", Turn: 6, Data: json.RawMessage(`{"role":"assistant","content":"msg 6"}`)},
	}
	for _, e := range entries {
		_, err = s.Append(sess.Header.ID, e)
		require.NoError(t, err)
	}

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, messages, 3, "expected only last compaction summary and messages after it")

	assert.Equal(t, "[Compaction Summary]\nsecond summary", messages[0].Content)
	assert.Equal(t, sdk.RoleUser, messages[1].Role)
	assert.Equal(t, "msg 5", messages[1].Content)
	assert.Equal(t, sdk.RoleAssistant, messages[2].Role)
	assert.Equal(t, "msg 6", messages[2].Content)
}

func TestLoadHistory_NoCompactionSummary(t *testing.T) {
	s := newTestStore(t)
	sess, err := s.Create("/tmp/project")
	require.NoError(t, err)

	entries := []Entry{
		{Type: "message", Turn: 1, Data: json.RawMessage(`{"role":"user","content":"hello"}`)},
		{Type: "message", Turn: 2, Data: json.RawMessage(`{"role":"assistant","content":"hi there"}`)},
	}
	for _, e := range entries {
		_, err = s.Append(sess.Header.ID, e)
		require.NoError(t, err)
	}

	messages, err := s.LoadHistory(sess.Header.ID)
	require.NoError(t, err)
	require.Len(t, messages, 2, "expected all messages when no compaction summary exists")

	assert.Equal(t, sdk.RoleUser, messages[0].Role)
	assert.Equal(t, "hello", messages[0].Content)
	assert.Equal(t, sdk.RoleAssistant, messages[1].Role)
	assert.Equal(t, "hi there", messages[1].Content)
}
