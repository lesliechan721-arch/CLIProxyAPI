package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func sessionTestKey(id string) string { return "openai::" + id + "::gpt-test" }

func readSessionTestSnapshot(t *testing.T, path string) sessionSnapshot {
	t.Helper()
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var snapshot sessionSnapshot
	if errDecode := json.Unmarshal(data, &snapshot); errDecode != nil {
		t.Fatal(errDecode)
	}
	return snapshot
}

func TestSessionAffinityPersistenceRestartPreservesAliasesAndExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".runtime", "session-affinity.cache")
	selector := NewSessionAffinitySelector(&FillFirstSelector{})
	manager := NewManager(nil, selector, nil)
	manager.ConfigureSessionAffinityPersistence(path, true)
	payload := []byte(`{"prompt_cache_key":"bucket","conversation":{"id":"conversation"}}`)
	auths := []*Auth{{ID: "auth-a"}, {ID: "auth-b"}}
	picked, errPick := selector.Pick(context.Background(), "openai", "gpt-test", cliproxyexecutor.Options{OriginalRequest: payload}, auths)
	if errPick != nil {
		t.Fatal(errPick)
	}
	primary, alias := sessionTestKey("pck:bucket"), sessionTestKey("conv:conversation")
	expires := time.Now().Add(25 * time.Minute).UTC()
	selector.cache.mu.Lock()
	entry := selector.cache.entries[primary]
	selector.cache.replaceAliasGroupsLocked(picked.ID, expires, entry.aliases, entry)
	selector.cache.replaceAliasGroupsLocked("expired-auth", time.Now().Add(-time.Minute), []string{sessionTestKey("expired")})
	selector.cache.mu.Unlock()
	manager.StopAutoRefresh()

	for _, tt := range []struct {
		name     string
		disabled bool
		removed  bool
	}{
		{name: "available"}, {name: "disabled", disabled: true}, {name: "removed", removed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			restored := NewSessionAffinitySelector(&FillFirstSelector{})
			restoredManager := NewManager(nil, restored, nil)
			restoredManager.ConfigureSessionAffinityPersistence(path, true)
			defer restoredManager.StopAutoRefresh()
			for _, key := range []string{primary, alias} {
				if got, ok := restored.cache.Get(key); !ok || got != picked.ID {
					t.Fatalf("restored alias = %q, %v, want %q", got, ok, picked.ID)
				}
				restored.cache.mu.RLock()
				actual := restored.cache.entries[key].expiresAt
				restored.cache.mu.RUnlock()
				if !actual.Equal(expires) {
					t.Fatalf("restoration extended expiration: %s, want %s", actual, expires)
				}
			}
			if _, ok := restored.cache.Get(sessionTestKey("expired")); ok {
				t.Fatal("expired binding restored")
			}
			candidates := []*Auth{{ID: "auth-a", Disabled: tt.disabled}, {ID: "auth-b"}}
			if tt.removed {
				candidates = candidates[1:]
			}
			got, errPick := restored.Pick(context.Background(), "openai", "gpt-test", cliproxyexecutor.Options{OriginalRequest: []byte(`{"conversation":{"id":"conversation"}}`)}, candidates)
			want := picked.ID
			if tt.disabled || tt.removed {
				want = "auth-b"
			}
			if errPick != nil || got == nil || got.ID != want {
				t.Fatalf("restored Pick = %+v, %v, want %q", got, errPick, want)
			}
		})
		// Keep each availability case independent of the previous case's refresh or failover.
		if errWrite := writeSessionSnapshot(path, sessionSnapshot{Version: sessionSnapshotVersion, Groups: []sessionSnapshotGroup{{
			AuthID: picked.ID, Aliases: []string{primary, alias}, ExpiresAt: expires,
		}}}); errWrite != nil {
			t.Fatal(errWrite)
		}
	}
}

func TestSessionCachePersistenceRefreshDeleteAndEviction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-affinity.cache")
	cache := NewSessionCacheWithCapacity(time.Hour, 4)
	cache.configurePersistence(path, false)
	cache.SetAliases("a", sessionTestKey("old"), sessionTestKey("old-alias"))
	cache.SetAliases("b", sessionTestKey("keep"), sessionTestKey("delete"))
	cache.mu.Lock()
	entry := cache.entries[sessionTestKey("keep")]
	cache.replaceAliasGroupsLocked(entry.authID, time.Now().Add(10*time.Minute), entry.aliases, entry)
	cache.mu.Unlock()
	cache.flushPersistence()
	initial := readSessionTestSnapshot(t, path).Groups[1].ExpiresAt
	if _, ok := cache.GetAndRefresh(sessionTestKey("keep")); !ok {
		t.Fatal("refresh failed")
	}
	if !cache.CompareAndDelete(sessionTestKey("delete"), "b") {
		t.Fatal("alias deletion failed")
	}
	cache.Set(sessionTestKey("new-1"), "c")
	cache.Set(sessionTestKey("new-2"), "d")
	cache.Stop()
	stored := readSessionTestSnapshot(t, path)
	if len(stored.Groups) != 3 || stored.Groups[0].AuthID != "b" || stored.Groups[1].AuthID != "c" || stored.Groups[2].AuthID != "d" {
		t.Fatalf("whole-group eviction or ordering changed: %+v", stored.Groups)
	}
	if !stored.Groups[0].ExpiresAt.After(initial) {
		t.Fatal("TTL refresh was not persisted")
	}
	restored := NewSessionCacheWithCapacity(time.Hour, 4)
	defer restored.Stop()
	restored.configurePersistence(path, true)
	for _, id := range []string{"old", "old-alias", "delete"} {
		if _, ok := restored.Get(sessionTestKey(id)); ok {
			t.Fatalf("deleted or evicted alias %q restored", id)
		}
	}
	for _, id := range []string{"keep", "new-1", "new-2"} {
		if _, ok := restored.Get(sessionTestKey(id)); !ok {
			t.Fatalf("live alias %q missing", id)
		}
	}
	restored.InvalidateAuth("c")
	restored.Invalidate(sessionTestKey("new-2"))
	restored.flushPersistence()
	if got := readSessionTestSnapshot(t, path); len(got.Groups) != 1 || got.Groups[0].AuthID != "b" {
		t.Fatalf("auth and alias invalidation not saved: %+v", got.Groups)
	}
}

func TestSessionCachePersistenceRestorePrunesInvalidExpiredAndCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-affinity.cache")
	future := time.Now().Add(time.Hour).UTC()
	snapshot := sessionSnapshot{Version: sessionSnapshotVersion, Groups: []sessionSnapshotGroup{
		{AuthID: "old", Aliases: []string{sessionTestKey("old"), sessionTestKey("old-alias")}, ExpiresAt: future},
		{AuthID: "expired", Aliases: []string{sessionTestKey("expired")}, ExpiresAt: time.Now().Add(-time.Hour)},
		{AuthID: "invalid", Aliases: []string{"missing-namespace"}, ExpiresAt: future},
		{AuthID: "new", Aliases: []string{sessionTestKey("new"), sessionTestKey("new-alias")}, ExpiresAt: future},
	}}
	if errWrite := writeSessionSnapshot(path, snapshot); errWrite != nil {
		t.Fatal(errWrite)
	}
	cache := NewSessionCacheWithCapacity(time.Hour, 2)
	cache.configurePersistence(path, true)
	if cache.Len() != 2 {
		t.Fatalf("restored capacity = %d, want 2", cache.Len())
	}
	if _, ok := cache.Get(sessionTestKey("old")); ok {
		t.Fatal("oldest alias group survived capacity pruning")
	}
	cache.Stop()
	if got := readSessionTestSnapshot(t, path); len(got.Groups) != 1 || got.Groups[0].AuthID != "new" {
		t.Fatalf("pruned records remain in file: %+v", got.Groups)
	}
	cache = NewSessionCacheWithCapacity(time.Hour, 1)
	cache.configurePersistence(path, true)
	cache.Stop()
	if got := readSessionTestSnapshot(t, path); len(got.Groups) != 0 {
		t.Fatal("oversized whole group was not pruned to an empty snapshot")
	}
}

func TestSessionCachePersistenceReadFailuresKeepMemoryRouting(t *testing.T) {
	for _, test := range []struct{ name, contents string }{
		{"missing", ""}, {"corrupt", `{secret invalid`}, {"unsupported", `{"version":99,"groups":[]}`}, {"unreadable", "directory"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "session-affinity.cache")
			if test.contents == "directory" {
				if errMkdir := os.Mkdir(path, 0o700); errMkdir != nil {
					t.Fatal(errMkdir)
				}
			} else if test.contents != "" {
				if errWrite := os.WriteFile(path, []byte(test.contents), 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
			}
			cache := NewSessionCache(time.Hour)
			defer cache.Stop()
			cache.Set(sessionTestKey("live"), "a")
			cache.configurePersistence(path, true)
			if got, ok := cache.Get(sessionTestKey("live")); !ok || got != "a" {
				t.Fatalf("snapshot error broke live routing: %q, %v", got, ok)
			}
		})
	}
}

func TestSessionCachePersistencePeriodicFlushAndFinalWrite(t *testing.T) {
	cache := NewSessionCache(time.Hour)
	defer cache.Stop()
	path := filepath.Join(t.TempDir(), "session-affinity.cache")
	p := newSessionCachePersistence(cache, path)
	ticks := make(chan time.Time)
	writes := make(chan sessionSnapshot, 3)
	p.save = func(path string, snapshot sessionSnapshot) error {
		if errWrite := writeSessionSnapshot(path, snapshot); errWrite != nil {
			return errWrite
		}
		writes <- snapshot
		return nil
	}
	go p.run(ticks)
	cache.Set(sessionTestKey("one"), "a")
	ticks <- time.Time{}
	first := <-writes
	if len(first.Groups) != 1 {
		t.Fatal("periodic tick did not save binding")
	}
	// A completed following tick confirms the previous flush released its lock.
	ticks <- time.Time{}
	p.flushMu.Lock()
	p.flushMu.Unlock()
	select {
	case <-writes:
		t.Fatal("unchanged state caused another disk write")
	default:
	}
	cache.Set(sessionTestKey("two"), "b")
	p.stop()
	p.stop()
	if got := <-writes; len(got.Groups) != 2 {
		t.Fatal("stop omitted latest binding")
	}
}

func TestSessionCachePersistenceStopWaitsAndRoutingDoesNotWaitForIO(t *testing.T) {
	cache := NewSessionCache(time.Hour)
	defer cache.Stop()
	p := newSessionCachePersistence(cache, filepath.Join(t.TempDir(), "session-affinity.cache"))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	p.save = func(path string, snapshot sessionSnapshot) error {
		once.Do(func() { close(entered); <-release })
		return writeSessionSnapshot(path, snapshot)
	}
	ticks := make(chan time.Time)
	go p.run(ticks)
	cache.Set(sessionTestKey("one"), "a")
	ticks <- time.Time{}
	<-entered
	mutationDone := make(chan struct{})
	go func() { cache.Set(sessionTestKey("two"), "b"); close(mutationDone) }()
	select {
	case <-mutationDone:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot I/O blocked request cache mutation")
	}
	stopped := make(chan struct{})
	go func() { p.stop(); close(stopped) }()
	<-p.stopCh
	select {
	case <-stopped:
		t.Fatal("stop returned before pending save finished")
	default:
	}
	close(release)
	<-stopped
	if got := readSessionTestSnapshot(t, p.path); len(got.Groups) != 2 {
		t.Fatal("final snapshot omitted mutation made during previous I/O")
	}
}

func TestSessionCachePersistenceSaveFailureRetriesAndAtomicReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session-affinity.cache")
	cache := NewSessionCache(time.Hour)
	defer cache.Stop()
	cache.Set(sessionTestKey("one"), "a")
	p := newSessionCachePersistence(cache, path)
	attempts := 0
	p.save = func(path string, snapshot sessionSnapshot) error {
		attempts++
		if attempts == 1 {
			return errors.New("write failed")
		}
		return writeSessionSnapshot(path, snapshot)
	}
	p.flush()
	p.flush()
	if attempts != 2 || len(readSessionTestSnapshot(t, path).Groups) != 1 {
		t.Fatal("failed save was not retried")
	}
	cache.Set(sessionTestKey("two"), "b")
	p.flush()
	if len(readSessionTestSnapshot(t, path).Groups) != 2 {
		t.Fatal("atomic replacement did not replace previous snapshot")
	}
	if runtime.GOOS != "windows" {
		for _, check := range []struct {
			path string
			mode os.FileMode
		}{{dir, 0o700}, {path, 0o600}} {
			info, errStat := os.Stat(check.path)
			if errStat != nil || info.Mode().Perm() != check.mode {
				t.Fatalf("snapshot permissions: info=%v error=%v", info, errStat)
			}
		}
	}
	blocked := filepath.Join(dir, "blocked.cache")
	if errMkdir := os.Mkdir(blocked, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errSave := writeSessionSnapshot(blocked, sessionSnapshot{Version: 1}); errSave == nil {
		t.Fatal("replacement of directory unexpectedly succeeded")
	}
	files, errReadDir := os.ReadDir(dir)
	if errReadDir != nil {
		t.Fatal(errReadDir)
	}
	if len(files) != 2 {
		t.Fatalf("failed write left temporary files: %v", files)
	}
}

func TestManagerSessionAffinityPersistenceLiveHandoffAndEnableDisable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-affinity.cache")
	old := NewSessionAffinitySelector(nil)
	manager := NewManager(nil, old, nil)
	defer manager.StopAutoRefresh()
	old.cache.Set(sessionTestKey("live"), "newest")
	if errWrite := writeSessionSnapshot(path, sessionSnapshot{Version: 1, Groups: []sessionSnapshotGroup{{
		AuthID: "stale", Aliases: []string{sessionTestKey("live")}, ExpiresAt: time.Now().Add(time.Hour),
	}}}); errWrite != nil {
		t.Fatal(errWrite)
	}
	manager.ConfigureSessionAffinityPersistence(path, false)
	if got, _ := old.cache.Get(sessionTestKey("live")); got != "newest" {
		t.Fatal("live enable restored stale state")
	}
	old.cache.Set(sessionTestKey("before"), "a")
	next := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{TTL: 2 * time.Hour})
	manager.SetSelector(next)
	if next.cache != old.cache {
		t.Fatal("replacement discarded the live cache")
	}
	if len(readSessionTestSnapshot(t, path).Groups) != 2 {
		t.Fatal("replacement did not flush old writer")
	}
	// Release an old request only after replacement is published.
	late := make(chan struct{})
	done := make(chan struct{})
	go func() { <-late; old.cache.Set(sessionTestKey("late"), "b"); close(done) }()
	close(late)
	<-done
	if got, _ := next.cache.Get(sessionTestKey("late")); got != "b" {
		t.Fatal("late old request wrote to discarded cache")
	}
	old.Stop()
	manager.ConfigureSessionAffinityPersistence("", false)
	if next.cache.persistence != nil {
		t.Fatal("disabled persistence kept writer")
	}
	if len(readSessionTestSnapshot(t, path).Groups) != 3 {
		t.Fatal("disable did not save late binding")
	}
	next.cache.Invalidate(sessionTestKey("before"))
	manager.ConfigureSessionAffinityPersistence(path, false)
	if _, ok := next.cache.Get(sessionTestKey("before")); ok {
		t.Fatal("re-enable resurrected deleted binding")
	}
	manager.StopAutoRefresh()
	if len(readSessionTestSnapshot(t, path).Groups) != 2 {
		t.Fatal("re-enabled final flush was stale")
	}
}

func TestSessionCachePersistenceConcurrentReplacementAndStop(t *testing.T) {
	manager := NewManager(nil, NewSessionAffinitySelector(nil), nil)
	path := filepath.Join(t.TempDir(), "session-affinity.cache")
	manager.ConfigureSessionAffinityPersistence(path, false)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for i := 0; i < 15; i++ {
				selector := manager.Selector().(*SessionAffinitySelector)
				selector.cache.Set(sessionTestKey(fmt.Sprintf("%d-%d", worker, i)), "a")
				if i%5 == 0 {
					manager.SetSelector(NewSessionAffinitySelector(nil))
				}
			}
		}(worker)
	}
	workers.Wait()
	var stops sync.WaitGroup
	for range 3 {
		stops.Add(1)
		go func() { defer stops.Done(); manager.StopAutoRefresh() }()
	}
	stops.Wait()
	if got := readSessionTestSnapshot(t, path); len(got.Groups) != 60 {
		t.Fatalf("concurrent handoff lost bindings: %d", len(got.Groups))
	}
}

func TestSessionCachePersistenceRestoreRevisionExcludesLaterMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-affinity.cache")
	if errWrite := writeSessionSnapshot(path, sessionSnapshot{Version: 1, Groups: []sessionSnapshotGroup{{
		AuthID: "a", Aliases: []string{sessionTestKey("restored")}, ExpiresAt: time.Now().Add(time.Hour),
	}}}); errWrite != nil {
		t.Fatal(errWrite)
	}
	cache := NewSessionCache(time.Hour)
	defer cache.Stop()
	revision, unchanged := cache.restoreSnapshot(path)
	if !unchanged {
		t.Fatal("valid restore unexpectedly needed normalization")
	}
	cache.Set(sessionTestKey("late"), "b")
	p := newSessionCachePersistence(cache, path)
	p.savedRevision = revision
	p.flush()
	if got := readSessionTestSnapshot(t, path); len(got.Groups) != 2 {
		t.Fatal("post-restore mutation was marked as already saved")
	}
}

func TestManagerSessionAffinityPersistenceStopThenReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-affinity.cache")
	old := NewSessionAffinitySelector(nil)
	manager := NewManager(nil, old, nil)
	manager.ConfigureSessionAffinityPersistence(path, false)
	old.cache.Set(sessionTestKey("old"), "a")
	manager.StopAutoRefresh()
	next := NewSessionAffinitySelector(nil)
	manager.SetSelector(next)
	if next.cache == old.cache {
		t.Fatal("replacement inherited stopped cache")
	}
	manager.ConfigureSessionAffinityPersistence(path, false)
	next.cache.Set(sessionTestKey("new"), "b")
	manager.StopAutoRefresh()
	if got := readSessionTestSnapshot(t, path); len(got.Groups) != 1 || got.Groups[0].AuthID != "b" {
		t.Fatalf("replacement cache failed to start persistence: %+v", got.Groups)
	}
}
