package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const sessionSnapshotVersion = 1

type sessionSnapshot struct {
	Version int                    `json:"version"`
	Groups  []sessionSnapshotGroup `json:"groups"`
}

// Groups are stored from oldest to newest to preserve whole-group eviction.
type sessionSnapshotGroup struct {
	AuthID    string    `json:"auth_id"`
	Aliases   []string  `json:"aliases"`
	ExpiresAt time.Time `json:"expires_at"`
}

type sessionCachePersistence struct {
	cache         *SessionCache
	path          string
	flushMu       sync.Mutex
	savedRevision uint64
	warned        bool
	stopCh        chan struct{}
	doneCh        chan struct{}
	stopOnce      sync.Once
	save          func(string, sessionSnapshot) error
}

func (c *SessionCache) configurePersistence(path string, restore bool) {
	c.persistenceMu.Lock()
	defer c.persistenceMu.Unlock()
	if c.stopped || (c.persistence == nil && path == "") ||
		(c.persistence != nil && c.persistence.path == path) {
		return
	}
	if c.persistence != nil {
		c.persistence.stop()
		c.persistence = nil
	}
	if path == "" {
		return
	}
	p := newSessionCachePersistence(c, path)
	if restore {
		if revision, unchanged := c.restoreSnapshot(path); unchanged {
			p.savedRevision = revision
		}
	} else {
		// Live memory is authoritative, including an empty cache with an old file.
		c.mu.Lock()
		c.revision++
		c.mu.Unlock()
	}
	c.persistence = p
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		p.run(ticker.C)
	}()
}

func newSessionCachePersistence(cache *SessionCache, path string) *sessionCachePersistence {
	return &sessionCachePersistence{
		cache: cache, path: path, stopCh: make(chan struct{}), doneCh: make(chan struct{}),
		save: writeSessionSnapshot,
	}
}

func (p *sessionCachePersistence) run(ticks <-chan time.Time) {
	defer close(p.doneCh)
	for {
		select {
		case <-p.stopCh:
			p.flush()
			return
		case <-ticks:
			p.flush()
		}
	}
}

func (p *sessionCachePersistence) stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
	<-p.doneCh
}

func (c *SessionCache) flushPersistence() {
	c.persistenceMu.Lock()
	defer c.persistenceMu.Unlock()
	if c.persistence != nil {
		c.persistence.flush()
	}
}

func (p *sessionCachePersistence) flush() {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()
	c := p.cache
	c.mu.Lock()
	c.cleanupLocked(time.Now())
	revision := c.revision
	if revision == p.savedRevision {
		c.mu.Unlock()
		return
	}
	snapshot := sessionSnapshot{Version: sessionSnapshotVersion, Groups: make([]sessionSnapshotGroup, 0, len(c.groups))}
	for element := c.evictionOrder.Front(); element != nil; element = element.Next() {
		group := c.groups[element.Value.(string)]
		snapshot.Groups = append(snapshot.Groups, sessionSnapshotGroup{
			AuthID: group.authID, Aliases: append([]string(nil), group.aliases...), ExpiresAt: group.expiresAt,
		})
	}
	c.mu.Unlock()
	if errSave := p.save(p.path, snapshot); errSave != nil {
		if !p.warned {
			log.WithError(errSave).Warn("failed to save session affinity snapshot; will retry")
			p.warned = true
		}
		return
	}
	p.savedRevision = revision
	p.warned = false
}

// restoreSnapshot returns whether the file already represents the restored state.
func (c *SessionCache) restoreSnapshot(path string) (uint64, bool) {
	data, errRead := os.ReadFile(path)
	if os.IsNotExist(errRead) {
		return 0, false
	}
	if errRead != nil {
		log.WithError(errRead).Warn("failed to read session affinity snapshot; using memory cache")
		return 0, false
	}
	var snapshot sessionSnapshot
	if errDecode := json.Unmarshal(data, &snapshot); errDecode != nil {
		log.Warn("invalid session affinity snapshot; using memory cache")
		return 0, false
	}
	if snapshot.Version != sessionSnapshotVersion {
		log.Warn("unsupported session affinity snapshot version; using memory cache")
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitializedLocked()
	now := time.Now()
	unchanged := len(c.entries) == 0
	invalid := false
	for _, group := range snapshot.Groups {
		if !validSessionSnapshotGroup(group) {
			invalid = true
			unchanged = false
			continue
		}
		if !now.Before(group.ExpiresAt) {
			unchanged = false
			continue
		}
		previous := make([]sessionEntry, 0, len(group.Aliases))
		conflict := false
		for _, alias := range group.Aliases {
			if entry, ok := c.entries[alias]; ok {
				if now.Before(entry.expiresAt) {
					conflict = true
					break
				}
				previous = append(previous, entry)
			}
		}
		if conflict {
			unchanged = false
			continue
		}
		groupCount := len(c.groups)
		c.replaceAliasGroupsLocked(group.AuthID, group.ExpiresAt, group.Aliases, previous...)
		if len(c.groups) < groupCount+1 {
			// Capacity pruning changes the persisted representation.
			unchanged = false
		}
	}
	if invalid {
		log.Warn("ignored invalid records in session affinity snapshot")
	}
	if !unchanged {
		c.revision++
	}
	return c.revision, unchanged
}

func validSessionSnapshotGroup(group sessionSnapshotGroup) bool {
	if strings.TrimSpace(group.AuthID) == "" || group.ExpiresAt.IsZero() || len(group.Aliases) == 0 ||
		len(group.Aliases) > maxStableSessionAliases+1 ||
		!equalSessionAliases(group.Aliases, compactSessionAliases(mergeSessionAliases(nil, group.Aliases...))) {
		return false
	}
	for _, alias := range group.Aliases {
		provider, rest, ok := strings.Cut(alias, "::")
		separator := strings.LastIndex(rest, "::")
		if !ok || provider == "" || separator <= 0 || separator+2 == len(rest) {
			return false
		}
	}
	return true
}

func writeSessionSnapshot(path string, snapshot sessionSnapshot) error {
	data, errEncode := json.Marshal(snapshot)
	if errEncode != nil {
		return fmt.Errorf("encode session affinity snapshot: %w", errEncode)
	}
	dir := filepath.Dir(path)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		return fmt.Errorf("create session affinity directory: %w", errMkdir)
	}
	if errChmod := os.Chmod(dir, 0o700); errChmod != nil {
		return fmt.Errorf("restrict session affinity directory: %w", errChmod)
	}
	file, errCreate := os.CreateTemp(dir, ".session-affinity-*")
	if errCreate != nil {
		return fmt.Errorf("create session affinity temporary file: %w", errCreate)
	}
	closed := false
	defer func() {
		if !closed {
			if errClose := file.Close(); errClose != nil {
				log.WithError(errClose).Warn("failed to close session affinity temporary file")
			}
		}
		if errRemove := os.Remove(file.Name()); errRemove != nil && !os.IsNotExist(errRemove) {
			log.WithError(errRemove).Warn("failed to remove session affinity temporary file")
		}
	}()
	if _, errWrite := file.Write(data); errWrite != nil {
		return fmt.Errorf("write session affinity snapshot: %w", errWrite)
	}
	if errSync := file.Sync(); errSync != nil {
		return fmt.Errorf("sync session affinity snapshot: %w", errSync)
	}
	errClose := file.Close()
	closed = true
	if errClose != nil {
		return fmt.Errorf("close session affinity snapshot: %w", errClose)
	}
	if errRename := os.Rename(file.Name(), path); errRename != nil {
		return fmt.Errorf("replace session affinity snapshot: %w", errRename)
	}
	return nil
}
