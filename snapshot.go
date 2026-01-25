package fleastore

import (
	"bufio"
	"encoding/json"
	"os"
	"time"
)

func (s *Store[ID, T]) snapshotLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for range t.C {
		s.mu.Lock()
		_ = s.snapshot()
		s.mu.Unlock()
	}
}

func (s *Store[ID, T]) loadSnapshot() error {
	path := s.getSnapshotPath()
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var i T
		if err := json.Unmarshal(sc.Bytes(), &i); err != nil {
			return err
		}
		s.records = append(s.records, &record[T]{value: &i})
	}
	s.recreateIndex()
	return nil
}

func (s *Store[ID, T]) snapshot() error {
	tmp := s.getPath("snapshot.tmp")
	final := s.getSnapshotPath()

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if s.dirty {
		s.compact()
		s.dirty = false
	}

	enc := json.NewEncoder(f)
	for _, r := range s.records {
		if r.deleted {
			continue
		}
		if err := enc.Encode(r.value); err != nil {
			f.Close()
			return err
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()

	if err := os.Rename(tmp, final); err != nil {
		return err
	}

	// reset WAL
	truncate(s.wal.file)

	return nil
}

func (s *Store[ID, T]) compact() {
	out := make([]*record[T], 0, len(s.index))
	newIndex := make(map[ID]*record[T], len(s.index))

	for _, rec := range s.records {
		if rec.deleted {
			continue
		}
		id, _ := s.idFunc(*rec.value)
		newIndex[id] = rec
		out = append(out, rec)
	}
	s.records = out
	s.index = newIndex
}

func (s *Store[ID, T]) recreateIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index = make(map[ID]*record[T])
	for _, rec := range s.records {
		id, err := s.idFunc(*rec.value)
		if err != nil {
			continue
		}
		s.index[id] = rec
	}
}
