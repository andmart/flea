package fleastore

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"sync"
)

// Predicate represents a pure boolean function used to filter stored values.
//
// A Predicate is applied to each non-deleted record in insertion order.
// It must be deterministic and free of side effects.
//
// The function receives a value of type T as stored in the engine
// (i.e. after normalization or transformation by any configured Checkers).
//
// Predicates must not mutate the input value. The result of a query
// is a logical snapshot of the store state at the time of evaluation.
type Predicate[T any] func(T) bool

// Checker is a pre-write function executed before a record is inserted or updated.
//
// A Checker receives the previous value (old) and the proposed new value (new).
// The old value is nil when the operation is an insert, and non-nil on updates.
//
// A Checker can:
//   - Block the operation by returning a non-nil error.
//   - Transform the value by returning a non-nil *T, which replaces the proposed value.
//   - Allow the operation unchanged by returning (nil, nil).
//
// Checkers are executed sequentially, and the output of one Checker is passed
// as input to the next. If any Checker returns an error, the write is aborted
// and no state is modified.
type Checker[T any] func(old *T, new T) (*T, error)

// IDFunc defines how the logical identity of a record is computed.
// Records producing the same Id are considered duplicates.
// When not provided, the default implementation uses the hash of the
// JSON representation of the record.
type IDFunc[ID comparable, T any] func(T) (ID, error)

type record[T any] struct {
	value   *T
	deleted bool
}

type Store[ID comparable, T any] struct {
	mu             sync.Mutex
	records        []record[T]
	dir            string
	wal            *wal[ID, T]
	idFunc         IDFunc[ID, T]
	index          map[ID]int
	dirty          bool
	checkers       []Checker[T]
	residencyFn    func(T) bool
	hasOfflineData bool
	maxOnline      int
	onlineCount    int
}

// Put inserts a record or update in case the id is already in the index.
func (s *Store[ID, T]) Put(value T) (ID, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	id, err := s.idFunc(value)
	if err != nil {
		return id, err
	}

	var current *T

	if rec, ok := s.index[id]; ok {
		tmp := rec.value
		current = tmp
	}

	value2, err := s.runCheckers(current, value)

	if err != nil {
		return id, err
	}

	if value2 != nil {
		value = *value2
	}

	if err = s.wal.append(
		[]walOp[ID, T]{
			{
				Op:    opPut,
				ID:    id,
				Value: value,
			},
		}); err != nil {
		var zero ID
		return zero, err
	}

	s.addOrUpdate(id, &value)

	s.handleResidency()

	return id, nil

}

func (s *Store[ID, T]) PutAll(values []T) ([]ID, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	pending := make([]walOp[ID, T], 0, len(values))
	ids := make([]ID, 0, len(values))

	for _, value := range values {
		id, err := s.idFunc(value)
		if err != nil {
			return []ID{id}, err
		}

		var current *T

		if rec, ok := s.index[id]; ok {
			tmp := rec.value
			current = tmp
		}

		_, err = s.runCheckers(current, value)

		if err != nil {
			return []ID{id}, err
		}

		pending = append(pending, walOp[ID, T]{
			Op:    opPut,
			ID:    id,
			Value: value,
		})

		ids = append(ids, id)

	}

	// Phase 2: commit
	if err := s.wal.append(pending); err != nil {
		return nil, err
	}
	for _, p := range pending {
		s.addOrUpdate(p.ID, &p.Value)
	}

	s.handleResidency()

	return ids, nil
}

func (s *Store[ID, T]) Get(p Predicate[T]) []T {
	if p == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []T
	for _, r := range s.records {
		if r.deleted {
			continue
		}
		if r.value == nil {
			continue
		}
		if p(*r.value) {
			out = append(out, *r.value)
		}
	}
	if s.hasOfflineData {
		offline, err := s.getOfflineMatching(p)
		if err != nil {
			return out
		}
		out = append(out, offline...)
	}
	return out
}

func (s *Store[ID, T]) Delete(p Predicate[T]) ([]T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []T
	for idx, rec := range s.index {
		if !rec.deleted && p(*rec.value) {
			err := s.wal.append([]walOp[ID, T]{{Op: opDelete, ID: idx}})
			if err != nil {
				return nil, err
			}
			rec.deleted = true
			delete(s.index, idx)
			out = append(out, *rec.value)
			s.dirty = true
		}
	}
	return out, nil
}

func Open[ID comparable, T any](opts Options[ID, T]) (*Store[ID, T], error) {

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	s := &Store[ID, T]{
		dir:      opts.Dir,
		idFunc:   opts.IDFunc,
		index:    make(map[ID]*record[T]),
		checkers: opts.Checkers,
		residencyFn: opts.ResidencyFunc,
		maxOnline:   *opts.MaxOnlineRecords,
	}

	s.makeDirs()

	s.handleDataFile(s.residencyFn)

	if err := s.loadSnapshot(); err != nil {
		return nil, err
	}

	if err := s.replayWAL(); err != nil {
		return nil, err
	}

	w, err := openWAL[ID, T](s.getWalPath())
	if err != nil {
		return nil, err
	}
	s.wal = w

	if _, err := os.Stat(s.getDataPath()); err == nil {
		s.hasOfflineData = true
	}

	go s.snapshotLoop(opts.SnapshotInterval)

	return s, nil
}

func (s *Store[ID, T]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		return s.wal.close()
	}
	return nil
}

func (s *Store[ID, T]) addOrUpdate(id ID, value *T) {
	if rec, ok := s.index[id]; ok {
		rec.value = value
		rec.deleted = false
	} else {
		s.records = append(s.records, &record[T]{value: value})
		s.index[id] = s.records[len(s.records)-1]
		s.onlineCount++
	}
}

func (s *Store[ID, T]) runCheckers(old *T, new T) (*T, error) {
	current := &new
	for _, checker := range s.checkers {
		next, err := checker(old, *current)
		if err != nil {
			return nil, err
		}
		if next != nil {
			current = next
		}
	}
	if current == nil {
		return &new, nil
	}
	return current, nil
}

func (s *Store[ID, T]) getOfflineMatching(predicate func(T) bool) ([]T, error) {

	file, err := os.Open(s.getDataPath()) // caminho completo de data.ndjson
	if err != nil {
		return nil, err
	}
	defer file.Close()

	const batchSize = 1000

	reader := bufio.NewReader(file)

	var result []T
	batch := make([]T, 0, batchSize)

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}

		if len(bytes.TrimSpace(line)) > 0 {
			var v T
			if err := json.Unmarshal(line, &v); err != nil {
				return nil, err
			}
			batch = append(batch, v)
		}

		// Processa batch completo
		if len(batch) == batchSize {
			for _, v := range batch {
				if predicate(v) {
					result = append(result, v)
				}
			}
			batch = batch[:0]
		}

		if err == io.EOF {
			break
		}
	}

	// Processa batch final (incompleto)
	if len(batch) > 0 {
		for _, v := range batch {
			if predicate(v) {
				result = append(result, v)
			}
		}
	}

	return result, nil
}
