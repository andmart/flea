package flea

import (
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
	offset  int64
	size    int64
}

type Store[ID comparable, T any] struct {
	mu             sync.RWMutex
	records        []*record[T]
	dir            string
	wal            *wal[ID, T]
	idFunc         IDFunc[ID, T]
	index          map[ID]*record[T]
	dirty          bool
	checkers       []Checker[T]
	residencyFn    func(T) bool
	hasOfflineData bool
	maxInMemory    int
	onlineCount    int
	dataFile       *os.File
	dataWindow     *dataWindow
	isDesc         bool
}

// --- STORE API ---

func (s *Store[ID, T]) Get(p Predicate[T]) ([]T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seq, errPtr := s.iterate(false)
	return getImpl(seq, errPtr, p)
}

func (s *Store[ID, T]) Limit(n, offset int) ([]T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seq, errPtr := s.iterate(false)
	return limitImpl(seq, errPtr, n, offset)
}

func (s *Store[ID, T]) TakeWhile(p Predicate[T]) ([]T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seq, errPtr := s.iterate(false)
	return takeWhileImpl(seq, errPtr, p)
}

func (s *Store[ID, T]) Delete(p Predicate[T]) ([]T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq, errPtr := s.iterate(false)
	return deleteImpl(s, seq, errPtr, p)
}

// DeleteLimit removes up to n records that match the predicate, starting from
// the given offset, scanning in insertion order (ascending).
// Only records that match the predicate count toward the offset and n.
// It returns the deleted values.
func (s *Store[ID, T]) DeleteLimit(n, offset int, p Predicate[T]) ([]T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seq, errPtr := s.iterate(false)
	return deleteLimitImpl(s, seq, errPtr, n, offset, p)
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

func (s *Store[ID, T]) Count() int {
	return len(s.index)
}

func (s *Store[ID, T]) CountIf(p Predicate[T]) (int, error) {
	recs, err := s.Get(p)
	return len(recs), err
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



// Return the value if exists, a bool representing if the value exists or not, and an error if something goes wrong.
func (s *Store[ID, T]) GetByID(id ID) (T, bool, error) {
	var v T
	rec, ok := s.index[id]
	if !ok || rec.deleted {
		return v, false, nil
	}

	if rec.value != nil {
		v = *rec.value
		return v, true, nil
	}

	// carregar do disco
	loaded, err := s.loadFromDisk(rec.offset, rec.size)
	if err != nil {
		return v, false, err
	}

	v = loaded
	return v, true, nil
}


func Open[ID comparable, T any](opts Options[ID, T]) (*Store[ID, T], error) {

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	s := &Store[ID, T]{
		dir:         opts.Dir,
		idFunc:      opts.IDFunc,
		index:       make(map[ID]*record[T]),
		checkers:    opts.Checkers,
		residencyFn: opts.ResidencyFunc,
		maxInMemory: *opts.MaxInMemoryRecords,
		dataWindow:  &dataWindow{},
	}

	s.makeDirs()

	hasFiles := s.checkFiles()

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

	if !hasFiles && opts.OnCreate != nil {
		if err := opts.OnCreate(s); err != nil {
			s.Close()
			return nil, err
		}
	}

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

/*func (s *Store[ID, T]) getOfflineMatching(predicate func(T) bool) ([]T, error) {
	//
	//file, err := os.Open(s.getDataPath())
	//if err != nil {
	//	return nil, err
	//}
	//defer file.Close()

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

	if len(batch) > 0 {
		for _, v := range batch {
			if predicate(v) {
				result = append(result, v)
			}
		}
	}

	return result, nil
}
*/
