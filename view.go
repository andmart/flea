package flea

// View provides a read-only (and delete-capable) view over the Store.
// It is used to scan records in a different direction (usually backward).
// It shares the same underlying lock, index, and records as the original Store.
type View[ID comparable, T any] struct {
	store *Store[ID, T]
}

// Desc returns a View that scans the records in reverse order.
func (s *Store[ID, T]) Desc() *View[ID, T] {
	return &View[ID, T]{store: s}
}

// Get returns all records that match the predicate, scanning in reverse
// insertion order (newest first). It always returns a non-nil slice.
func (v *View[ID, T]) Get(p Predicate[T]) ([]T, error) {
	v.store.mu.RLock()
	defer v.store.mu.RUnlock()
	seq, errPtr := v.store.iterate(true)
	return getImpl(seq, errPtr, p)
}

// Limit returns up to n records starting at the given offset, scanning in
// reverse insertion order (newest first). Logically deleted records are
// skipped and do not count toward the offset or the limit.
// It always returns a non-nil slice.
func (v *View[ID, T]) Limit(n, offset int) ([]T, error) {
	v.store.mu.RLock()
	defer v.store.mu.RUnlock()
	seq, errPtr := v.store.iterate(true)
	return limitImpl(seq, errPtr, n, offset)
}

// TakeWhile returns records in reverse insertion order, stopping as soon as
// the predicate returns false for the first time. Logically deleted records
// are skipped and do not interrupt the scan.
// It always returns a non-nil slice.
func (v *View[ID, T]) TakeWhile(p Predicate[T]) ([]T, error) {
	v.store.mu.RLock()
	defer v.store.mu.RUnlock()
	seq, errPtr := v.store.iterate(true)
	return takeWhileImpl(seq, errPtr, p)
}

// DeleteLimit removes up to n records that match the predicate, starting from
// the given offset, scanning in reverse insertion order (descending).
// Only records that match the predicate count toward the offset and n.
// It returns the deleted values.
func (v *View[ID, T]) DeleteLimit(n, offset int, p Predicate[T]) ([]T, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()

	seq, errPtr := v.store.iterate(true)
	return deleteLimitImpl(v.store, seq, errPtr, n, offset, p)
}
