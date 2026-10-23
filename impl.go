package flea

import (
	"iter"
	"slices"
)

// ---------------------------------------------------------------------------
// Core Iterator
// ---------------------------------------------------------------------------

// iterate returns an iterator sequence over the store's records in the requested direction.
// It skips logically deleted records, loads offline data on demand, and yields
// the record pointer and the materialized value.
//
// If a disk load error occurs, the sequence is halted and the error is stored in
// the returned error pointer.
//
// The caller MUST hold the appropriate lock (RLock for reading, Lock for writing).
func (s *Store[ID, T]) iterate(desc bool) (iter.Seq2[*record[T], T], *error) {
	var iterErr error

	seq := func(yield func(*record[T], T) bool) {
		var base iter.Seq2[int, *record[T]]
		if desc {
			base = slices.Backward(s.records)
		} else {
			base = slices.All(s.records)
		}

		for _, rec := range base {
			if rec.deleted {
				continue
			}

			var val T
			if rec.value != nil {
				val = *rec.value
			} else {
				loaded, err := s.loadFromDisk(rec.offset, rec.size)
				if err != nil {
					iterErr = err
					return // Stop iteration on disk error
				}
				val = loaded
			}

			if !yield(rec, val) {
				break // Stop iteration if the caller returns false
			}
		}
	}

	return seq, &iterErr
}

// ---------------------------------------------------------------------------
// Shared Logic Implementations
// ---------------------------------------------------------------------------

// getImpl implements the logic for Get. It works for both Store and View.
func getImpl[T any](seq iter.Seq2[*record[T], T], errPtr *error, p Predicate[T]) ([]T, error) {
	results := make([]T, 0, 10000)
	for _, val := range seq {
		if p(val) {
			results = append(results, val)
		}
	}

	if *errPtr != nil {
		return nil, *errPtr
	}
	return results, nil
}

// limitImpl implements the logic for Limit. It works for both Store and View.
func limitImpl[T any](seq iter.Seq2[*record[T], T], errPtr *error, n, offset int) ([]T, error) {
	if n <= 0 {
		return []T{}, nil
	}
	if offset < 0 {
		offset = 0
	}

	results := make([]T, 0)
	skipped := 0

	for _, val := range seq {
		if skipped < offset {
			skipped++
			continue
		}
		results = append(results, val)
		if len(results) >= n {
			break
		}
	}

	if *errPtr != nil {
		return nil, *errPtr
	}
	return results, nil
}

// takeWhileImpl implements the logic for TakeWhile. It works for both Store and View.
func takeWhileImpl[T any](seq iter.Seq2[*record[T], T], errPtr *error, p Predicate[T]) ([]T, error) {
	results := make([]T, 0)
	for _, val := range seq {
		if !p(val) {
			break // Stop at the first false
		}
		results = append(results, val)
	}

	if *errPtr != nil {
		return nil, *errPtr
	}
	return results, nil
}

// deleteImpl implements the logic for Delete. It works for both Store and View.
// It receives the underlying *Store to write to the WAL, and the pre-built sequence.
func deleteImpl[ID comparable, T any](s *Store[ID, T], seq iter.Seq2[*record[T], T], errPtr *error, p Predicate[T]) ([]T, error) {
	out := make([]T, 0)

	for rec, val := range seq {
		if p(val) {
			// Compute the ID for the WAL log
			idx, err := s.idFunc(val)
			if err != nil {
				return out, err
			}

			// Append delete operation to WAL
			if err := s.wal.append([]walOp[ID, T]{{Op: opDelete, ID: idx}}); err != nil {
				return out, err
			}

			// Mark the record as deleted using the shared pointer
			rec.deleted = true

			// Collect the deleted value
			out = append(out, val)
		}
	}

	// Check if a load-from-disk error occurred during iteration
	if *errPtr != nil {
		return out, *errPtr
	}
	return out, nil
}

// deleteLimitImpl implements the logic for DeleteLimit.
// It scans the sequence, skips the first `offset` records that match the predicate,
// and then deletes up to `n` records that match. Returns the deleted values.
func deleteLimitImpl[ID comparable, T any](s *Store[ID, T], seq iter.Seq2[*record[T], T], errPtr *error, n, offset int, p Predicate[T]) ([]T, error) {
	if n <= 0 {
		return []T{}, nil
	}
	if offset < 0 {
		offset = 0
	}

	out := make([]T, 0, n)
	skipped := 0

	for rec, val := range seq {
		// Only records that match the predicate count toward offset and n.
		if !p(val) {
			continue
		}

		if skipped < offset {
			skipped++
			continue
		}

		if len(out) >= n {
			break
		}

		idx, err := s.idFunc(val)
		if err != nil {
			return out, err
		}

		if err := s.wal.append([]walOp[ID, T]{{Op: opDelete, ID: idx}}); err != nil {
			return out, err
		}

		rec.deleted = true
		out = append(out, val)
	}

	if *errPtr != nil {
		return out, *errPtr
	}
	return out, nil
}