package flea

import (
	"testing"
)

// TestView_Get_ReverseOrder verifies that Desc().Get() returns records
// in reverse insertion order while preserving the predicate filter.
func TestView_Get_ReverseOrder(t *testing.T) {
	store := newHybridStore(t, 20)

	// 1. Get all in reverse
	got, err := store.Desc().Get(func(u testUser) bool { return true })
	if err != nil {
		t.Fatalf("desc get: %v", err)
	}
	assertIDs(t, got, seqReverse(20, 1))

	// 2. Get even IDs in reverse (20, 18, 16...)
	got, err = store.Desc().Get(func(u testUser) bool { return u.Id%2 == 0 })
	if err != nil {
		t.Fatalf("desc get even: %v", err)
	}
	want := []uint64{20, 18, 16, 14, 12, 10, 8, 6, 4, 2}
	assertIDs(t, got, want)
}

// TestView_Limit verifies that Desc().Limit(n, offset) works like a paginated
// tail, reading from the end towards the beginning.
func TestView_Limit(t *testing.T) {
	store := newHybridStore(t, 20)

	// Last 5 records (20 down to 16)
	got, err := store.Desc().Limit(5, 0)
	if err != nil {
		t.Fatalf("desc limit: %v", err)
	}
	assertIDs(t, got, seqReverse(20, 16))

	// Next 5 records (15 down to 11)
	got, err = store.Desc().Limit(5, 5)
	if err != nil {
		t.Fatalf("desc limit: %v", err)
	}
	assertIDs(t, got, seqReverse(15, 11))

	// Start past the end (should be empty)
	got, err = store.Desc().Limit(5, 25)
	if err != nil {
		t.Fatalf("desc limit: %v", err)
	}
	assertIDs(t, got, []uint64{})
}

// TestView_TakeWhile verifies that Desc().TakeWhile() stops at the first false
// from the end towards the beginning.
func TestView_TakeWhile(t *testing.T) {
	store := newHybridStore(t, 20)

	// Take from 20 down to 11
	got, err := store.Desc().TakeWhile(func(u testUser) bool {
		return u.Id > 10
	})
	if err != nil {
		t.Fatalf("desc takewhile: %v", err)
	}
	assertIDs(t, got, seqReverse(20, 11))

	// Take from 20 down to 15
	got, err = store.Desc().TakeWhile(func(u testUser) bool {
		return u.Id >= 15
	})
	if err != nil {
		t.Fatalf("desc takewhile: %v", err)
	}
	assertIDs(t, got, seqReverse(20, 15))
}

// TestView_DeleteLimit verifies that Desc().DeleteLimit() deletes from the end
// towards the beginning, respecting the offset and limit.
func TestView_DeleteLimit(t *testing.T) {
	t.Run("deletes last N matches with offset", func(t *testing.T) {
		store := newHybridStore(t, 20)

		// Delete 2 even IDs, skipping the first match from the end (20), so deletes 18 and 16
		deleted, err := store.Desc().DeleteLimit(2, 1, func(u testUser) bool {
			return u.Id%2 == 0
		})
		if err != nil {
			t.Fatalf("desc delete limit: %v", err)
		}
		assertIDs(t, deleted, []uint64{18, 16})

		// Verify 20 is still there, but 18 and 16 are gone from the store
		remaining, _ := store.Get(func(u testUser) bool { return true })
		assertIDs(t, remaining, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 19, 20})

		// Verify from the view as well
		remainingView, _ := store.Desc().Get(func(u testUser) bool { return true })
		wantView := []uint64{20, 19, 17, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
		assertIDs(t, remainingView, wantView)
	})
}

// TestView_SharesState verifies that a View reads from the same live data
// as the Store. If you mutate the Store, the View should see the changes.
func TestView_SharesState(t *testing.T) {
	store := newHybridStore(t, 10)
	view := store.Desc()

	// Insert a new record in the Store
	_, err := store.Put(testUser{Id: 11, Val: 11})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// The View should see the new record at the top (first in reverse)
	got, err := view.Get(func(u testUser) bool { return true })
	if err != nil {
		t.Fatalf("view get: %v", err)
	}
	assertIDs(t, got, seqReverse(11, 1))
}

// Helper to generate a reverse sequence [from, to] inclusive.
func seqReverse(from, to uint64) []uint64 {
	if from < to {
		return []uint64{}
	}
	out := make([]uint64, 0, from-to+1)
	for i := from; i >= to; i-- {
		out = append(out, i)
	}
	return out
}