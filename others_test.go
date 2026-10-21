package flea

import (
	"testing"
)

// newHybridStore creates a store where odd IDs stay in memory and even IDs
// are moved to disk, then inserts IDs 1..n in order.
//
// This ensures every test exercises both the online and offline paths,
// which is where ordering and paging bugs tend to hide.
func newHybridStore(t *testing.T, n int) *Store[uint64, testUser] {
	t.Helper()

	dir := t.TempDir()
	minusOne := -1

	store, err := Open[uint64, testUser](Options[uint64, testUser]{
		Dir:                dir,
		MaxInMemoryRecords: &minusOne,
		IDFunc: func(u testUser) (uint64, error) {
			return u.Id, nil
		},
		ResidencyFunc: func(u testUser) bool {
			// Odd IDs stay in memory, even IDs go to disk.
			return u.Id%2 != 0
		},
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	for i := 1; i <= n; i++ {
		if _, err := store.Put(testUser{Id: uint64(i), Val: i}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	return store
}

// assertIDs checks that results have exactly the given IDs, in order.
func assertIDs(t *testing.T, results []testUser, want []uint64) {
	t.Helper()

	if results == nil {
		t.Fatalf("expected non-nil result, got nil")
	}
	if len(results) != len(want) {
		t.Fatalf("expected %d results, got %d", len(want), len(results))
	}
	for i, u := range results {
		if u.Id != want[i] {
			t.Fatalf("position %d: expected ID %d, got %d", i, want[i], u.Id)
		}
	}
}

// seq returns [from, to] inclusive as a []uint64.
func seq(from, to uint64) []uint64 {
	out := make([]uint64, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// ---------------------------------------------------------------------------
// Limit
//
// Signature is Limit(n, offset): return up to n records starting at offset,
// in insertion order. Logically deleted records are skipped and do not count
// toward the offset or the limit.
// ---------------------------------------------------------------------------

func TestLimit_HappyPath(t *testing.T) {
	store := newHybridStore(t, 20)

	t.Run("first window", func(t *testing.T) {
		got, err := store.Limit(5, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, seq(1, 5))
	})

	t.Run("middle window", func(t *testing.T) {
		got, err := store.Limit(5, 5)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, seq(6, 10))
	})

	t.Run("last aligned window", func(t *testing.T) {
		got, err := store.Limit(5, 15)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, seq(16, 20))
	})

	t.Run("truncates at end", func(t *testing.T) {
		got, err := store.Limit(5, 18)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, seq(19, 20))
	})

	t.Run("all", func(t *testing.T) {
		got, err := store.Limit(20, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, seq(1, 20))
	})

	t.Run("n larger than store", func(t *testing.T) {
		got, err := store.Limit(100, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, seq(1, 20))
	})
}

func TestLimit_Boundaries(t *testing.T) {
	store := newHybridStore(t, 20)

	t.Run("n zero", func(t *testing.T) {
		got, err := store.Limit(0, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, []uint64{})
	})

	t.Run("n negative", func(t *testing.T) {
		got, err := store.Limit(-3, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, []uint64{})
	})

	t.Run("offset negative treated as zero", func(t *testing.T) {
		got, err := store.Limit(3, -5)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, seq(1, 3))
	})

	t.Run("offset past end", func(t *testing.T) {
		got, err := store.Limit(5, 25)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, []uint64{})
	})

	t.Run("empty store", func(t *testing.T) {
		empty := newHybridStore(t, 0)
		got, err := empty.Limit(5, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, []uint64{})
	})
}

func TestLimit_SkipsDeleted(t *testing.T) {
	t.Run("deleted in the middle do not count", func(t *testing.T) {
		store := newHybridStore(t, 20)

		// Delete IDs 3, 7 and 12 (spread across memory and disk).
		if _, err := store.Delete(func(u testUser) bool {
			return u.Id == 3 || u.Id == 7 || u.Id == 12
		}); err != nil {
			t.Fatalf("delete: %v", err)
		}

		// First five surviving records are 1, 2, 4, 5, 6.
		got, err := store.Limit(5, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, []uint64{1, 2, 4, 5, 6})
	})

	t.Run("deleted prefix is skipped", func(t *testing.T) {
		store := newHybridStore(t, 20)

		if _, err := store.Delete(func(u testUser) bool {
			return u.Id <= 5
		}); err != nil {
			t.Fatalf("delete: %v", err)
		}

		got, err := store.Limit(3, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, seq(6, 8))
	})

	t.Run("all deleted", func(t *testing.T) {
		store := newHybridStore(t, 10)

		if _, err := store.Delete(func(u testUser) bool {
			return true
		}); err != nil {
			t.Fatalf("delete: %v", err)
		}

		got, err := store.Limit(5, 0)
		if err != nil {
			t.Fatalf("limit: %v", err)
		}
		assertIDs(t, got, []uint64{})
	})
}

func TestLimit_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	minusOne := -1

	open := func() *Store[uint64, testUser] {
		t.Helper()
		s, err := Open[uint64, testUser](Options[uint64, testUser]{
			Dir:                dir,
			MaxInMemoryRecords: &minusOne,
			IDFunc: func(u testUser) (uint64, error) {
				return u.Id, nil
			},
			ResidencyFunc: func(u testUser) bool {
				return u.Id%2 != 0
			},
		})
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return s
	}

	store := open()
	for i := 1; i <= 20; i++ {
		if _, err := store.Put(testUser{Id: uint64(i), Val: i}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := open()
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened: %v", err)
		}
	}()

	got, err := reopened.Limit(5, 5)
	if err != nil {
		t.Fatalf("limit: %v", err)
	}
	assertIDs(t, got, seq(6, 10))
}

// ---------------------------------------------------------------------------
// TakeWhile
//
// Returns records in insertion order, stopping as soon as the predicate
// returns false for the first time. Logically deleted records are skipped
// and do not interrupt the scan.
// ---------------------------------------------------------------------------

func TestTakeWhile_StopsAtFirstFalse(t *testing.T) {
	store := newHybridStore(t, 20)

	t.Run("stops on ordered predicate", func(t *testing.T) {
		got, err := store.TakeWhile(func(u testUser) bool {
			return u.Id <= 10
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, seq(1, 10))
	})

	t.Run("always true yields everything", func(t *testing.T) {
		got, err := store.TakeWhile(func(u testUser) bool {
			return true
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, seq(1, 20))
	})

	t.Run("always false yields empty", func(t *testing.T) {
		got, err := store.TakeWhile(func(u testUser) bool {
			return false
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, []uint64{})
	})

	t.Run("excludes last", func(t *testing.T) {
		got, err := store.TakeWhile(func(u testUser) bool {
			return u.Id < 20
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, seq(1, 19))
	})

	t.Run("only first", func(t *testing.T) {
		got, err := store.TakeWhile(func(u testUser) bool {
			return u.Id <= 1
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, seq(1, 1))
	})
}

func TestTakeWhile_AcrossMemoryAndDisk(t *testing.T) {
	store := newHybridStore(t, 20)

	t.Run("stops at memory to disk transition", func(t *testing.T) {
		// Odd IDs are online, even IDs are offline.
		// ID 7 is online and is the first false, so the scan stops there.
		got, err := store.TakeWhile(func(u testUser) bool {
			return u.Id <= 6
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, seq(1, 6))
	})

	t.Run("stops in the middle of offline records", func(t *testing.T) {
		got, err := store.TakeWhile(func(u testUser) bool {
			return u.Id <= 12
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, seq(1, 12))
	})
}

func TestTakeWhile_SkipsDeleted(t *testing.T) {
	t.Run("deleted in the middle is skipped, not a stop", func(t *testing.T) {
		store := newHybridStore(t, 20)

		if _, err := store.Delete(func(u testUser) bool {
			return u.Id == 5
		}); err != nil {
			t.Fatalf("delete: %v", err)
		}

		got, err := store.TakeWhile(func(u testUser) bool {
			return u.Id <= 10
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, []uint64{1, 2, 3, 4, 6, 7, 8, 9, 10})
	})

	t.Run("deleted prefix is skipped", func(t *testing.T) {
		store := newHybridStore(t, 20)

		if _, err := store.Delete(func(u testUser) bool {
			return u.Id == 1
		}); err != nil {
			t.Fatalf("delete: %v", err)
		}

		got, err := store.TakeWhile(func(u testUser) bool {
			return u.Id <= 5
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, seq(2, 5))
	})

	t.Run("all deleted", func(t *testing.T) {
		store := newHybridStore(t, 10)

		if _, err := store.Delete(func(u testUser) bool {
			return true
		}); err != nil {
			t.Fatalf("delete: %v", err)
		}

		got, err := store.TakeWhile(func(u testUser) bool {
			return true
		})
		if err != nil {
			t.Fatalf("takewhile: %v", err)
		}
		assertIDs(t, got, []uint64{})
	})
}

func TestTakeWhile_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	minusOne := -1

	open := func() *Store[uint64, testUser] {
		t.Helper()
		s, err := Open[uint64, testUser](Options[uint64, testUser]{
			Dir:                dir,
			MaxInMemoryRecords: &minusOne,
			IDFunc: func(u testUser) (uint64, error) {
				return u.Id, nil
			},
			ResidencyFunc: func(u testUser) bool {
				return u.Id%2 != 0
			},
		})
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return s
	}

	store := open()
	for i := 1; i <= 20; i++ {
		if _, err := store.Put(testUser{Id: uint64(i), Val: i}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := open()
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened: %v", err)
		}
	}()

	got, err := reopened.TakeWhile(func(u testUser) bool {
		return u.Id <= 10
	})
	if err != nil {
		t.Fatalf("takewhile: %v", err)
	}
	assertIDs(t, got, seq(1, 10))
}