package fleastore

import (
	"testing"
)

type testUser struct {
	Id  uint64
	Val int
}

func TestMaxInMemoryNilUnlimited(t *testing.T) {
	dir := t.TempDir()

	store, err := Open[uint64, testUser](Options[uint64, testUser]{
		Dir: dir,
		IDFunc: func(u testUser) (uint64, error) {
			return u.Id, nil
		},
		MaxInMemoryRecords: nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	values := make([]testUser, 100)
	for i := range values {
		values[i] = testUser{Id: uint64(i + 1), Val: i}
	}

	if _, err := store.PutAll(values); err != nil {
		t.Fatal(err)
	}

	count := 0
	for _, rec := range store.records {
		if rec.value != nil && !rec.deleted {
			count++
		}
	}
	if count != 100 {
		t.Fatalf("expected 100 in-memory records, got %d", count)
	}
}

func TestMaxInMemoryCap(t *testing.T) {
	dir := t.TempDir()
	max := 10

	store, err := Open[uint64, testUser](Options[uint64, testUser]{
		Dir: dir,
		IDFunc: func(u testUser) (uint64, error) {
			return u.Id, nil
		},
		MaxInMemoryRecords: &max,
		ResidencyFunc: func(u testUser) bool {
			return false
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	values := make([]testUser, 50)
	for i := range values {
		values[i] = testUser{Id: uint64(i + 1), Val: i}
	}

	if _, err := store.PutAll(values); err != nil {
		t.Fatal(err)
	}

	count := 0
	for _, rec := range store.records {
		if rec.value != nil && !rec.deleted {
			count++
		}
	}
	if count > 10 {
		t.Fatalf("expected at most 10 in-memory records, got %d", count)
	}
}

func TestMaxInMemoryMinusOneAlwaysRunsResidency(t *testing.T) {
	dir := t.TempDir()
	minusOne := -1

	store, err := Open[uint64, testUser](Options[uint64, testUser]{
		Dir: dir,
		IDFunc: func(u testUser) (uint64, error) {
			return u.Id, nil
		},
		MaxInMemoryRecords: &minusOne,
		ResidencyFunc: func(u testUser) bool {
			return false
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	values := make([]testUser, 20)
	for i := range values {
		values[i] = testUser{Id: uint64(i + 1), Val: i}
	}

	if _, err := store.PutAll(values); err != nil {
		t.Fatal(err)
	}

	count := 0
	for _, rec := range store.records {
		if rec.value != nil && !rec.deleted {
			count++
		}
	}
	if count != 0 {
		t.Fatalf("expected 0 in-memory records, got %d", count)
	}
}
