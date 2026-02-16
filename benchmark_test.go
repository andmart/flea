package fleastore

import (
	"fmt"
	"math/rand"
	"testing"
)

const USERS_AMOUNT = 300000

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func BenchmarkStore_Load_Users(b *testing.B) {

	users := make([]User, USERS_AMOUNT)
	for i := 0; i < USERS_AMOUNT; i++ {
		users[i] = User{
			Name: randString(25),
			Age:  rand.Intn(100),
		}
	}

	for b.Loop() {

		b.StopTimer()

		var index uint64 = 0

		store, err := Open[uint64, User](Options[uint64, User]{
			IDFunc: func(u User) (uint64, error) {
				index++
				return index, nil
			},
			Dir: b.TempDir(),
		})
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()

		if _, err := store.PutAll(users); err != nil {
			b.Fatal(err)
		}

		getAll := store.Get(func(u User) bool {
			return true
		})

		if len(getAll) != USERS_AMOUNT {
			b.Fatalf("expected %d users, got %d", USERS_AMOUNT, len(getAll))
		}
	}
}

func BenchmarkGet_All_MixedMemoryDisk(b *testing.B) {
	dir := b.TempDir()
	minusOne := -1

	store, _ := Open[uint64, testUser](Options[uint64, testUser]{
		Dir: dir,
		IDFunc: func(u testUser) (uint64, error) {
			return u.Id, nil
		},
		MaxInMemoryRecords: &minusOne,
		ResidencyFunc: func(u testUser) bool {
			return u.Id%2 != 0
		},
	})

	values := make([]testUser, USERS_AMOUNT)
	for i := 0; i < USERS_AMOUNT; i++ {
		values[i] = testUser{Id: uint64(i + 1), Val: i}
	}
	store.PutAll(values)

	for b.Loop() {
		store.Get(func(u testUser) bool { return true })
	}
}

func BenchmarkPerf_1MUsers_90PercentInDisk_GetFromDisk(b *testing.B) {
	const total = 1_000_000
	maxOnline := 100_000 // 10% online

	// ---------------------------
	// Setup pesado (fora da medição)
	// ---------------------------
	dir := b.TempDir()

	store, err := Open[uint64, User](Options[uint64, User]{

		IDFunc: func(u User) (uint64, error) {
			return u.Id, nil
		},
		ResidencyFunc: func(u User) bool {
			return u.Id%10 == 0
		},
		Dir:                dir,
		MaxInMemoryRecords: &maxOnline,
	})
	if err != nil {
		b.Fatal(err)
	}

	users := make([]User, total)
	for i := 0; i < total; i++ {
		users[i] = User{
			Id:        uint64(i),
			Name:      fmt.Sprintf("user-%d", i),
			Email:     fmt.Sprintf("user-%d@example.com", i),
			Age:       18 + (i % 50),
			Country:   []string{"PT", "ES", "FR", "DE", "US"}[i%5],
			Active:    i%2 == 0,
			Score:     float64(i%1000) / 10.0,
			CreatedAt: 1700000000, // fixo para evitar ruído
		}
	}

	if _, err := store.PutAll(users); err != nil {
		b.Fatal(err)
	}

	// registro garantidamente offline
	targetId := uint64(999_999)

	rec, ok := store.index[targetId]
	if !ok || rec.value != nil {
		b.Fatal("target must be offline")
	}

	// ---------------------------
	// Benchmark apenas do Get
	// ---------------------------
	for b.Loop() {
		res, success, err := store.GetByID(targetId)

		if err != nil {
			b.Fatal(err)
		}
		if !success {
			b.Fatal("target not found")
		}
		if res.Name != "user-999999" {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkHandleResidency(b *testing.B) {
	minusOne := -1

	// ---- 1. Criar massa base fora do benchmark ----
	baseRecords := make([]*record[testUser], USERS_AMOUNT)
	baseIndex := make(map[uint64]*record[testUser], USERS_AMOUNT)

	for j := 0; j < USERS_AMOUNT; j++ {
		u := testUser{Id: uint64(j + 1), Val: j}
		rec := &record[testUser]{
			value: &u,
		}
		baseRecords[j] = rec
		baseIndex[u.Id] = rec
	}

	for b.Loop() {
		b.StopTimer()
		dir := b.TempDir()

		store, _ := Open[uint64, testUser](Options[uint64, testUser]{
			Dir: dir,
			IDFunc: func(u testUser) (uint64, error) {
				return u.Id, nil
			},
			MaxInMemoryRecords: &minusOne,
			ResidencyFunc: func(u testUser) bool {
				return false
			},
		})

		// ---- 2. Clonar estrutura leve ----
		store.records = make([]*record[testUser], USERS_AMOUNT)
		store.index = make(map[uint64]*record[testUser], USERS_AMOUNT)

		for j := 0; j < USERS_AMOUNT; j++ {
			u := testUser{Id: uint64(j + 1), Val: j}
			rec := &record[testUser]{value: &u}

			store.records[j] = rec
			store.index[u.Id] = rec
		}

		store.onlineCount = USERS_AMOUNT

		b.StartTimer()
		_ = store.handleResidency()
	}
}

func BenchmarkHandleResidencyWithPutAll(b *testing.B) {

	for b.Loop() {
		b.StopTimer()
		dir := b.TempDir()

		store, _ := Open[uint64, testUser](Options[uint64, testUser]{
			Dir: dir,
			IDFunc: func(u testUser) (uint64, error) {
				return u.Id, nil
			},
			MaxInMemoryRecords: nil,
			ResidencyFunc: func(u testUser) bool {
				return false
			},
		})

		values := make([]testUser, USERS_AMOUNT)
		for i := 0; i < USERS_AMOUNT; i++ {
			values[i] = testUser{Id: uint64(i + 1), Val: i}
		}

		b.StartTimer()
		store.PutAll(values)
	}
}

func BenchmarkGetByID_InMemory(b *testing.B) {
	dir := b.TempDir()

	store, _ := Open[uint64, testUser](Options[uint64, testUser]{
		Dir: dir,
		IDFunc: func(u testUser) (uint64, error) {
			return u.Id, nil
		},
	})

	n := USERS_AMOUNT
	values := make([]testUser, n)
	for i := 0; i < n; i++ {
		values[i] = testUser{Id: uint64(i + 1), Val: i}
	}

	store.PutAll(values)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := uint64((i % n) + 1)
		_, _, _ = store.GetByID(id)
	}
}

func BenchmarkGetByID_OnDisk(b *testing.B) {
	dir := b.TempDir()
	minusOne := -1

	store, _ := Open[uint64, testUser](Options[uint64, testUser]{
		Dir: dir,
		IDFunc: func(u testUser) (uint64, error) {
			return u.Id, nil
		},
		MaxInMemoryRecords: &minusOne,
		ResidencyFunc: func(u testUser) bool {
			return false
		},
	})

	n := USERS_AMOUNT
	values := make([]testUser, n)
	for i := 0; i < n; i++ {
		values[i] = testUser{Id: uint64(i + 1), Val: i}
	}

	store.PutAll(values)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := uint64((i % USERS_AMOUNT) + 1)
		u, s, err := store.GetByID(id)
		if err != nil {
			b.Fatal(err)
		}
		if !s {
			b.Fatal("expected to find a single user")
		}
		if u.Id != id {
			b.Fatalf("wrong user")
		}
	}

}
