package fleastore

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestStore_Load_100000Users(t *testing.T) {
	const total = 100_000

	var index uint64 = 0

	store, err := Open[uint64, User](Options[uint64, User]{
		IDFunc: func(u User) (uint64, error) {
			index++
			return index, nil
		},
		Dir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	users := make([]User, 0, total)

	for i := 0; i < total; i++ {
		users = append(users, User{
			Name: randString(25),
			Age:  rand.Intn(100),
		})
	}

	startPut := time.Now()

	fmt.Println("startPut", startPut)

	if _, err := store.PutAll(users); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	putDuration := time.Since(startPut)
	t.Logf("Put %d users took %s (avg %s/op)",
		total,
		putDuration,
		putDuration/time.Duration(total),
	)

	startGet := time.Now()

	getAll := store.Get(func(u User) bool {
		return true
	})

	getDuration := time.Since(startGet)
	t.Logf("Get %d users took %s", len(getAll), getDuration)

	if len(getAll) != total {
		t.Fatalf("expected %d users, got %d", total, len(getAll))
	}
}
