package fleastore

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const total = 100_000

var users []User

func init() {
	users = make([]User, total)

	for i := 0; i < total; i++ {
		users[i] = genUser(i)
	}
}

func genUser(i int) User {
	return User{
		Id:        uint64(i),
		Name:      fmt.Sprintf("user-%d", i),
		Email:     fmt.Sprintf("user-%d@example.com", i),
		Age:       18 + (i % 50),
		Country:   []string{"PT", "ES", "FR", "DE", "US"}[i%5],
		Active:    i%3 != 0,
		Score:     float64(i%1000) / 10.0,
		CreatedAt: time.Now().Unix(),
	}
}

func openUserStoreWithOpts(t *testing.T, opts Options[uint64, User]) *Store[uint64, User] {
	t.Helper()

	s, err := Open[uint64, User](opts)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	return s
}

func TestOfflineResidencyAlwaysAppliedWhenMaxOnlineNil(t *testing.T) {
	dir := t.TempDir()

	opts := Options[uint64, User]{
		Dir: dir,
		ResidencyFunc: func(user User) bool {
			return user.Age > 5
		},
		IDFunc: func(user User) (uint64, error) {
			return user.Id, nil
		},
	}

	store := openUserStoreWithOpts(t, opts)

	for i := 0; i < 10; i++ {
		store.Put(User{Id: uint64(i), Age: i})
	}

	store.Close()

	store = openUserStoreWithOpts(t, opts)

	if store.onlineCount != 4 {
		t.Fatalf("online count should be 4, got %d", store.onlineCount) //913
	}
}

func TestOfflineLargeDatasetResidency(t *testing.T) {
	dir := t.TempDir()
	maxOnline := 10_000

	store, err := Open[uint64, User](Options[uint64, User]{
		Dir:              dir,
		SnapshotInterval: 0,
		IDFunc:           userID,
		Checkers:         nil,
		ResidencyFunc: func(u User) bool {
			// mantém apenas usuários ativos e jovens
			return u.Active && u.Age < 40
		},
		MaxOnlineRecords: &maxOnline,
	})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	store.PutAll(users)

	for _, rec := range store.records {
		if rec.value != nil {
			if !store.residencyFn(*rec.value) {
				t.Fatalf("invalid online record: %+v", *rec.value)
			}
		}
	}

	info, err := os.Stat(store.getDataPath())
	if err != nil {
		t.Fatalf("offline data missing: %v", err)
	}

	if info.Size() == 0 {
		t.Fatalf("offline file is empty")
	}
}

func TestOfflineLargeDatasetGetOffline(t *testing.T) {
	dir := t.TempDir()

	store, err := Open[uint64, User](Options[uint64, User]{
		IDFunc: userID,
		ResidencyFunc: func(u User) bool {
			return u.Active
		},
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	store.PutAll(users)

	// força leitura pesada do offline
	res := store.Get(func(u User) bool {
		return u.Country == "PT" && u.Score > 50
	})

	// valida semanticamente
	for _, u := range res {
		if u.Country != "PT" || u.Score <= 50 {
			t.Fatalf("invalid user returned: %+v", u)
		}
	}
}

func TestOfflineLargeDatasetSnapshotReopen(t *testing.T) {
	dir := t.TempDir()
	maxOnline := 5_000

	store, err := Open[uint64, User](Options[uint64, User]{
		IDFunc: userID,
		ResidencyFunc: func(u User) bool {
			return u.Active && u.Age < 30
		},
		Dir:              dir,
		MaxOnlineRecords: &maxOnline,
	})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	store.PutAll(users)

	if err := store.snapshot(); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	store2, err := Open[uint64, User](Options[uint64, User]{
		IDFunc: userID,
		ResidencyFunc: func(u User) bool {
			return u.Active && u.Age < 30
		},
		Dir:              dir,
		MaxOnlineRecords: &maxOnline,
	})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}

	for _, rec := range store2.records {
		if rec.value != nil {
			u := *rec.value
			if !(u.Active && u.Age < 30) {
				t.Fatalf(
					"invalid online record after reopen: %+v",
					u,
				)
			}
		}
	}
}

func TestOfflineLargeDatasetGetStreaming(t *testing.T) {
	dir := t.TempDir()

	store, err := Open[uint64, User](Options[uint64, User]{
		IDFunc: userID,
		ResidencyFunc: func(u User) bool {
			return u.Id%10 == 0
		},
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	store.PutAll(users)

	res := store.Get(func(u User) bool {
		return u.Id%777 == 0
	})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	for _, u := range res {
		if u.Id%777 != 0 {
			t.Fatalf("invalid result: %+v", u)
		}
	}
}
