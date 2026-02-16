package fleastore

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type User struct {
	Id        uint64
	Name      string
	Email     string
	Age       int
	Country   string
	Active    bool
	Score     float64
	CreatedAt int64
}

type Order struct {
	Id     uint64
	Amount int
	User   *User
}

func userID(u User) (uint64, error) {
	return uint64(u.Id), nil
}

func all[T any](T) bool { return true }

func openUserStore(t *testing.T, dir string, checkers ...Checker[User]) *Store[uint64, User] {
	t.Helper()

	s, err := Open[uint64, User](Options[uint64, User]{
		IDFunc:   userID,
		Checkers: checkers,
		Dir:      dir,
	})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	return s
}

func openOrderStore(t *testing.T, dir string, checkers ...Checker[Order]) *Store[uint64, Order] {
	t.Helper()

	s, err := Open[uint64, Order](Options[uint64, Order]{
		IDFunc:   func(o Order) (uint64, error) { return o.Id, nil },
		Checkers: checkers,
		Dir:      dir,
	})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	return s
}

func TestPutUpdateAndGet(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)

	_, _ = s.Put(User{Id: 1, Name: "Alice"})
	_, _ = s.Put(User{Id: 2, Name: "Bob"})
	_, _ = s.Put(User{Id: 1, Name: "Alice v2"}) // update

	users := s.Get(all[User])

	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	if users[0].Name != "Alice v2" {
		t.Fatalf("update not applied")
	}
}

func TestOpenFailsWhenIDFuncIsNil(t *testing.T) {
	store, err := Open[uint64, int](Options[uint64, int]{})

	if err == nil {
		t.Fatalf("expected error when IDFunc is nil")
	}

	if store != nil {
		t.Fatalf("expected store to be nil when error occurs")
	}
}

func TestStore_PersistUsersAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)

	u1 := User{Id: 1, Name: "Alice"}
	u2 := User{Id: 2, Name: "Bob"}

	s.Put(u1)
	s.Put(u2)

	users := s.Get(func(User) bool { return true })
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store failed: %v", err)
	}

	// ---- restart ----
	s = openUserStore(t, dir)
	defer s.Close()

	users = s.Get(func(User) bool { return true })
	if len(users) != 2 {
		t.Fatalf("expected 2 users after restart, got %d", len(users))
	}

	found := map[uint64]User{}
	for _, u := range users {
		found[u.Id] = u
	}

	if found[1].Name != "Alice" {
		t.Fatalf("user 1 name mismatch: %s", found[1].Name)
	}
	if found[2].Name != "Bob" {
		t.Fatalf("user 2 name mismatch: %s", found[2].Name)
	}
}

func TestStore_TwoTypesInParallel(t *testing.T) {

	dir := t.TempDir()

	done := make(chan struct{}, 2)

	// ---- users store ----
	go func() {
		userStore := openUserStore(t, dir)

		u1 := User{Id: 1, Name: "Alice"}
		u2 := User{Id: 2, Name: "Bob"}

		userStore.Put(u1)
		userStore.Put(u2)

		if err := userStore.Close(); err != nil {
			t.Errorf("close user store: %v", err)
		}

		done <- struct{}{}
	}()

	// ---- orders store ----
	go func() {
		orderStore := openOrderStore(t, dir)

		o1 := Order{Id: 100, Amount: 50}
		o2 := Order{Id: 200, Amount: 75}

		orderStore.Put(o1)
		orderStore.Put(o2)

		if err := orderStore.Close(); err != nil {
			t.Errorf("close order store: %v", err)
		}

		done <- struct{}{}
	}()

	// aguarda ambos
	<-done
	<-done

	// ---- reopen users ----
	userStore := openUserStore(t, dir)

	defer userStore.Close()

	users := userStore.Get(func(User) bool { return true })
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	// ---- reopen orders ----
	orderStore := openOrderStore(t, dir)
	defer orderStore.Close()

	orders := orderStore.Get(func(Order) bool { return true })
	if len(orders) != 2 {
		t.Fatalf("expected 2 orders, got %d", len(orders))
	}
}

func TestNewStore_RequiresIDFunc(t *testing.T) {
	_, err := Open[uint64, User](Options[uint64, User]{})
	if err == nil {
		t.Fatalf("expected error when IDFunc is nil")
	}
}

func TestPut_User_BlockChecker(t *testing.T) {
	dir := t.TempDir()

	checker := func(old *User, new User) (*User, error) {
		if new.Age < 0 {
			return nil, fmt.Errorf("invalid age")
		}
		return nil, nil
	}

	s := openUserStore(t, dir, checker)

	_, err := s.Put(User{
		Id:   1,
		Name: "Alice",
		Age:  -10,
	})

	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	users := s.Get(nil)
	if len(users) != 0 {
		t.Fatalf("expected no records, got %d", len(users))
	}
}

func TestPut_UpdatePreservesInsertionOrder(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "A"})
	s.Put(User{Id: 2, Name: "B"})
	s.Put(User{Id: 1, Name: "A v2"}) // update

	users := s.Get(all[User])

	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	if users[0].Id != 1 || users[1].Id != 2 {
		t.Fatalf("insertion order not preserved: %+v", users)
	}
}

func TestPut_UpdateDoesNotCreateDuplicate(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "Alice"})
	s.Put(User{Id: 1, Name: "Alice v2"})
	s.Put(User{Id: 1, Name: "Alice v3"})

	users := s.Get(all[User])

	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}

	if users[0].Name != "Alice v3" {
		t.Fatalf("unexpected value after update: %+v", users[0])
	}
}

func TestDelete_ByPredicate(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "Alice", Age: 10})
	s.Put(User{Id: 2, Name: "Bob", Age: 20})
	s.Put(User{Id: 3, Name: "Carol", Age: 30})

	deleted, err := s.Delete(func(u User) bool {
		return u.Age < 18
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deleted) != 1 || deleted[0].Id != 1 {
		t.Fatalf("unexpected deleted users: %+v", deleted)
	}

	users := s.Get(all[User])
	if len(users) != 2 {
		t.Fatalf("expected 2 remaining users, got %d", len(users))
	}
}

func TestDelete_PersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)

	s.Put(User{Id: 1, Name: "Alice"})
	s.Put(User{Id: 2, Name: "Bob"})

	s.Delete(func(u User) bool { return u.Id == 1 })

	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// ---- restart ----
	s = openUserStore(t, dir)
	defer s.Close()

	users := s.Get(all[User])

	if len(users) != 1 {
		t.Fatalf("expected 1 user after restart, got %d", len(users))
	}

	if users[0].Id != 2 {
		t.Fatalf("unexpected remaining user: %+v", users[0])
	}
}

func TestPut_User_NormalizeChecker(t *testing.T) {
	dir := t.TempDir()

	checker := func(old *User, new User) (*User, error) {
		u := new
		u.Name = strings.ToUpper(u.Name)
		return &u, nil
	}

	s := openUserStore(t, dir, checker)
	defer s.Close()

	s.Put(User{Id: 1, Name: "alice"})

	users := s.Get(all[User])

	if users[0].Name != "ALICE" {
		t.Fatalf("expected normalized name, got %s", users[0].Name)
	}
}

func TestPut_CheckerSeesOldOnUpdate(t *testing.T) {

	dir := t.TempDir()

	calledWithOld := false

	checker := func(old *User, new User) (*User, error) {
		if old != nil {
			calledWithOld = true
		}
		return nil, nil
	}

	s := openUserStore(t, dir, checker)
	defer s.Close()

	s.Put(User{Id: 1, Name: "Alice"})
	s.Put(User{Id: 1, Name: "Alice v2"})

	if !calledWithOld {
		t.Fatalf("checker was not called with old value on update")
	}
}

func TestCheckerDoesNotRunOnReplay(t *testing.T) {

	dir := t.TempDir()

	blockingChecker := func(old *User, new User) (*User, error) {
		return nil, fmt.Errorf("should not run")
	}

	s := openUserStore(t, dir)
	s.Put(User{Id: 1, Name: "Alice"})
	s.Close()

	s = openUserStore(t, dir, blockingChecker)
	defer s.Close()

	users := s.Get(all[User])
	if len(users) != 1 {
		t.Fatalf("expected user to be restored despite checker")
	}
}

func TestDelete_NoMatchIsNoOp(t *testing.T) {

	dir := t.TempDir()

	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "Alice"})

	deleted, err := s.Delete(func(u User) bool {
		return u.Id == 999
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deleted) != 0 {
		t.Fatalf("expected no deleted users, got %+v", deleted)
	}

	users := s.Get(all[User])
	if len(users) != 1 {
		t.Fatalf("unexpected state after delete")
	}
}

func TestGet_NoMatch(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "Alice"})

	users := s.Get(func(u User) bool {
		return u.Age > 100
	})

	if len(users) != 0 {
		t.Fatalf("expected empty result, got %d", len(users))
	}
}

func TestDelete_All(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "A"})
	s.Put(User{Id: 2, Name: "B"})

	deleted, err := s.Delete(all[User])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleted users, got %d", len(deleted))
	}

	users := s.Get(all[User])
	if len(users) != 0 {
		t.Fatalf("expected empty store after delete-all")
	}
}

func TestPut_IDFuncError(t *testing.T) {
	dir := t.TempDir()

	s, err := Open[uint64, User](Options[uint64, User]{
		Dir: dir,
		IDFunc: func(User) (uint64, error) {
			return 0, errors.New("id error")
		},
	})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer s.Close()

	_, err = s.Put(User{Id: 1, Name: "Alice"})
	if err == nil {
		t.Fatalf("expected error from IDFunc")
	}

	users := s.Get(all[User])
	if len(users) != 0 {
		t.Fatalf("store modified despite IDFunc error")
	}
}

func TestPut_Delete_PutSameID(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "Alice"})
	s.Delete(func(u User) bool { return u.Id == 1 })
	s.Put(User{Id: 1, Name: "Alice v2"})

	users := s.Get(all[User])
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}

	if users[0].Name != "Alice v2" {
		t.Fatalf("unexpected value after reinsert")
	}
}

func TestDelete_TwiceIsNoOp(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "Alice"})
	s.Delete(func(u User) bool { return u.Id == 1 })

	deleted, err := s.Delete(func(u User) bool { return u.Id == 1 })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deleted) != 0 {
		t.Fatalf("expected no deleted users on second delete")
	}
}

func TestGet_ReturnedValueIsCopy(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	s.Put(User{Id: 1, Name: "Alice"})

	users := s.Get(all[User])
	users[0].Name = "Hacked"

	users2 := s.Get(all[User])
	if users2[0].Name != "Alice" {
		t.Fatalf("store value was mutated through Get")
	}
}

func TestPut_Concurrent(t *testing.T) {
	dir := t.TempDir()
	s := openUserStore(t, dir)
	defer s.Close()

	const n = 10
	done := make(chan struct{}, n)

	for i := 0; i < n; i++ {
		go func(i int) {
			s.Put(User{Id: uint64(i), Name: "U"})
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < n; i++ {
		<-done
	}

	users := s.Get(all[User])
	if len(users) != n {
		t.Fatalf("expected %d users, got %d", n, len(users))
	}
}

func TestGet_PreservesOrderAcrossMemoryAndDisk(t *testing.T) {
	dir := t.TempDir()

	minusOne := -1

	store, err := Open[uint64, testUser](Options[uint64, testUser]{
		Dir: dir,
		IDFunc: func(u testUser) (uint64, error) {
			return u.Id, nil
		},
		MaxInMemoryRecords: &minusOne,
		ResidencyFunc: func(u testUser) bool {
			// ímpares ficam em memória
			// pares vão para disco
			return u.Id%2 != 0
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Inserir 20 registros
	for i := 1; i <= 20; i++ {
		_, err := store.Put(testUser{
			Id:  uint64(i),
			Val: i,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	results := store.Get(func(u testUser) bool {
		return true
	})

	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}

	// Verificar ordem lógica absoluta
	for i, u := range results {
		expectedID := uint64(i + 1)
		if u.Id != expectedID {
			t.Fatalf(
				"order broken at position %d: expected %d, got %d",
				i,
				expectedID,
				u.Id,
			)
		}
	}
}
