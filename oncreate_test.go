package flea

import (
	"errors"
	"testing"
)

// type User struct {
// 	Id   uint64
// 	Name string
// 	Age  int
// }

func userOptions(dir string) Options[uint64, User] {
	return Options[uint64, User]{
		Dir: dir,
		IDFunc: func(u User) (uint64, error) {
			return u.Id, nil
		},
	}
}

func TestOpen_OnCreate(t *testing.T) {
	dir := t.TempDir()
	called := 0

	opts := userOptions(dir)
	opts.OnCreate = func(s *Store[uint64, User]) error {
		called++

		_, err := s.Put(User{
			Id:   1,
			Name: "Alice",
			Age:  30,
		})
		return err
	}

	store, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("OnCreate called %d times, want 1", called)
	}

	store.Close()

	// Reopening an existing store must not invoke OnCreate.
	store, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if called != 1 {
		t.Fatalf("OnCreate called %d times after reopen, want 1", called)
	}

	users := store.Get(func(u User) bool {
		return u.Id == 1
	})

	if len(users) != 1 {
		t.Fatalf("got %d users, want 1", len(users))
	}
	if users[0].Name != "Alice" {
		t.Errorf("Name = %q, want Alice", users[0].Name)
	}
}

func TestOpen_OnCreateNil(t *testing.T) {
	opts := userOptions(t.TempDir())

	store, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
}

func TestOpen_OnCreateError(t *testing.T) {
	opts := userOptions(t.TempDir())
	expected := errors.New("initialization failed")

	opts.OnCreate = func(s *Store[uint64, User]) error {
		return expected
	}

	store, err := Open(opts)
	if store != nil {
		store.Close()
		t.Fatal("expected nil store when initialization fails")
	}
	if !errors.Is(err, expected) {
		t.Fatalf("error = %v, want %v", err, expected)
	}
}