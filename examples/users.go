package main

import (
	fleastore "flea"
	"fmt"
	"log"
	"time"
)

type User struct {
	Id    uint64
	Name  string
	Age   int
	Alive bool
}

func main() {
	// place
	dir := "./data"

	interval := time.Duration(10) * time.Second

	store, err := fleastore.Open[uint64, User](
		fleastore.Options[uint64, User]{
			Dir: dir, SnapshotInterval: interval,
			IDFunc: func(u User) (uint64, error) { return u.Id, nil },
		})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	//Check existing users
	fmt.Println("Checking existing users")
	users := store.Get(func(u User) bool { return true })

	for i, u := range users {
		fmt.Println(i, u.Name)
	}

	// PUTs
	store.Put(User{Id: 1, Name: "Alice", Age: 30, Alive: true})
	store.Put(User{Id: 2, Name: "Bob", Age: 42, Alive: true})
	store.Put(User{Id: 3, Name: "Charlie", Age: 50, Alive: false})

	// GET
	alive := store.Get(func(u User) bool {
		return u.Alive
	})

	fmt.Println("Alive users:")
	for _, u := range alive {
		fmt.Printf(" - %+v\n", u)
	}

	// DELETE
	deleted, err := store.Delete(func(u User) bool {
		return !u.Alive
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Deleted %d users\n", deleted)

	//Update Alice
	store.Put(User{Id: 1, Name: "Alice Older", Age: 31, Alive: true})

	// GET again
	all := store.Get(func(u User) bool {
		return true
	})

	fmt.Println("Remaining users:")
	for _, u := range all {
		fmt.Printf(" - %+v\n", u)
	}

	fmt.Println("Waiting for snapshot...")
	time.Sleep(interval + time.Second)

	fmt.Println("Checking existing users")
	users = store.Get(func(u User) bool { return true })

	for i, u := range users {
		fmt.Println(i, u.Name)
	}

	fmt.Println("Done. You can now kill the process and restart to test recovery.")
}
