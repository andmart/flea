package fleastore

import (
	"errors"
	"time"
)

var LOW int = -1

type Options[ID comparable, T any] struct {
	// Path to local where store will be created.
	Dir string

	// Time interval for snapshot creation
	SnapshotInterval time.Duration
	IDFunc           IDFunc[ID, T]
	Checkers         []Checker[T]
	// Experimental: controls which records remain resident in memory
	ResidencyFunc      func(T) bool
	MaxInMemoryRecords *int
}

func (o *Options[ID, T]) Validate() error {
	// Dir default: current directory
	if o.Dir == "" {
		o.Dir = "."
	}

	// SnapshotInterval default: 30s
	if o.SnapshotInterval == 0 {
		o.SnapshotInterval = 30 * time.Second
	}

	if o.Checkers == nil {
		o.Checkers = []Checker[T]{}
	}

	if o.MaxInMemoryRecords == nil {
		o.MaxInMemoryRecords = &LOW
	}

	if o.IDFunc == nil {
		return errors.New("IDFunc must be provided")
	}

	return nil
}
