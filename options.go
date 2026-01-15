package fleastore

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"time"
)

type Options[ID comparable, T any] struct {
	// Path to local where store will be created.
	Dir string

	// Time interval for snapshot creation
	SnapshotInterval time.Duration
	IDFunc           IDFunc[ID, T]
	Checkers         []Checker[T]
}

func DefaultIDFunc[ID uint64, T any](v T) (uint64, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}

	h := fnv.New64a()
	_, _ = h.Write(data)
	return h.Sum64(), nil
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

	if o.IDFunc == nil {
		return errors.New("IDFunc must be provided")
	}

	return nil
}
