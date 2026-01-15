package fleastore

import (
	"bufio"
	"encoding/json"
	"os"
)

type walOpType string

const (
	opPut    walOpType = "put"
	opDelete walOpType = "delete"
)

type walOp[ID comparable, T any] struct {
	Op    walOpType `json:"op"`
	ID    ID        `json:"Id"`
	Value T         `json:"Value,omitempty"`
}

type wal[ID comparable, T any] struct {
	file *os.File
	w    *bufio.Writer
}

func openWAL[ID comparable, T any](path string) (*wal[ID, T], error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &wal[ID, T]{
		file: f,
		w:    bufio.NewWriter(f),
	}, nil
}

func (w *wal[ID, T]) append(ops []walOp[ID, T]) error {
	enc := json.NewEncoder(w.w)
	for _, op := range ops {
		if err := enc.Encode(op); err != nil {
			return err
		}
		if err := w.w.Flush(); err != nil {
			return err
		}
	}
	return w.file.Sync()
}

func (w *wal[ID, T]) close() error {
	return w.file.Close()
}
