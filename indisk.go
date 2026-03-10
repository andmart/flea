package flea

import (
	"encoding/json"
	"io"
	"os"
)

type dataWindow struct {
	buf        []byte
	baseOffset int64
}

func (w *dataWindow) read(file *os.File, offset, size int64) ([]byte, error) {

	if offset >= w.baseOffset && offset+size <= w.baseOffset+int64(len(w.buf)) {
		start := offset - w.baseOffset
		return w.buf[start : start+size], nil
	}

	if len(w.buf) == 0 {
		w.buf = make([]byte, max(4096, int(size)*10))
	}
	n, err := file.ReadAt(w.buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	w.baseOffset = offset
	w.buf = w.buf[:n]

	start := offset - w.baseOffset
	return w.buf[start : start+size], nil
}

func (s *Store[ID, T]) loadFromDisk(offset, size int64) (T, error) {
	var zero T

	var v T

	data, err := s.dataWindow.read(s.dataFile, offset, size)
	if err != nil {
		return zero, err
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return zero, err
	}
	return v, nil
}

func (s *Store[ID, T]) appendToDisk(batch []*record[T]) error {

	offset, err := s.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	for _, rec := range batch {
		if err != nil {
			return err
		}

		b, err := json.Marshal(*rec.value)
		if err != nil {
			return err
		}

		if _, err := s.dataFile.Write(b); err != nil {
			return err
		}

		rec.offset = offset
		rec.size = int64(len(b))
		offset += rec.size
		rec.value = nil
	}

	return nil
}

func (s *Store[ID, T]) handleResidency() error {
	if s.residencyFn == nil {
		return nil
	}

	if s.maxInMemory >= 0 && len(s.index) <= s.maxInMemory {
		return nil
	}

	offline := make([]*record[T], 0, 1024)

	ids := make([]ID, 0, len(s.index))
	for id := range s.index {
		ids = append(ids, id)
	}

	for _, id := range ids {

		rec, ok := s.index[id]
		if !ok {
			continue
		}

		obj := rec.value

		if obj == nil {
			continue
		}

		if s.residencyFn(*obj) {
			continue
		}

		offline = append(offline, rec)
		//rec.value = nil
		s.onlineCount--

		if s.maxInMemory >= 0 && s.onlineCount <= s.maxInMemory {
			break
		}

		if len(offline) == 0 {
			return nil
		}

	}
	return s.appendToDisk(offline)
}
