package fleastore

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
)

func (s *Store[ID, T]) loadFromDisk(offset int64) (T, error) {
	var zero T

	f, err := os.Open(s.getDataPath())
	if err != nil {
		return zero, err
	}
	defer f.Close()

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return zero, err
	}

	reader := bufio.NewReader(f)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return zero, err
	}

	var v T
	if err := json.Unmarshal(line, &v); err != nil {
		return zero, err
	}

	return v, nil
}

func (s *Store[ID, T]) appendOffline(batch []*record[T]) error {
	f, err := os.OpenFile(s.getDataPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	offset, err := f.Seek(0, io.SeekEnd)
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

		b = append(b, '\n')

		if _, err := f.Write(b); err != nil {
			return err
		}

		rec.offset = offset
		offset += int64(len(b))
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
	return s.appendOffline(offline)
}
