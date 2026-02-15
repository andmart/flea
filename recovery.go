package fleastore

import (
	"bufio"
	"encoding/json"
	"os"
)

func (s *Store[ID, T]) replayWAL() error {
	path := s.getWalPath()
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var op walOp[ID, T]
		if err := json.Unmarshal(sc.Bytes(), &op); err != nil {
			return err
		}
		switch op.Op {
		case opPut:
			s.addOrUpdate(op.ID, &op.Value)
		case opDelete:
			s.deleteByID(op.ID)
		}
	}
	truncate(f)
	s.handleResidency()
	return nil
}

func truncate(f *os.File) error {
	if err := f.Truncate(0); err != nil {
		return err
	}
	_, err := f.Seek(0, 0)
	return err
}

func (s *Store[ID, T]) deleteByID(id ID) {
	rec, ok := s.index[id]
	if !ok {
		return
	}

	rec.deleted = true
	delete(s.index, id)

	s.dirty = true
}
