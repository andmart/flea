package fleastore

import (
	"encoding/json"
	"os"
)

func (s *Store[ID, T]) appendOffline(v T) error {
	f, err := os.OpenFile(s.getDataPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	return enc.Encode(v)
}

func (s *Store[ID, T]) handleResidency() error {
	if s.residencyFn == nil {
		return nil
	}

	if s.maxOnline >= 0 && len(s.index) <= s.maxOnline {
		return nil
	}

	ids := make([]ID, 0, len(s.index))
	for id := range s.index {
		ids = append(ids, id)
	}

	for _, id := range ids {

		v, ok := s.index[id]
		if !ok {
			continue
		}

		obj := s.records[v].value

		//already offline
		if obj == nil {
			continue
		}

		if s.residencyFn(*obj) {
			continue
		}

		// Move para offline
		if err := s.appendOffline(*obj); err != nil {
			return err
		}

		s.records[v].value = nil
		s.onlineCount--

		// Se há limite explícito, parar quando normalizar
		if s.maxOnline >= 0 && s.onlineCount <= s.maxOnline {
			break
		}
	}

	return nil
}
