package fleastore

import (
	"encoding/json"
	"os"
)

func (s *Store[ID, T]) appendOffline(batch []T) error {
	f, err := os.OpenFile(s.getDataPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 0, 32*1024)

	for _, v := range batch {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		b = append(b, '\n')

		// bigger than buffer. So flush whatever is in the buffer and write directly the big record.
		if len(b) > cap(buf) {
			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
			if _, err := f.Write(b); err != nil {
				return err
			}
			continue
		}

		// no room for the record. flush and add.
		if len(buf)+len(b) > cap(buf) {
			if _, err := f.Write(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
		buf = append(buf, b...)
	}

	// flush
	if len(buf) > 0 {
		_, err = f.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store[ID, T]) handleResidency() error {
	if s.residencyFn == nil {
		return nil
	}

	if s.maxOnline >= 0 && len(s.index) <= s.maxOnline {
		return nil
	}

	offline := make([]T, 0, 1024)

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

		//already offline
		if obj == nil {
			continue
		}

		if s.residencyFn(*obj) {
			continue
		}

		// marcar para offline
		offline = append(offline, *rec.value)
		rec.value = nil
		s.onlineCount--

		// Se há limite explícito, parar quando normalizar
		if s.maxOnline >= 0 && s.onlineCount <= s.maxOnline {
			break
		}

		if len(offline) == 0 {
			return nil
		}

	}
	return s.appendOffline(offline)
}
