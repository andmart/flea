package fleastore

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func (s *Store[ID, T]) getSnapshotPath() string {
	return s.getPath("snapshot.ndjson")
}

func (s *Store[ID, T]) getWalPath() string {
	return s.getPath("wal.log")
}

func (s *Store[ID, T]) getDataPath() string {
	return s.getPath("data.ndjson")
}

func (s *Store[ID, T]) getPath(file string) string {
	modelDir := filepath.Join(s.dir, s.getModelName())
	return filepath.Join(modelDir, file)
}

func (s *Store[ID, T]) getModelName() string {
	var zero T
	cls := sanitizeTypeName(reflect.TypeOf(zero).String())
	return cls
}

func (s *Store[ID, T]) makeDirs() {
	path := s.getPath("")
	os.MkdirAll(path, os.ModePerm)
}

func sanitizeTypeName(name string) string {
	replacer := strings.NewReplacer(
		".", "_",
		"/", "_",
		"*", "",
		"[", "",
		"]", "",
	)
	return strings.ToLower(replacer.Replace(name))
}

func (s *Store[ID, T]) handleDataFile(f func(T) bool) error {

	if f != nil {
		dataPath := s.getDataPath()

		f, err := os.OpenFile(
			dataPath,
			os.O_CREATE|os.O_RDWR,
			0644,
		)
		if err != nil {
			return err
		}

		_ = f.Close()
		s.hasOfflineData = true
	}
	return nil
}
