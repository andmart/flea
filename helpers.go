package fleastore

import (
	"fmt"
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

func (s *Store[ID, T]) getPath(suffix string) string {
	return filepath.Join(s.dir, s.getName(suffix))
}

func (s *Store[ID, T]) getName(suffix string) string {
	var zero T
	cls := sanitizeTypeName(reflect.TypeOf(zero).String())
	return fmt.Sprintf("%s-%s", cls, suffix)
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
