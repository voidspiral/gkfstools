package metadata

import (
	"testing"
)

func TestWriteToFile(t *testing.T) {
	buf := make([]byte, 4096)
	word := []byte("hello world")
	copy(buf, word)
	WriteMetaData("/dev/shm/gkfs/4k", buf, 0)
}
