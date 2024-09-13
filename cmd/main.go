package main

import "tools/internal/metadata"

func main() {
	metadata.WriteMetaData("/dev/shm/gkfs/4k", make([]byte, 4096), 0)
}
