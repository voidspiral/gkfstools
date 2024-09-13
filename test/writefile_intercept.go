package main

import (
	"log"
	"os"
)

func main() {
	//export LD_PRELOAD=xxx/lib/libgkfs_intercept.so
	//mount /dev/shm/gkfs. write file in mount FS path
	//f, err := os.Create("/dev/shm/gkfs/4k")
	f, err := os.OpenFile("/dev/shm/gkfs/4k", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 1024)
	for i := range buf {
		buf[i] = '9'
	}
	//TODO syscall_intercept can not correctly work in GO. Write operation can't be intercepted by sycall_intercept lib.
	n, err := f.Write(buf)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("write byte %v\n", n)
	return
}
