package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"tools/internal/mpi"
)

/*
#cgo CXXFLAGS: -std=c++14
#cgo LDFLAGS: -L. -lhash -lstdc++
extern unsigned long Cal_Hash(const char* val);
*/
import "C"

func main() {
	//if len(os.Args) < 5 {
	//	log.Fatalf("args: source_file target_file /path-to/gkfs_hosts.txt.pid /gkfs-data-dir/ ")
	//}
	var hostSize int
	if env := os.Getenv("HOST_SIZE"); env != "" {
		var err error
		hostSize, err = strconv.Atoi(env)
		if err != nil {
			log.Fatalf("HOST_SIZE is not digit")
		}
	}
	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("Get hostname error")
	}

	mpi.Init()
	defer mpi.Finalize()

	start := time.Now().UnixMilli()
	comm, err := mpi.NewComm(nil)
	if err != nil {
		log.Fatalf("failed to create communicator: %v", err)
	}

	rank := comm.Rank()
	size := comm.Size()
	//if hostSize != size {
	//	log.Fatalf("host_size is not equal to ranks")
	//}
	inputPath := os.Args[1]
	filename := filepath.Base(os.Args[2])
	hostsFile := os.Args[3]
	gkfsDataPath := os.Args[4]

	pid := GetDaemonPidByRank(hostsFile, rank)
	var outputPath string = "/"
	outputPath += filename
	writeBaseDir := gkfsDataPath + "/chunks/" + filename + "/"

	if err := os.Mkdir(writeBaseDir, 0777); err == nil {
		fmt.Println("mkdir succeed")
	} else {
		if os.IsExist(err) {
			fmt.Println("dir exist")
		} else {
			fmt.Printf("Rank：%d base_dir: %s %s mkdir error\n", rank, writeBaseDir, err)
		}
	}
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	log.Println(inputPath, filename, gkfsDataPath, pid, host, hostSize)
	log.Printf("Process rank: %d of %d", rank, size)
	cur := time.Now().UnixMilli()
	log.Println("time: ", cur-start)
}

// chnkLalign aligns the offset to the nearest lower multiple of chnkSize.
func chnkLalign(offset int64, chnkSize uint64) int64 {
	return offset & ^(int64(chnkSize) - 1)
}

// chnkCountForOffset computes the chunk count for a given offset and count.
func chnkCountForOffset(offset int64, count uint64, chnkSize uint64) uint64 {
	chnkStart := chnkLalign(offset, chnkSize)
	chnkEnd := chnkLalign(offset+int64(count)-1, chnkSize)

	return uint64((chnkEnd >> uint64(math.Log2(float64(chnkSize)))) -
		(chnkStart >> uint64(math.Log2(float64(chnkSize)))) + 1)
}

func GetDaemonPidByRank(hostFile string, line int) (pid string) {
	file, err := os.Open(hostFile)
	if err != nil {
		log.Fatalf("open host file err : %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	current := 0
	for scanner.Scan() {
		text := scanner.Text()
		if current == line {
			parts := strings.Split(text, ":")
			if len(parts) > 2 {
				pid = parts[len(parts)-1] // 获取最后一个部分
			}
			return pid
		}
		current++
	}
	return pid
}
