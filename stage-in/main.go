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
	"tools/internal/hash"
	"tools/internal/mpi"
)

var CHUNKSIZE uint = 512 * 1024

func Debug(rank int) {

}

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
	countRanks := comm.Size()
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
		log.Println("mkdir succeed")
	} else {
		if os.IsExist(err) {
			log.Println("dir exist")
		} else {
			log.Printf("Rank：%d base_dir: %s %s mkdir error\n", rank, writeBaseDir, err)
		}
	}

	iFile, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer iFile.Close()
	iFileInfo, err := iFile.Stat()
	if err != nil {
		log.Fatal(err)
	}

	oFile, err := os.Create(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer oFile.Close()
	rankSize := int(iFileInfo.Size()) / countRanks
	bs := uint64(CHUNKSIZE)
	chnkStart := blockIndex(0, bs)
	chnkEnd := blockIndex(0+uint64(iFileInfo.Size())-1, bs)
	totalChunks := blockCount(0, uint64(iFileInfo.Size()), bs)
	lastChunkSize := uint64(iFileInfo.Size()) - (totalChunks-1)*bs

	// the file only has one chunk
	if totalChunks == 1 {
		lastChunkSize = uint64(iFileInfo.Size())
	}
	if rank == 0 {
		fmt.Printf("num of process: %d, file_size: %d, each process_size: %d\n", countRanks, iFileInfo.Size(), rankSize)
		fmt.Printf("total chunks: %d\n", totalChunks)
		fmt.Printf("chunk_id start: %d; chunk_id end: %d\n", chnkStart, chnkEnd)
		fmt.Printf("output file: %v\n", oFile)
		fmt.Printf("write base dir: %s\n", writeBaseDir)

		hash := hash.GetHash(outputPath)
		fmt.Printf("Hash value of output file: %d\n", hash)
	}

	// Collect all chunk IDs that have the same target for a single rpc bulk transfer
	targetChnks := make(map[uint64][]uint64)
	// Target ID for accessing the target_chunks map
	var targets []uint64

	// The first and last chunk's targets need special processing
	var chnkStartTarget uint64
	var chnkEndTarget uint64

	for chnkId := chnkStart; chnkId <= chnkEnd; chnkId++ {
		target := hash.GetHash(outputPath+strconv.FormatUint(chnkId, 10)) % uint64(hostSize)

		if _, exists := targetChnks[target]; !exists {
			targetChnks[target] = []uint64{chnkId}
			targets = append(targets, target)
		} else {
			targetChnks[target] = append(targetChnks[target], chnkId)
		}
		cur := uint64(chnkId)
		// Set the target for the first and last chunks
		if cur == chnkStart {
			chnkStartTarget = target
		}

		if cur == chnkEnd {
			chnkEndTarget = target
		}
	}

	myAllDataSize := uint(len(targetChnks[uint(rank)])) * CHUNKSIZE
	if chnkEndTarget == uint(rank) {
		// I have the last chunk
		myAllDataSize = (uint(len(targetChnks[uint(rank)]))-1)*CHUNKSIZE + lastChunkSize
	}

	// Other logic can continue or be handled here
	log.Println(inputPath, filename, gkfsDataPath, pid, host, hostSize)
	log.Printf("Process rank: %d of %d", rank, size)
	cur := time.Now().UnixMilli()
	log.Println("time: ", cur-start)
}

// isPowerOf2 checks if a number is a power of 2.
func isPowerOf2(x uint64) bool {
	return x > 0 && (x&(x-1)) == 0
}

// log2 calculates the base 2 logarithm for power of 2 numbers.
func log2(x uint64) uint64 {
	result := uint64(0)
	for x > 1 {
		x >>= 1
		result++
	}
	return result
}

// alignLeft aligns the offset to the left according to the block size.
func alignLeft(offset uint64, bs uint64) uint64 {
	if !isPowerOf2(bs) {
		panic("bs must be a power of 2")
	}
	return offset &^ (bs - 1)
}

// isAligned checks if a number is aligned to a given block size.
func isAligned(offset uint64, bs uint64) bool {
	return (offset & (bs - 1)) == 0
}

// blockIndex returns the index of the block that contains the given offset.
func blockIndex(offset uint64, bs uint64) uint64 {
	if !isPowerOf2(bs) {
		log.Fatal("bs must be a power of 2")
	}
	return alignLeft(offset, bs) >> log2(bs)
}

// blockCount returns the number of blocks that the range [offset, offset + size) spans.
func blockCount(offset uint64, size uint64, bs uint64) uint64 {
	if !isPowerOf2(bs) {
		log.Fatal("bs must be a power of 2")
	}

	// Check for overflow (simple version, you may want to handle more rigorously in production)
	if size > 0 && offset+size < offset {
		log.Fatal("overflow detected in offset + size")
	}

	firstBlock := alignLeft(offset, bs)
	finalBlock := alignLeft(offset+size, bs)

	mask := uint64(0)
	if size > 0 {
		mask = ^uint64(0)
	}

	return ((finalBlock >> log2(bs)) -
		(firstBlock >> log2(bs)) +
		boolToUint64(!isAligned(offset+size, bs))) &
		mask
}

// boolToUint64 converts a boolean to uint64 (true -> 1, false -> 0).
func boolToUint64(condition bool) uint64 {
	if condition {
		return 1
	}
	return 0
} // chnkLalign aligns the offset to the nearest lower multiple of chnkSize.
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
