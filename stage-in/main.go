package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"tools/internal/hash"
	"tools/internal/mpi"
	"tools/utils"
)

const MAXGOROUTINE = 10000

// CHUNK SIZE default 512k
var CHUNKSIZE uint64 = 512 * 1024

func Debug(rank int) {

}

func main() {
	if len(os.Args) < 5 {
		log.Fatalf("args: source_file target_file /path-to/gkfs_hosts.txt.pid /gkfs-data-dir/ ")
	}
	//go func() {
	//	log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
	//}()
	log.SetOutput(os.Stdout)
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

	//start := time.Now().UnixMilli()
	comm, err := mpi.NewComm(nil)
	if err != nil {
		log.Fatalf("failed to create communicator: %v", err)
	}

	rank := comm.Rank()
	countRanks := comm.Size()
	log.Println(hostSize)
	if hostSize != countRanks {
		log.Fatalf("host_size is not equal to ranks")
	}
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

	chnkStart := utils.BlockIndex(0, CHUNKSIZE)
	chnkEnd := utils.BlockIndex(0+uint64(iFileInfo.Size())-1, CHUNKSIZE)
	totalChunks := utils.BlockCount(0, uint64(iFileInfo.Size()), CHUNKSIZE)
	lastChunkSize := uint64(iFileInfo.Size()) - (totalChunks-1)*CHUNKSIZE

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

		// Set the target for the first and last chunks
		if chnkId == chnkStart {
			chnkStartTarget = target
		}

		if chnkId == chnkEnd {
			chnkEndTarget = target
		}
	}
	comm.Barrier()
	myAllDataSize := uint64(len(targetChnks[uint64(rank)])) * CHUNKSIZE
	if chnkEndTarget == uint64(rank) {
		// I have the last chunk
		myAllDataSize = (uint64(len(targetChnks[uint64(rank)]))-1)*CHUNKSIZE + lastChunkSize
	}
	if myChunks, ok := targetChnks[uint64(rank)]; ok {
		readBuffer := make([]byte, myAllDataSize)
		wg := sync.WaitGroup{}
		readTime := time.Now()
		for index, chunkId := range myChunks {

			wg.Add(1)
			//TODO refactory ind func
			go func(i uint64, chunkId uint64) {
				defer wg.Done()
				//TODO limit maximum goroutine

				offset := int64(chunkId * CHUNKSIZE)
				boffset := i * CHUNKSIZE
				//TODO handle error
				if chunkId != chnkEndTarget {
					ReadChunk(iFile, readBuffer[boffset:], CHUNKSIZE, offset)
				} else {
					ReadChunk(iFile, readBuffer[boffset:], lastChunkSize, offset)
				}
				//if rank == 0 {
				//	log.Println("read size", rSize)
				//}
			}(uint64(index), chunkId)
		}
		wg.Wait()
		endTime := time.Since(readTime)
		log.Printf("read time %.3f", endTime.Seconds())
		//write file
		if uint64(rank) != chnkEndTarget {
			wg := sync.WaitGroup{}
			writeTime := time.Now()
			for index, chunkId := range myChunks {
				wg.Add(1)
				go func(index uint64, chunkId uint64) {
					defer wg.Done()
					filename := writeBaseDir + strconv.FormatUint(chunkId, 10)

					file, err := os.Create(filename)
					if err != nil {
						//TODO graceful handle error
						log.Println(err)
						return
					}
					defer file.Close()
					offset := int64(chunkId * CHUNKSIZE)
					boffset := int64(index * CHUNKSIZE)
					log.Println("rank:", rank, "filename: ", filename, "offset :", offset)
					// TODO handle error
					WriteChunk(file, readBuffer[boffset:], CHUNKSIZE, 0, filename)
				}(uint64(index), chunkId)
			}
			wg.Wait()
			log.Printf("write time :%.3f", time.Since(writeTime).Seconds())
		} else {
			lastChunkId := myChunks[len(myChunks)-1]
			myChunks = myChunks[:len(myChunks)-1]
			chunksCount := uint64(len(myChunks) - 1)
			wg := sync.WaitGroup{}
			writeTime := time.Now()
			for index, chunkId := range myChunks {
				wg.Add(1)
				go func(index uint64, chunkId uint64) {
					defer wg.Done()
					filename := writeBaseDir + strconv.FormatUint(chunkId, 10)
					file, err := os.Create(filename)
					if err != nil {
						//TODO graceful handle error
						log.Println(err)
						return
					}
					defer file.Close()
					offset := int64(chunkId * CHUNKSIZE)
					boffset := int64(index * CHUNKSIZE)
					// TODO handle error
					log.Println("rank:", rank, "filename: ", filename, "offset :", offset)
					WriteChunk(file, readBuffer[boffset:], CHUNKSIZE, 0, filename)
				}(uint64(index), chunkId)
			}
			wg.Wait()
			log.Printf("write time :%.3f", time.Since(writeTime).Seconds())
			//write metadata  in GekkoFS. write operation can be intercepted
			//offset := (totalChunks - 1) * CHUNKSIZE
			//_, err := oFile.WriteAt(readBuffer[chunksCount*CHUNKSIZE:chunksCount*CHUNKSIZE+lastChunkSize], int64(offset))
			//if err != nil {
			//	log.Fatalf("write metadat error %v", err)
			//}
			offset := (totalChunks - 1) * CHUNKSIZE
			oFile.Seek(int64(offset), os.SEEK_SET)
			n, err := oFile.Write(readBuffer[chunksCount*CHUNKSIZE : chunksCount*CHUNKSIZE+lastChunkSize])
			if err != nil {
				log.Println(err)
			}
			log.Printf("write num  %v\n", n)
			fmt.Println("i am", rank)
			fmt.Println("last_chunk_id:", lastChunkId)
		}

	}
	// Other logic can continue or be handled here
	log.Println(inputPath, filename, gkfsDataPath, pid, host, hostSize, myAllDataSize, chnkStart, chnkStartTarget)
	log.Printf("Process rank: %d of %d", rank, rankSize)

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
				pid = parts[len(parts)-1]
			}
			return pid
		}
		current++
	}
	return pid
}

// WriteChunk writes a chunk to a file and handles any interruptions.
func WriteChunk(fh *os.File, buf []byte, size uint64, offset int64, chunkPath string) (uint64, error) {
	var wroteTotal uint64

	for wroteTotal < size {
		wrote, err := fh.WriteAt(buf[wroteTotal:], offset+int64(wroteTotal))
		if err != nil {
			// Check for recoverable errors
			if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				continue // Retry operation
			}

			// Formatting error message
			errMsg := fmt.Sprintf("%s() Failed to write chunk file. File: '%s', size: '%d', offset: '%d', Error: '%s'",
				"WriteChunk", chunkPath, size, offset, err.Error())
			return wroteTotal, errors.New(errMsg)
		}
		wroteTotal += uint64(wrote)
	}

	return wroteTotal, nil
}

// ReadChunk reads data from a file.
func ReadChunk(fh *os.File, buf []byte, size uint64, offset int64) (uint64, error) {
	var readTotal uint64

	for readTotal < size {
		read, err := fh.ReadAt(buf[readTotal:], offset+int64(readTotal))
		if read == 0 {
			// End-of-file
			break
		}
		if err != nil {
			// Check for recoverable errors
			if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				continue // Retry operation
			}

			// Formatting error message
			errMsg := fmt.Sprintf("Failed to read chunk file. File: '%s', size: '%d', offset: '%d', Error: '%s'",
				size, offset, err.Error())
			return readTotal, errors.New(errMsg)
		}
		uread := uint64(read)
		// Debug output for less-than-requested reads
		if readTotal+uread < size {
			fmt.Printf("Read less bytes than requested: '%d'/%d. Total read was '%d'. This is not an error!\n", read, size-readTotal, size)
		}

		readTotal += uread
	}

	return readTotal, nil
}
