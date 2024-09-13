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
	"tools/internal/hash"
	"tools/internal/metadata"
	"tools/internal/mpi"
	"tools/utils"
)

const MAXGOROUTINE = 10000

// CHUNKSIZE CHUNK SIZE default 512k
var CHUNKSIZE uint64 = 512 * 1024
var flage = false

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

	comm, err := mpi.NewComm(nil)
	if err != nil {
		log.Fatalf("failed to create communicator: %v", err)
	}

	rank := comm.Rank()
	countRanks := comm.Size()
	if hostSize != countRanks {
		log.Fatalf("host_size is not equal to ranks")
	}
	//inputPath := os.Args[1]
	filename := filepath.Base(os.Args[2])
	//hostsFile := os.Args[3]
	gkfsDataPath := os.Args[4]

	//pid := GetDaemonPidByRank(hostsFile, rank)
	outputPath := "/"
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

	//oFile, err := os.Create(os.Args[2])
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer oFile.Close()
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
		log.Printf("Num of process: %d, file_size: %d, each process_size: %d\n", countRanks, iFileInfo.Size(), rankSize)
		log.Printf("Total chunks: %d\n", totalChunks)
		log.Printf("Chunk_id start: %d; chunk_id end: %d\n", chnkStart, chnkEnd)
		log.Printf("Output file: %v\n", os.Args[2])
		log.Printf("Write base dir: %s\n", writeBaseDir)

		hashVal := hash.GetHash(outputPath)
		fmt.Printf("Hash value of output file: %d\n", hashVal)
	}

	// Collect all chunk IDs that have the same target for a single rpc bulk transfer
	targetChnks := make(map[uint64][]uint64)
	// Target ID for accessing the target_chunks map
	var targets []uint64 = make([]uint64, 0, int(totalChunks)/countRanks)
	// The first and last chunk's targets need special processing
	var chnkEndTarget uint64
	Num := uint64(hostSize)
	for chnkId := chnkStart; chnkId <= chnkEnd; chnkId++ {
		target := hash.GetHash(outputPath+strconv.FormatUint(chnkId, 10)) % Num

		if _, exists := targetChnks[target]; !exists {
			targetChnks[target] = []uint64{chnkId}
			targets = append(targets, target)
		} else {
			targetChnks[target] = append(targetChnks[target], chnkId)
		}

		// Set the target for the first and last chunks
		//if chnkId == chnkStart {
		//	chnkStartTarget = target
		//}

		if chnkId == chnkEnd {
			//target host machine
			chnkEndTarget = target
		}
	}
	comm.Barrier()
	myAllDataSize := uint64(len(targetChnks[uint64(rank)])) * CHUNKSIZE
	var lastBuf []byte
	if chnkEndTarget == uint64(rank) {
		// I have the last chunk
		myAllDataSize = (uint64(len(targetChnks[uint64(rank)]))-1)*CHUNKSIZE + lastChunkSize

	}

	var readStart, writeStart, readEnd, writeEnd float64
	if myChunks, ok := targetChnks[uint64(rank)]; ok {
		readBuffer := make([]byte, myAllDataSize)
		wg := sync.WaitGroup{}
		sem := make(chan struct{}, MAXGOROUTINE)
		readStart = mpi.WTime()
		for index, chunkId := range myChunks {
			wg.Add(1)
			//TODO refactory ind func
			go func(i uint64, chunkId uint64) {
				defer wg.Done()
				//TODO limit maximum goroutine
				sem <- struct{}{}
				offset := int64(chunkId * CHUNKSIZE)
				boffset := i * CHUNKSIZE

				//TODO handle error
				//log.Println("[Debug offset :]", "rank: ", rank, "index:", i, "chunkId: ", chunkId, boffset, " ", offset)
				if chunkId != chnkEnd {
					if readNum, err := ReadChunk(iFile, readBuffer[boffset:boffset+CHUNKSIZE], CHUNKSIZE, offset); err != nil {
						log.Println("read :", readNum, err)
					}
					//log.Println("[Debug1]: ", "rank :", rank, "chnk: ", chnkEnd)

				} else {

					if readNum, err := ReadChunk(iFile, readBuffer[boffset:boffset+lastChunkSize], lastChunkSize, offset); err != nil {
						log.Print("read :", readNum, err)
					}
					lastBuf = make([]byte, lastChunkSize)
					copy(lastBuf, readBuffer[boffset:boffset+lastChunkSize])
					//log.Println("[Debug1]: ", string(readBuffer[boffset:boffset+lastChunkSize]), "rank :", rank, "chnk: ", chnkEnd, "last buf :", lastBuf)
				}
				//if rank == 0 {
				//	log.Println("read size", rSize)
				//}
				<-sem
			}(uint64(index), chunkId)
		}
		wg.Wait()
		readEnd = mpi.WTime()
		//write file
		if uint64(rank) != chnkEndTarget {
			wg := sync.WaitGroup{}
			sem := make(chan struct{}, MAXGOROUTINE)
			writeStart = mpi.WTime()
			for index, chunkId := range myChunks {
				wg.Add(1)
				go func(index uint64, chunkId uint64) {
					defer wg.Done()
					sem <- struct{}{}
					filename := writeBaseDir + strconv.FormatUint(chunkId, 10)

					file, err := os.Create(filename)
					if err != nil {
						//TODO gracefully handle error
						log.Println(err)
						return
					}
					defer file.Close()
					//offset := int64(chunkId * CHUNKSIZE)
					boffset := index * CHUNKSIZE

					//log.Println("[Debug1]: ", "rank :", rank, "boffset: ", boffset)
					// TODO handle error
					if writeNum, err := WriteChunk(file, readBuffer[boffset:boffset+CHUNKSIZE], CHUNKSIZE, 0, filename); err != nil {
						log.Println("write Num: ", writeNum, err)
					}
					<-sem
				}(uint64(index), chunkId)
			}
			wg.Wait()
			writeEnd = mpi.WTime()
		} else {
			lastChunkId := myChunks[len(myChunks)-1]
			myChunks = myChunks[:len(myChunks)-1]
			//chunksCount := uint64(len(myChunks) - 1)
			wg := sync.WaitGroup{}
			writeStart = mpi.WTime()
			for index, chunkId := range myChunks {
				wg.Add(1)
				go func(index uint64, chunkId uint64) {
					defer wg.Done()
					filename := writeBaseDir + strconv.FormatUint(chunkId, 10)
					file, err := os.Create(filename)
					if err != nil {
						//TODO gracefully handle error
						log.Println(err)
						return
					}
					defer file.Close()
					//offset := int64(chunkId * CHUNKSIZE)
					boffset := index * CHUNKSIZE
					// TODO handle error
					//log.Println("rank:", rank, "filename: ", filename, "boffset :", boffset)
					if writeNum, err := WriteChunk(file, readBuffer[boffset:boffset+CHUNKSIZE], CHUNKSIZE, 0, filename); err != nil {
						log.Println("Write Num: ", writeNum, err)
					}
				}(uint64(index), chunkId)
			}
			wg.Wait()
			writeEnd = mpi.WTime()
			//write metadata  in GekkoFS. write operation can be intercepted
			//offset := (totalChunks - 1) * CHUNKSIZE
			//_, err := oFile.WriteAt(readBuffer[chunksCount*CHUNKSIZE:chunksCount*CHUNKSIZE+lastChunkSize], int64(offset))
			//if err != nil {
			//	log.Fatalf("write metadat error %v", err)
			//}
			offset := (totalChunks - 1) * CHUNKSIZE
			//write last file chunk and file metadata in GekkoFS through syscall_intercept
			//fmt.Println("[Debug ReadBuffer]", len(lastBuf), " ", string(lastBuf))
			if err := metadata.WriteMetaData(os.Args[2], lastBuf, offset); err != nil {
				log.Println(err)
			}
			fmt.Printf("write filename %v\n", os.Args[2])
			fmt.Println("i am", rank)
			fmt.Println("last_chunk_id:", lastChunkId)
		}

	}
	comm.Barrier()
	fmt.Printf("myRank = %v hostname: %v, read_time = %.6f write_time = %.6f\n", rank, host, readEnd-readStart, writeEnd-writeStart)
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
		//log.Println("filename: ", fh.Name(), "wroteTotal:", wroteTotal, "offset :", offset, "size: ", size)
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
				fh.Name(), size, offset, err.Error())
			return readTotal, errors.New(errMsg)
		}
		uread := uint64(read)
		// Debug output for less-than-requested reads
		if readTotal+uread < size {
			fmt.Printf("Read less bytes than requested: '%d'/%d. Total read was '%d'. This is not an error!\n", read, size-readTotal, size)
			log.Println(string(buf[readTotal:]))
		}

		readTotal += uread
	}

	return readTotal, nil
}
