package main

import (
	"log"
	"os"
	"testing"
	"tools/internal/hash"
)

func TestGetDaemonPidByRank(t *testing.T) {
	type args struct {
		hostFile string
		line     int
	}
	tests := []struct {
		name    string
		args    args
		wantPid string
	}{
		// TODO: Add test cases.
		{name: "testfile", args: args{
			hostFile: "gkfs_host.txt.pid",
			line:     2,
		}, wantPid: "2163292"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotPid := GetDaemonPidByRank(tt.args.hostFile, tt.args.line); gotPid != tt.wantPid {
				t.Errorf("GetDaemonPidByRank() = %v, want %v", gotPid, tt.wantPid)
			}
		})
	}
}

// use of cgo in test main_test.go not supported
func Test_chnkCountForOffset(t *testing.T) {
	filename := "../test/1G"
	f, err := os.Open(filename)
	if err != nil {
		panic("open file error")
	}
	file, _ := f.Stat()
	size := file.Size()
	countSize := int64(4)
	rankSize := size / countSize
	blockSize := uint64(512 * 1024)
	chnkStart := blockIndex(0, blockSize)
	chnkEnd := blockIndex(0+uint64(size)-1, blockSize)
	totalChunks := blockCount(0, uint64(size), blockSize)
	log.Println(chnkStart, chnkEnd, totalChunks)
	hashValue := hash.GetHash(filename)
	log.Println(rankSize, hashValue)
	filename = "/1G"
	hashValue = hash.GetHash(filename)
	log.Println(filename)
}
