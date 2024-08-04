package main

import (
	"log"
	"tools/internal/mpi"
)

func main() {
	// 初始化 MPI
	mpi.Init()
	defer mpi.Finalize() // 确保在最后调用 Finalize()

	// 创建一个新的通信器
	start := mpi.WTime()
	comm, err := mpi.NewComm(nil) // 如果需要指定 ranks，可以传递对应的切片
	if err != nil {
		log.Fatalf("failed to create communicator: %v", err)
	}

	// 获取当前进程的 rank 和 size
	rank := comm.Rank()
	size := comm.Size()

	log.Printf("Process rank: %d of %d", rank, size)
	log.Println("time:", start)
	// 在这里添加其他 MPI 操作，例如 Barrier、Abort 等
}
