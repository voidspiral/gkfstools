package main

import (
	"os"
	"runtime/pprof"
)

//import (
//	_ "net/http/pprof"
//	"os"
//	"runtime/pprof"
//)

//func main() {
//	go func() {
//		log.Println(http.ListenAndServe(":13456", nil))
//	}()
//	select {}
//}

func main() {
	// 创建 CPU 性能分析文件
	cpuFile, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	defer cpuFile.Close()

	// 开始 CPU 性能分析
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	// 这里是你的程序逻辑
	// ...

	// 创建堆分析文件
	heapFile, err := os.Create("heap.prof")
	if err != nil {
		panic(err)
	}
	defer heapFile.Close()

	// 写入堆分析数据
	if err := pprof.WriteHeapProfile(heapFile); err != nil {
		panic(err)
	}
}
