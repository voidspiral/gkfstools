package hash

/*
#cgo CXXFLAGS: -std=c++14
#cgo LDFLAGS: -L. -lhash -lstdc++
extern unsigned long Cal_Hash(const char* val);
*/
import "C"

func GetHash(str string) uint64 {
	return uint64(C.Cal_Hash(C.CString(str)))
}
