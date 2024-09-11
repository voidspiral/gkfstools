package metadata

/*
#include <stdio.h>
#include <stdlib.h>
int writeToFile(const char *filePath, const char *data, size_t dataSize, size_t offset) {
    FILE *output = fopen(filePath, "w");

    if (output == NULL) {
        perror("open file error");
        return 1;
    }
    fseek(output, offset, SEEK_SET);
    fwrite(data, sizeof(char), dataSize, output);
	fclose(output);
    return 0;
}
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func WriteMetaData(filePath string, data []byte, offset uint64) error {
	cFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFilePath))
	//log.Println("Original data:", string(data))
	cData := C.CBytes(data)
	defer C.free(cData)
	//cDataBytes := C.GoBytes(cData, C.int(len(data)))
	//log.Println("Converted C data:", string(cDataBytes))
	//log.Println("WriteMetaData :", len(data))
	result := C.writeToFile(cFilePath, (*C.char)(cData), C.size_t(len(data)), C.size_t(offset))
	if result != 0 {
		return fmt.Errorf("failed to write to file: %s", filePath)
	}
	return nil
}
