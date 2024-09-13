package metadata

/*
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdio.h>

int writeToFile(const char *filePath, const char *data, size_t dataSize, size_t offset) {
    int fd = open(filePath, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        perror("open file error");
        return 1;
    }

    ssize_t bytesWritten = pwrite(fd, data, dataSize, offset);
    if (bytesWritten == -1) {
        perror("pwrite error");
        close(fd);
        return 1;
    }
    close(fd);
    return 0;
}
*/
import "C"
import (
	"fmt"
)

func WriteMetaData(filePath string, data []byte, offset uint64) error {
	cFilePath := C.CString(filePath)
	//defer C.free(unsafe.Pointer(cFilePath))
	//log.Println("Original data:", string(data))
	cData := C.CBytes(data)
	//defer C.free(cData)
	//cDataBytes := C.GoBytes(cData, C.int(len(data)))
	//log.Println("Converted C data:", string(cDataBytes))
	//log.Println("WriteMetaData :", len(data))
	result := C.writeToFile(cFilePath, (*C.char)(cData), C.size_t(len(data)), C.size_t(offset))
	if result != 0 {
		return fmt.Errorf("failed to write to file: %s", filePath)
	}
	return nil
}
