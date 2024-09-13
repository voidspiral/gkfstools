#include <stdio.h>
int main() {
    FILE *file = fopen("/dev/shm/gkfs/4k", "r"); // 以只读模式打开文件
    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }
    // 假设我们要读取 1GB 的数据
    size_t size = 1024; // 每次读取 1024 字节
    char buffer[1024]; // 存储读取数据的缓冲区
    size_t bytesRead;

    // 循环读取文件
    while ((bytesRead = fread(buffer, 1, size, file)) > 0) {
        // 处理读取的数据
        // 例如，可以将数据写入另一个文件或进行其他操作
        // fwrite(buffer, 1, bytesRead, outputFile); // 示例：写入另一个文件
        printf("%d\n", bytesRead);
    }

    if (ferror(file)) {
        perror("Error reading file");
    }
    fclose(file); // 关闭文件
    return 0;
}
