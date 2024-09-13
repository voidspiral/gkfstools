#include <stdio.h>
#include <stdlib.h>
int main()
{
    FILE *output = fopen("/dev/shm/gkfs/4k", "w");
    if (output == NULL) {
        perror("open file error");
        return 1;
    }
    char *buffer = (char*)malloc(1024);
    fseek(output,1024, SEEK_SET);
    fwrite(buffer, sizeof(char), 1024, output);
    return 0;
}