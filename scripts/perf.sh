#!/bin/bash

declare -a files=(
    "4K-1"
    "13K-1"
    "2M-1"
    "8M-1"
    "50M-1"
    "1G-1"
    "10G-1"
)

for file in "${files[@]}"; do
    src="/fs2/home/wuhuijun/lmj/code/new/tools/scripts/${file}"
    dst="/dev/shm/gkfs/${file}"
    { time ./auto.sh "$src" "$dst"; } 2>> "$log_file"
done

echo "All files processed."