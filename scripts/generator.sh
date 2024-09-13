#!/bin/bash

declare -A sizes
sizes=( ["4k"]="4K-1" ["13k"]="13K-1" ["2M"]="2M-1" ["8M"]="8M-1" ["50M"]="50M-1" ["1G"]="1G-1" ["10G"]="10G-1" )

for size in "${!sizes[@]}"; do
    filename=${sizes[$size]}
    echo "Generating file: ${filename} with size ${size}"
    dd if=/dev/urandom of=${filename} bs=${size} count=1 iflag=fullblock
done

echo "All files generated."