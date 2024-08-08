#!/usr/bin/bash
export HOST_SIZE=4
mpirun --allow-run-as-root -np 4 ../stage-in/stage-in ../test/1G ../test/output ../test/gkfs_hosts.txt ../test/gkfs_hosts.txt.pid