# Makefile for building shared library from hash.cpp

# Variables
CC = gcc
CFLAGS = -fPIC
LIBS = -shared

# Output
TARGET = stage-in/libhash.so
SOURCE = ./internal/hash/hash.cpp

# Targets
all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

clean:
	rm -f $(TARGET)

.PHONY: all clean