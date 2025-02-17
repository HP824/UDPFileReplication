# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -Wno-deprecated-declarations

# Directories
SRC_DIR = src
BIN_DIR = bin

# Targets
CLIENT_SRC = $(SRC_DIR)/myclient.cpp
SERVER_SRC = $(SRC_DIR)/myserver.cpp
CLIENT_BIN = $(BIN_DIR)/myclient
SERVER_BIN = $(BIN_DIR)/myserver

# Default target
all: $(BIN_DIR) $(CLIENT_BIN) $(SERVER_BIN)

# Create the bin directory if it doesn't exist
$(BIN_DIR):
	mkdir -p $(BIN_DIR)

# Build the client binary
$(CLIENT_BIN): $(CLIENT_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $<

# Build the server binary
$(SERVER_BIN): $(SERVER_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $<

# Clean up generated files
clean:
	rm -rf $(BIN_DIR)/*

# Phony targets
.PHONY: all clean

