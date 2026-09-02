APP_NAME := fleet-tracking-backend
CMD_PATH := ./cmd
BIN_PATH := bin

.PHONY: run build clean test

run: 
	go run $(CMD_PATH)

build:
	go build -o $(BIN_PATH)/$(APP_NAME) $(CMD_PATH)

clean:
	rm -rf $(BIN_PATH)

test:
	go test ./...
