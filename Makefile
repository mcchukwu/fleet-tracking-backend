BIN_PATH := bin

.PHONY: run build clean test vet integration

run:
	go run ./cmd/ingest

build:
	mkdir -p $(BIN_PATH)
	go build -o $(BIN_PATH)/ingest ./cmd/ingest
	go build -o $(BIN_PATH)/loadgen ./cmd/loadgen
	go build -o $(BIN_PATH)/routetest ./cmd/routetest

clean:
	rm -rf $(BIN_PATH)

test:
	go test ./...

vet:
	go vet ./...

integration:
	./scripts/integration.sh
