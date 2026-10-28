BIN_PATH := bin

.PHONY: run api simulator wsprobe maintenance build clean test vet migrate seed seed-device seed-console bootstrap dev integration

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

migrate:
	go run ./cmd/migrate

api:
	go run ./cmd/api

simulator:
	go run ./cmd/simulator

wsprobe:
	go run ./cmd/wsprobe

maintenance:
	go run ./cmd/maintenance

seed-device:
	set -a; . ./.env; set +a; \
	docker compose exec -T postgres psql -U "$$DB_USER" -d "$$DB_NAME" -v ON_ERROR_STOP=1 < scripts/seed_dev.sql

seed-console:
	go run ./cmd/seed

seed: seed-device seed-console

bootstrap:
	docker compose up -d --wait
	$(MAKE) migrate

dev: bootstrap seed

integration:
	./scripts/integration.sh
