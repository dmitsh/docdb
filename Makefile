all: build

build:
	go build ./cmd/docdb

test:
	go test ./...

run:
	./scripts/run-all.sh
