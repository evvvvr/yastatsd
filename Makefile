all: clean fmt test build

build:
	go build

clean:
	go clean

test:
	go test ./...

fmt:
	go fmt ./...