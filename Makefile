PROJECT_DIRS = $(shell glide novendor)

all: clean fmt test build

build:
	go build

clean:
	go clean

test:
	go test $(PROJECT_DIRS)

fmt:
	go fmt $(PROJECT_DIRS)