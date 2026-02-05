
.PHONY: build vendorbuild clean fmt gen

default: build

build: fmt
	go build -o diffy main.go



fmt:
	@go mod tidy && find . -path vendor -prune -o -type f -iname '*.go' -exec go fmt {} \;


