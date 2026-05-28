GO ?= go
BIN := rss2msg

.PHONY: build
build:
	$(GO) build -o $(BIN) ./cmd/rss2msg

.PHONY: test
test:
	$(GO) test -race ./...

.PHONY: test-integration
test-integration:
	$(GO) test -race -tags=integration ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: clean
clean:
	rm -f $(BIN)
