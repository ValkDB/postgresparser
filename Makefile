.PHONY: all test vet lint race help

all: vet test lint

test:
	go test -v -race -count=1 -cover $$(go list ./... | grep -v /gen)

vet:
	go vet $$(go list ./... | grep -v /gen)

race:
	go test -v -race -count=1 $$(go list ./... | grep -v /gen)

lint:
	if command -v golangci-lint >/dev/null; then golangci-lint run; else echo "golangci-lint not found"; fi

help:
	@echo "Available targets:"
	@echo "  test  - Run tests with race detector and coverage"
	@echo "  vet   - Run go vet excluding generated code"
	@echo "  race  - Run tests with race detector (no coverage overhead)"
	@echo "  lint  - Run golangci-lint"
	@echo "  all   - Run vet, test, and lint"
