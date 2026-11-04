MVN := ./mvnw -B
SIDECAR := $(MAKE) -C handler-sidecar

.PHONY: help build test verify clean

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

## build: compile and package all modules without tests
build:
	$(MVN) package -DskipTests
	$(SIDECAR) build

## test: unit tests
test:
	$(MVN) test
	$(SIDECAR) test

## verify: clean build, unit tests and architecture rules of Java and Go (run before every commit)
verify:
	$(MVN) clean verify
	$(SIDECAR) check

## clean: remove build output
clean:
	$(MVN) clean
	rm -rf handler-sidecar/bin
