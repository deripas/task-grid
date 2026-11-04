MVN := ./mvnw -B

.PHONY: help build test verify clean

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

## build: compile and package all modules without tests
build:
	$(MVN) package -DskipTests

## test: unit tests
test:
	$(MVN) test

## verify: full check - unit tests and architecture rules (run before every commit)
verify:
	$(MVN) verify

## clean: remove build output
clean:
	$(MVN) clean
