MVN := ./mvnw -B
SIDECAR := $(MAKE) -C handler-sidecar

STAND := docker compose -f crawler-example/stand/compose.yaml
PROFILE ?= memory

.PHONY: help build test verify it image stand stand-down stand-it clean

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

## it: integration tests: Java node against the handler-sidecar image (needs Docker); not part of verify
it:
	$(MVN) -Pit verify

## stand: docker-compose stand of the crawler, PROFILE=memory|cluster (default memory); Ctrl+C stops it
stand:
	$(MVN) package -DskipTests -pl crawler-example/url-manager -am
	$(STAND) --profile $(PROFILE) up --build

## stand-down: remove the containers and volumes of the stand (both profiles)
stand-down:
	$(STAND) --profile memory --profile cluster down -v

## stand-it: StandIT - the stand of each profile under requests, a url-manager stopped in the middle (needs Docker)
stand-it:
	$(MVN) -Pstand verify -pl crawler-example/url-manager -am -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false

## image: docker image of handler-sidecar
image:
	$(SIDECAR) image

## clean: remove build output
clean:
	$(MVN) clean
	rm -rf handler-sidecar/bin
