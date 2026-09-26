.PHONY: help start project & test file

PROJECT := mini-redis
TEST_FILE := client

RUNTIME := cargo run --bin

COMPOSE := $(RUNTIME)

help:
	@echo "make start            Start mini-redis"
	@echo "make test          Run test file"


start:
	$(COMPOSE) $(PROJECT)

test:
	$(COMPOSE) $(TEST_FILE)