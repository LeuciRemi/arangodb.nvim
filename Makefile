NVIM ?= nvim

.PHONY: test integration format lint docs

test:
	$(NVIM) --clean --headless -u tests/minimal_init.lua -l tests/run.lua

integration:
	@test -n "$(ARANGODB_TEST_URL)" || (echo "ARANGODB_TEST_URL must point to a disposable database" && exit 1)
	$(NVIM) --clean --headless -u tests/minimal_init.lua -l tests/integration.lua

format:
	stylua lua plugin tests

lint:
	stylua --check lua plugin tests
	$(MAKE) test

docs:
	$(NVIM) --clean --headless -u NONE "+helptags doc" +qa
