.PHONY: install test lint smoke-local

install:
	python -m pip install -e .

test:
	python -m unittest discover -s tests -p 'test_*.py' -v

lint:
	python -m compileall src tests

smoke-local:
	./scripts/smoke_test_local_stack.sh
