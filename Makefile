fmt:
	ruff format aioc tests examples

mypy:
	mypy --strict aioc tests

lint:
	ruff check aioc tests

fix:
	ruff check --fix aioc tests examples

test:
	pytest -sv tests/

activate:
	. .venv/bin/activate

protos:
	protoc --proto_path=./ --python_out=./  --pyi_out=./ aioc/protos/messages.proto
