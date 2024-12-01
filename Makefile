fmt:
	uv ruff format aiocluster tests examples

mypy:
	uv mypy --strict aiocluster tests

lint:
	uv run ruff check aiocluster tests

fix:
	uv run ruff check --fix aiocluster tests examples

test:
	uv run pytest -sv tests/

# activate:
# 	. .venv/bin/activate

protos:
	protoc --proto_path=./ --python_out=./  --mypy_out ./ aiocluster/protos/messages.proto
	# same thing but without mypy  extension
	#protoc --proto_path=./ --python_out=./  --pyi_out=./ aiocluster/protos/messages.proto
