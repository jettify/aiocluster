fmt:
	ruff format aiocluster tests examples

mypy:
	mypy --strict aiocluster tests

lint:
	ruff check aiocluster tests

fix:
	ruff check --fix aiocluster tests examples

test:
	pytest -sv tests/

activate:
	. .venv/bin/activate

protos:
	protoc --proto_path=./ --python_out=./  --mypy_out ./ aiocluster/protos/messages.proto
	# same thing but without mypy  extension
	#protoc --proto_path=./ --python_out=./  --pyi_out=./ aiocluster/protos/messages.proto
