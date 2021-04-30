lint:
	pre-commit run --all-files

install:
	pip install -e .

changelog:
	npm run release
