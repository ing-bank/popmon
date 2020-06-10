lint:
	isort --project popmon --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=88 -y
	black .

install:
	pip install -e .
