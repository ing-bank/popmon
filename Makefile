lint:
	isort --project popmon --thirdparty histogrammar --thirdparty pybase64 --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=88 -y
	black .

lint_check:
	isort --check-only --project popmon --thirdparty histogrammar --thirdparty pybase64 --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=88 -y
	black --check .
