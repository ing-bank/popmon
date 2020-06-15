ifeq ($(check),1)
	ISORT_ARG= --check-only
	BLACK_ARG= --check
else
	ISORT_ARG=
	BLACK_ARG=
endif

lint:
	isort $(ISORT_ARG) --project popmon --thirdparty histogrammar --thirdparty pybase64 --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=88 -y
	black $(BLACK_ARG) .
