ifeq ($(check),1)
	CHECK_ARG= --check
else
	CHECK_ARG=
endif

lint:
	isort $(CHECK_ARG) --profile black --project popmon --thirdparty histogrammar --thirdparty pybase64 .
	black $(CHECK_ARG) .

install:
	pip install -e .
