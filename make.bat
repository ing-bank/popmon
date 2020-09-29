:: Emulation of Makefile on Windows based on the one found in `pandas-profiling`
@echo off
setlocal enabledelayedexpansion

IF "%1%" == "lint" (
	IF "%2%" == "check" (
		SET CHECK_ARG= --check
	) ELSE (
		set CHECK_ARG=
	)
	isort !CHECK_ARG! --profile black --project popmon --thirdparty histogrammar --thirdparty pybase64 .
	black !CHECK_ARG! .
	GOTO end
)

IF "%1%" == "install" (
	pip install -e .
	GOTO end
)

ECHO "No command matched"
:end