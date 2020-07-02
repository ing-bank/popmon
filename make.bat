:: Emulation of Makefile on Windows based on the one found in `pandas-profiling`
@echo off
setlocal enabledelayedexpansion

IF "%1%" == "lint" (
	IF "%2%" == "check" (
		SET ISORT_ARG= --check-only
		SET BLACK_ARG= --check
	) ELSE (
		set ISORT_ARG=
		set BLACK_ARG=
	)
	isort !ISORT_ARG! --project popmon --thirdparty histogrammar --thirdparty pybase64 --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=88 -y
	black !BLACK_ARG! .
	GOTO end
)

IF "%1%" == "install" (
	pip install -e .
	GOTO end
)

ECHO "No command matched"
:end