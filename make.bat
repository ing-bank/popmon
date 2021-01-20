:: Emulation of Makefile on Windows based on the one found in `pandas-profiling`
@echo off
setlocal enabledelayedexpansion

IF "%1%" == "lint" (
	pre-commit run --all-files
	GOTO end
)

IF "%1%" == "install" (
	pip install -e .
	GOTO end
)

ECHO "No command matched"
:end