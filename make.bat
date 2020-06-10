:: Emulation of Makefile on Windows based on the one found in `pandas-profiling`
@echo off
setlocal enabledelayedexpansion

IF "%1%" == "lint" (
	isort --project popmon --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=88 -y
	black .
	GOTO end
)

ECHO "No command matched"
:end