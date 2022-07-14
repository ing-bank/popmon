#!/bin/bash

# (re)create required directories
rm -rf autogen
mkdir -p source/_static autogen

# auto-generate code documentation
export SPHINX_APIDOC_OPTIONS="members,show-inheritance,ignore-module-all"
sphinx-apidoc -f -M -H "API Documentation" -o autogen ../popmon
mv autogen/modules.rst autogen/code.rst
mv autogen/* source/ 

# remove auto-gen directory
rm -rf autogen
