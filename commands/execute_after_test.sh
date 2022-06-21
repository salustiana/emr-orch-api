#!/bin/sh

# The following is done to avoid conflicts when running
# tests locally and with `fury run`
echo "Removing cached .pycs after test"
find . -name __pycache__ | xargs rm -rf
