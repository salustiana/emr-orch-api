#!/bin/sh

echo "Removing cached .pycs before test"
find . -name __pycache__ | xargs rm -rf

pip install -r requirements-test.txt

exit $?
