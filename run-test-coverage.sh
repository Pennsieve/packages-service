#!/usr/bin/env sh

if [ $# -eq 0 ]; then
    echo "Error: No modules specified. Pass modules as arguments."
    exit 1
fi

exit_status=0

for mod in "$@"; do
    echo "RUNNING $mod TEST COVERAGE"
    go -C "$mod" test -coverprofile=coverage.out -v ./... || exit_status=1
    go -C "$mod" tool cover -func=coverage.out
done

exit $exit_status
