#!/usr/bin/env sh

if [ $# -eq 0 ]; then
    echo "Error: No modules specified. Pass modules as arguments."
    exit 1
fi

exit_status=0

for mod in "$@"; do
    echo "RUNNING $mod TESTS"
    go -C "$mod" test -v ./... || exit_status=1
done

echo "test exit status: $exit_status"
exit $exit_status
