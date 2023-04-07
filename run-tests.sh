#!/usr/bin/env sh

for env_file in "$@"; do
    if [ -f "$env_file" ]; then
        set -o allexport && . "$env_file" && set +o allexport
    else
        echo "environment file $env_file is missing"
        exit 1
    fi
done

root_dir=$(pwd)

exit_status=0
echo "RUNNING lambda/service TESTS"
cd "$root_dir/lambda/service"
go test -v ./...; exit_status=$((exit_status || $? ))

echo "RUNNING lambda/restore TESTS"
cd "$root_dir/lambda/restore"
go test -v ./...; exit_status=$((exit_status || $? ))

cd "$root_dir/api"
echo "RUNNING api TESTS"
go test -v ./...; exit_status=$((exit_status || $? ))

exit $exit_status

