#!/usr/bin/env sh

if [ -n "$1" ]; then
  env_file=$1
  export $(grep -v '^#' "$env_file" | xargs)
fi

root_dir=$(pwd)

exit_status=0
echo "RUNNING lambda/service TESTS"
cd "$root_dir/lambda/service"
go test -v ./...; exit_status=$((exit_status || $? ))

cd "$root_dir/api"
echo "RUNNING api TESTS"
go test -v ./...; exit_status=$((exit_status || $? ))

exit $exit_status

