#!/usr/bin/env sh

if [ -n "$1" ]; then
  env_file=$1
  export $(grep -v '^#' "$env_file" | xargs)
fi

root_dir=$(pwd)

exit_status=0
echo "RUNNING lambda/service TEST COVERAGE"
cd "$root_dir/lambda/service"
go test -coverprofile=coverage.out -v ./...; exit_status=$((exit_status || $? ))
go tool cover -func=coverage.out

cd "$root_dir/api"
echo "RUNNING api TEST COVERAGE"
go test -coverprofile=coverage.out -v ./...; exit_status=$((exit_status || $? ))
go tool cover -func=coverage.out

exit $exit_status

