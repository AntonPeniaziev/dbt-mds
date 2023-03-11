#!/bin/bash
# wait-for-s3s.sh
# usage in docker-compose:
#  command: ["./wait-for-s3.sh"]

set -e

args="$@"

export AWS_SECRET_ACCESS_KEY=test
export AWS_ACCESS_KEY_ID=test

until aws --region us-west-2 --no-sign-request --endpoint-url=http://localhost:4566 s3 mb s3://mytestbucket; do
  >&2 echo "S3 is unavailable - sleeping"
  sleep 1
done

>&2 echo "S3 is up - executing command"
exec aws --region us-west-2 --no-sign-request --endpoint-url=http://localhost:4566 s3 rb s3://mytestbucket;
