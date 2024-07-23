#! /usr/bin/env bash

s3_object_arn_regex="^arn:aws:s3:::([0-9A-Za-z-]*/)(.*)$"

if ! [[ "$S3_OBJECT_ARN" =~ $s3_object_arn_regex ]]; then
    echo "Received invalid S3 Object S3 ARN: $S3_OBJECT_ARN, skipping"
    exit 1
fi

S3_BUCKET=${BASH_REMATCH[1]%/*}
S3_KEY=${BASH_REMATCH[2]}

DATABASE=${S3_KEY##*/}
export DATABASE_NAME=${DATABASE%.*}
echo "DATABASE NAME: $DATABASE_NAME"
echo "$EVENT_ID: running with settings: S3_BUCKET=$S3_BUCKET, S3_KEY=$S3_KEY, DATABASE=$DATABASE, DATABASE_NAME=$DATABASE_NAME"


echo "$EVENT_ID: Run EFS Collection Sync"
python3 -m task.sqlite_sync --key=$S3_KEY --bucket=$S3_BUCKET
