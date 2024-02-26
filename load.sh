#! /usr/bin/env bash
DATABASE=${S3_KEY##*/}
export DATABASE_NAME=${DATABASE%.*}
echo "DATABASE NAME: $DATABASE_NAME"
echo "$EVENT_ID: running with settings: S3_BUCKET=$S3_BUCKET, S3_KEY=$S3_KEY, DATABASE=$DATABASE, DATABASE_NAME=$DATABASE_NAME"


echo "$EVENT_ID: Run EFS Collection Sync"
python3 -m task.sqlite_sync --key=$S3_KEY --bucket=$S3_BUCKET
