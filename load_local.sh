#! /usr/bin/env bash
DATABASE=${S3_KEY##*/}
export DATABASE_NAME=${DATABASE%.*}
echo "DATABASE NAME: $DATABASE_NAME"
echo "$EVENT_ID: running with settings: S3_BUCKET=$S3_BUCKET, S3_KEY=$S3_KEY, DATABASE=$DATABASE, DATABASE_NAME=$DATABASE_NAME"


# Testing
echo "$EVENT_ID: Run EFS Collection Sync"

python3 -c "
from collection_sync import CollectionSync

# Create an instance of the CollectionSync
collection_sync = CollectionSync('$EVENT_ID')

# Call the processObject
collection_sync.processObject('$S3_KEY', '$S3_BUCKET')
"
