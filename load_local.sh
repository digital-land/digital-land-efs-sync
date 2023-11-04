#! /usr/bin/env bash
S3_BUCKET="development-collection-data"
S3_KEY="central-activities-zone-collection/dataset/central-activities-zone.sqlite3"
EVENT_ID="78afd819-ae61-458b-81a7-421f61848465"
# DATABASE=central-activities-zone.sqlite3
# DATABASE_NAME=central-activities-zone

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
