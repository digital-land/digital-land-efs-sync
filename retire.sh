#! /usr/bin/env bash
set -e

echo "Retiring $DATASET_NAME from datasette (DRY_RUN=$DRY_RUN)"
python3 -m task.retire --dataset="$DATASET_NAME" --dry-run="$DRY_RUN"
