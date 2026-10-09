"""
Remove a retired dataset's database from datasette.

Datasette serves the databases listed in inspect-data-all.json, and restarts itself
when that file changes. It will not start if a listed database's file is missing, which
would take every database offline, so the dataset is taken out of
inspect-data-all.json first, and its database file is only removed once datasette has
had time to restart without it.
"""

import json
import logging
import time
from pathlib import Path

import click

from task.sqlite_sync import CollectionSync

logger = logging.getLogger("efs-sync")

# Built by the builders rather than by a collection, and always served by datasette
PROTECTED_DATABASES = {"digital-land", "performance", "entity"}

# Datasette checks inspect-data-all.json every 2 seconds, then waits 5 seconds for the
# old process to stop before starting the new one
RESTART_WAIT_SECONDS = 30


def is_dry_run(value):
    """Only an explicit "false" removes anything; any other value is a dry run."""
    return value != "false"


def is_served(dataset_dir, dataset):
    inspect_path = dataset_dir / "inspect-data-all.json"
    if not inspect_path.exists():
        return False
    with open(inspect_path) as file:
        return dataset in json.load(file)


def retire_dataset(mnt_dir, dataset, dry_run, wait_seconds=RESTART_WAIT_SECONDS):
    """
    Remove the dataset's database, its inspection file and its stored hash from the
    datasette volume, or only list them on a dry run. Returns the files found.
    """
    if dataset in PROTECTED_DATABASES:
        raise ValueError(
            f"{dataset} is built by a builder, not a collection, and cannot be retired"
        )

    # Rebuilds inspect-data-all.json with the sync's own method, so the two can't drift
    collection_sync = CollectionSync(mnt_dir=Path(mnt_dir))
    inspection_file = collection_sync.dataset_dir / f"{dataset}.sqlite3.json"
    database_file = collection_sync.dataset_dir / f"{dataset}.sqlite3"
    # Removed too, or the sync would skip the database if it were added back unchanged
    hash_file = collection_sync.hash_dir / f"{dataset}.json"

    served = is_served(collection_sync.dataset_dir, dataset)
    files = [
        path for path in (inspection_file, database_file, hash_file) if path.exists()
    ]

    logger.info(f"{dataset} is {'' if served else 'not '}served by datasette")
    for path in files:
        logger.info(
            f"{'would remove' if dry_run else 'removing'} {path} ({path.stat().st_size:,} bytes)"
        )
    if not files:
        logger.info(f"no files found for {dataset} in {collection_sync.dataset_dir}")

    if dry_run or not files:
        return files

    if inspection_file.exists():
        inspection_file.unlink()
    collection_sync.update_inspection_file()

    if served:
        logger.info(
            f"waiting {wait_seconds}s for datasette to restart without {dataset}"
        )
        time.sleep(wait_seconds)

    for path in (database_file, hash_file):
        if path.exists():
            path.unlink()

    logger.info(f"removed {len(files)} files for {dataset}")
    return files


@click.command()
@click.option("--dataset", required=True)
@click.option(
    "--dry-run",
    "dry_run_value",
    default="true",
    help='Removes files only when exactly "false"',
)
@click.option("--mnt-dir", default="/mnt")
def retire(dataset, dry_run_value, mnt_dir):
    retire_dataset(mnt_dir, dataset, is_dry_run(dry_run_value))


if __name__ == "__main__":
    retire()
