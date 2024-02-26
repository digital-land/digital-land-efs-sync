import os
import logging
import sqlite3
import csv
import requests
import boto3
import json
import sys
import shutil
import click
import tempfile

from pathlib import Path
from botocore.client import Config

from task.utils import generate_sqlite_hash

SPECIFICATION_URL = "https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv"


class CollectionSync:
    def __init__(self, mnt_dir=None,temp_dir=None):
        
        if mnt_dir:
            self.mnt_dir = Path(mnt_dir)
        else:
            mnt_dir = Path('/mnt/')
        
        if temp_dir:
            self.temp_dir = Path(temp_dir)
        else:
            self.temp_dir = Path('var')
        


        # add output directories for info to be stored, it's assumed this
        # will be in a mounted volume
        self.dataset_dir = mnt_dir / 'datasets'
        self.hash_dir = mnt_dir/ 'datasets'/ 'hashes'

        # make sure file structure is made
        self.temp_dir.mkdir(parents=True,exist_ok=True)
        self.dataset_dir.mkdir(parents=True,exist_ok=True)
        self.hash_dir.mkdir(parents=True,exist_ok=True)


        # set up s3 client
        self.s3_client = boto3.client("s3")
        self.logger = logging.getLogger("efs-sync")
        self.specifications = None

        # set logger for class, this can probably be done for the whole file
        self.logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        self.logger.addHandler(console_handler)


    def get_current_sqlite_hash(self,sqlite_stem):
        hash_json_path = self.hash_dir / f'{sqlite_stem}.json'
        if hash_json_path.exists():
            with open(self.hash_dir / f'{sqlite_stem}.json') as file:
                hash = json.load(file)['hash']
            return hash

    def update_current_sqlite_hash(self,sqlite_name,new_sqlite_hash):

        # Specify the file path
        file_path = self.hash_dir / f"{sqlite_name}.json"

        # Open the file in write mode and write the JSON string to it
        with open(file_path, 'w') as file:
            hash_dict = {'hash':new_sqlite_hash}
            file.write(json.dumps(hash_dict))
        
        self.logger.info(f'check file exists:{file_path.exists()}')


    def process_object(self, key, bucket):
        key_path  = Path(key)
        self.logger.info(f"Processing new object - Key: {key} Bucket: {bucket}") 

        if self.should_sync(key):
            
            
            temp_file_path = self.temp_dir / key_path.name
            final_file_path = self.dataset_dir / key_path.name

            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

            self.copy_file_from_s3(key, bucket, temp_file_path)
            self.check_sqlite_integrity(temp_file_path)

            current_sqlite_hash = self.get_current_sqlite_hash(key_path.stem)
            new_sqlite_hash = generate_sqlite_hash(temp_file_path)
            self.logger.info(
                    f"new hash :{new_sqlite_hash}, current hash:{current_sqlite_hash} - Key: {key} Bucket: {bucket}"
                )

            if new_sqlite_hash != current_sqlite_hash:
            
                self.move_database(temp_file_path, final_file_path, key, bucket)

                if key not in [
                    "digital-land-builder/dataset/digital-land.sqlite3",
                    "entity-builder/dataset/entity.sqlite3",
                ]:
                    self.copy_file_from_s3(f"{key}.json", bucket, f"{final_file_path}.json")

                self.update_inspection_file()

                # sqlite has been moved now to update the json 
                self.update_current_sqlite_hash(key_path.stem,new_sqlite_hash)
                self.logger.info(
                    f"Object has been updated, including hashes and json inspection file new hash:{self.get_current_sqlite_hash(key_path.stem)} - Key: {key} Bucket: {bucket}"
                )

            
            else:
                self.logger.info(
                f"Object is not subject to sync hashes match so no updates to data, skipping - Key: {key} Bucket: {bucket}"
            )
        else:
            self.logger.info(
                f"Object is not subject to sync update specification to include, skipping - Key: {key} Bucket: {bucket}"
            )

    def should_sync(self, key):
        if key in [
            "digital-land-builder/dataset/digital-land.sqlite3",
            "entity-builder/dataset/entity.sqlite3",
        ]:
            self.logger.info("Match builders keys")
            return True

        # TODO need to review the below, it seems like a weird way to do this
        for cd in self.get_specifications():
            # self.logger.info(cd)  # Log the details of cd
            if cd["collection"] in key:
                self.logger.info(f"Found Item in Specifications: Collection: {cd}")
                return True

        return False

    def check_sqlite_integrity(self, sqlite_path):
        db = sqlite3.connect(sqlite_path)
        logger = self.logger
        try:
            with db:
                cursor = db.cursor()
                cursor.execute("pragma quick_check;")
                result = cursor.fetchone()
                if result and result[0] != "ok":
                    raise Exception(f"Integrity check failed {result[0]}")
                logger.info(f"SQLite integrity check - Result: {result}")
        except sqlite3.Error as e:
            logger.error(f"SQLite error: {e}")

    def move_database(self, temporary_file_path, final_file_path, key, bucket):
        if os.path.exists(final_file_path):
            # Try to delete a related .json file
            json_file_path = f"{final_file_path}.json"
            try:
                os.remove(json_file_path)
            except OSError as e:
                self.logger.error(
                    f"Error deleting JSON file: {e} : Key: {key}, Bucket: {bucket}"
                )

            self.logger.info(f"Deleting old file. Key: {key}, Bucket: {bucket}")

            # Delete the file at the final path
            try:
                os.remove(final_file_path)
            except OSError as e:
                self.logger.error(
                    f"Error deleting final file: {e} : Key: {key}, Bucket: {bucket}"
                )
                return

            # Rename the file from the temporary path to the final path
            try:
                shutil.move(temporary_file_path, final_file_path)
            except OSError as e:
                self.logger.error(
                    f"Error moving file:: {e} : Key: {key}, Bucket: {bucket}"
                )
                return

            self.logger.info(f"Renaming file to new path: Key: {key}, Bucket: {bucket}")
        else:
            try:
                shutil.move(temporary_file_path, final_file_path)
                self.logger.info(
                    f"Renaming file to new path: Key: {key}, Bucket: {bucket}"
                )
            except OSError as e:
                self.logger.error(
                    f"Error moving file:: {e} : Key: {key}, Bucket: {bucket}"
                )

    def copy_database_contents(self, source_path, destination_path):
        source_db = sqlite3.connect(source_path)
        destination_db = sqlite3.connect(destination_path)
        source_tables = destination_tables = []

        try:
            with source_db:
                cursor = source_db.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                )
                source_tables = [row[0] for row in cursor.fetchall()]

            with destination_db:
                cursor = destination_db.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                )
                destinationTables = [row[0] for row in cursor.fetchall()]

        except sqlite3.Error as e:
            self.logger.error(f"SQLite error: {e}")

        tablesToCreate = [
            table for table in source_tables if table not in destinationTables
        ]

        for table in tablesToCreate:
            cursor = source_db.cursor()
            cursor.execute(
                f"SELECT sql FROM sqlite_master WHERE tbl_name = '{table}' AND sql IS NOT NULL;"
            )
            createTable = cursor.fetchone()
            if createTable:
                self.logger.debug(
                    f"Will create new table {table} with SQL {createTable[0]}"
                )
                with destination_db:
                    destination_db.execute(createTable[0])

        for destinationTable in destinationTables:
            with destination_db:
                destination_db.execute(f"DELETE FROM {destinationTable}")

        statement = f"""
            ATTACH '{source_path}' AS src;
            {";".join([f"INSERT INTO {table} SELECT * FROM src.{table}" for table in source_tables])};
        """
        with destination_db:
            destination_db.executescript(statement)

        source_db.close()
        destination_db.close()

    def get_specifications(self):
        if not hasattr(self, "specifications"):
            self.specifications = None

        try:
            response = requests.get(SPECIFICATION_URL)
            response.raise_for_status()
            lines = response.text.splitlines()
            reader = csv.reader(lines, delimiter=",")

            records = list(reader)
        except requests.RequestException as e:
            self.logger.info(f"Error fetching specifications: {e}")
            return None
        except csv.Error as e:
            self.logger.info(f"Error parsing CSV data: {e}")
            return None

        fields = records.pop(0)
        collectionField = fields.index("collection")
        datasetField = fields.index("dataset")

        self.specifications = []
        for spec in records:
            if spec[collectionField]:  # Ensure the 'collection' field is not empty
                specification = {
                    "collection": spec[collectionField],
                    "dataset": spec[datasetField],
                }
                self.specifications.append(specification)

        # Log the entire specifications list
        self.logger.info(f"Specifications lists: {self.specifications}")

        # self.logger.info('Finished getSpecifications')

        return self.specifications

    def copy_file_from_s3(self, key, bucket, destination_path):
        try:
            print(key)
            self.s3_client.download_file(bucket, key, destination_path)
            self.logger.info(
                f"Finished copying file - key: {key}, bucket: {bucket}, destination_path: {destination_path}"
            )
        except Exception as error:
            self.logger.error(
                f"Error copying file: {error}",
                {"key": key, "bucket": bucket, "destination_path": destination_path},
            )
    
    def update_inspection_file(self):
        files = [file_path.name for file_path in self.dataset_dir.iterdir() if file_path.is_file()]
        current_inspections = {}

        self.logger.info(f"Found files to process for inspections - files: {files}")

        for file in files:
            if not file.endswith(".json") or file == "inspect-data-all.json":
                continue

            try:
                with open(self.dataset_dir/ file, "r", encoding="utf-8") as f:
                    inspection = json.load(f)
                    current_inspections.update(inspection)
            except Exception as error:
                self.logger.error(
                    "Failed to parse inspection file",
                    {"inspection_file": self.dataset_dir / file},
                )

        with open(self.dataset_dir / "inspect-data-all.json", "w") as f:
            json.dump(current_inspections, f)

        self.logger.info("Refreshed inspections: %s", list(current_inspections.keys()))

@click.command()
@click.option('--key')
@click.option('--bucket')
def sync_dataset_sqlite_file(key,bucket):
    # Create an instance of the CollectionSync
    collection_sync = CollectionSync()
    collection_sync.process_object(key, bucket)


if __name__ == '__main__':
    sync_dataset_sqlite_file()