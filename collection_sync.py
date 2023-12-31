import os
import logging
import sqlite3
import csv
import requests
from botocore.client import Config
import boto3
import json
import sys
import shutil

SPECIFICATION_URL = "https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv"


class CollectionSync:
    def __init__(self, eventId):
        self.s3_client = boto3.client("s3")
        self.logger = logging.getLogger("efs-sync")
        self.specifications = None
        self.logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        self.logger.addHandler(console_handler)

    def processObject(self, Key, Bucket):
        self.logger.info(f"Processing new object - Key: {Key} Bucket: {Bucket}")
        fileName = os.path.basename(Key)

        if self.shouldSync(Key):
            temporaryFilePath = f"/mnt/datasets/temporary/{fileName}"
            finalFilePath = f"/mnt/datasets/{fileName}"

            if os.path.exists(temporaryFilePath):
                os.remove(temporaryFilePath)

            self.copyFileFromS3(Key, Bucket, temporaryFilePath)
            self.checkDatabaseIntegrity(temporaryFilePath)
            self.moveDatabase(temporaryFilePath, finalFilePath, Key, Bucket)

            if Key not in [
                "digital-land-builder/dataset/digital-land.sqlite3",
                "entity-builder/dataset/entity.sqlite3",
            ]:
                self.copyFileFromS3(f"{Key}.json", Bucket, f"{finalFilePath}.json")

            self.updateInspectionFile()
        else:
            self.logger.info(
                f"Object is not subject to sync, skipping - Key: {Key} Bucket: {Bucket}"
            )

    def shouldSync(self, Key):
        if Key in [
            "digital-land-builder/dataset/digital-land.sqlite3",
            "entity-builder/dataset/entity.sqlite3",
        ]:
            self.logger.info("Match builders keys")
            return True

        for cd in self.getSpecifications():
            # self.logger.info(cd)  # Log the details of cd
            if cd["collection"] in Key:
                self.logger.info(f"Found Item in Specifications: Collection: {cd}")
                return True

        return False

    def checkDatabaseIntegrity(self, databasePath):
        db = sqlite3.connect(databasePath)
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

    def moveDatabase(self, temporaryFilePath, finalFilePath, Key, Bucket):
        if os.path.exists(finalFilePath):
            # Try to delete a related .json file
            json_file_path = f"{finalFilePath}.json"
            try:
                os.remove(json_file_path)
            except OSError as e:
                self.logger.error(
                    f"Error deleting JSON file: {e} : Key: {Key}, Bucket: {Bucket}"
                )

            self.logger.info(f"Deleting old file. Key: {Key}, Bucket: {Bucket}")

            # Delete the file at the final path
            try:
                os.remove(finalFilePath)
            except OSError as e:
                self.logger.error(
                    f"Error deleting final file: {e} : Key: {Key}, Bucket: {Bucket}"
                )
                return

            # Rename the file from the temporary path to the final path
            try:
                shutil.move(temporaryFilePath, finalFilePath)
            except OSError as e:
                self.logger.error(
                    f"Error moving file:: {e} : Key: {Key}, Bucket: {Bucket}"
                )
                return

            self.logger.info(f"Renaming file to new path: Key: {Key}, Bucket: {Bucket}")
        else:
            try:
                shutil.move(temporaryFilePath, finalFilePath)
                self.logger.info(
                    f"Renaming file to new path: Key: {Key}, Bucket: {Bucket}"
                )
            except OSError as e:
                self.logger.error(
                    f"Error moving file:: {e} : Key: {Key}, Bucket: {Bucket}"
                )

    def copyDatabaseContents(self, sourcePath, destinationPath):
        sourceDB = sqlite3.connect(sourcePath)
        destinationDB = sqlite3.connect(destinationPath)
        sourceTables = destinationTables = []

        try:
            with sourceDB:
                cursor = sourceDB.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                )
                sourceTables = [row[0] for row in cursor.fetchall()]

            with destinationDB:
                cursor = destinationDB.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                )
                destinationTables = [row[0] for row in cursor.fetchall()]

        except sqlite3.Error as e:
            self.logger.error(f"SQLite error: {e}")

        tablesToCreate = [
            table for table in sourceTables if table not in destinationTables
        ]

        for table in tablesToCreate:
            cursor = sourceDB.cursor()
            cursor.execute(
                f"SELECT sql FROM sqlite_master WHERE tbl_name = '{table}' AND sql IS NOT NULL;"
            )
            createTable = cursor.fetchone()
            if createTable:
                self.logger.debug(
                    f"Will create new table {table} with SQL {createTable[0]}"
                )
                with destinationDB:
                    destinationDB.execute(createTable[0])

        for destinationTable in destinationTables:
            with destinationDB:
                destinationDB.execute(f"DELETE FROM {destinationTable}")

        statement = f"""
            ATTACH '{sourcePath}' AS src;
            {";".join([f"INSERT INTO {table} SELECT * FROM src.{table}" for table in sourceTables])};
        """
        with destinationDB:
            destinationDB.executescript(statement)

        sourceDB.close()
        destinationDB.close()

    def getSpecifications(self):
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

    def copyFileFromS3(self, Key, Bucket, destinationPath):
        try:
            self.s3_client.download_file(Bucket, Key, destinationPath)
            self.logger.info(
                f"Finished copying file - Key: {Key}, Bucket: {Bucket}, destinationPath: {destinationPath}"
            )
        except Exception as error:
            self.logger.error(
                f"Error copying file: {error}",
                {"Key": Key, "Bucket": Bucket, "destinationPath": destinationPath},
            )

    def updateInspectionFile(self):
        files = os.listdir("/mnt/datasets")
        currentInspections = {}

        self.logger.info(f"Found files to process for inspections - files: {files}")

        for file in files:
            if not file.endswith(".json") or file == "inspect-data-all.json":
                continue

            try:
                with open(f"/mnt/datasets/{file}", "r", encoding="utf-8") as f:
                    inspection = json.load(f)
                    currentInspections.update(inspection)
            except Exception as error:
                self.logger.error(
                    "Failed to parse inspection file",
                    {"inspectionFile": f"/mnt/datasets/{file}"},
                )

        with open("/mnt/datasets/inspect-data-all.json", "w") as f:
            json.dump(currentInspections, f)

        self.logger.info("Refreshed inspections: %s", list(currentInspections.keys()))
