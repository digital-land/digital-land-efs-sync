import os
import logging
import sqlite3
import csv
import requests
from botocore.client import Config
import boto3
import json

SPECIFICATION_URL = 'https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv'
LOG_LEVEL = 'debug'

class CollectionSync:
    def __init__(self, eventId):
        self.s3_client = boto3.client('s3')
        self.logger = logging.getLogger('efs-sync-collection')
        self.logger.setLevel(LOG_LEVEL) 
        
        formatter = logging.Formatter('{"service": "efs-sync-collection", "event": "' + eventId + '"}')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(console_handler)

    def processObject(self, Key, Bucket):
        self.logger.info('Processing new object', {'Key': Key, 'Bucket': Bucket})
        fileName = os.path.basename(Key)

        if self.shouldSync(Key):
            temporaryFilePath = f'/mnt/datasets/temporary/{fileName}'
            finalFilePath = f'/mnt/datasets/{fileName}'

            if os.path.exists(temporaryFilePath):
                os.remove(temporaryFilePath)

            self.copyFileFromS3(Key, Bucket, temporaryFilePath)
            self.checkDatabaseIntegrity(temporaryFilePath)
            self.moveDatabase(temporaryFilePath, finalFilePath, Key, Bucket)

            if Key not in [
                "digital-land-builder/dataset/digital-land.sqlite3",
                "entity-builder/dataset/entity.sqlite3"
            ]:
                self.copyFileFromS3(f'{Key}.json', Bucket, f'{finalFilePath}.json')

            self.updateInspectionFile()
        else:
            self.logger.info('Object is not subject to sync, skipping.', {'Key': Key, 'Bucket': Bucket})

    def shouldSync(self, Key):
        return Key in [
            "digital-land-builder/dataset/digital-land.sqlite3",
            "entity-builder/dataset/entity.sqlite3"
        ] or any(
            cd['collection'] == f"{cd['collection']}-collection/dataset/{cd['dataset']}.sqlite3"
            for cd in self.getSpecifications()
        )

    def checkDatabaseIntegrity(self, databasePath):
        db = sqlite3.connect(databasePath)
        logger = self.logger
        try:
            with db:
                cursor = db.cursor()
                cursor.execute('pragma quick_check;')
                result = cursor.fetchone()
                if result and result[0] != 'ok':
                    raise Exception(f'Integrity check failed {result[0]}')
                logger.info('SQLite integrity check', {'result': result})
        except sqlite3.Error as e:
            logger.error(f'SQLite error: {e}')

    def moveDatabase(self, temporaryFilePath, finalFilePath, Key, Bucket):
        if os.path.exists(finalFilePath):
            try:
                # self.copyDatabaseContents(temporaryFilePath, finalFilePath)
                pass
            except Exception as error:
                self.logger.error('Something went wrong syncing the database, falling back.',
                                  {'Key': Key, 'Bucket': Bucket, 'error': error})
                try:
                    os.remove(f'{finalFilePath}.json')
                except OSError:
                    pass
                self.logger.info('Deleting old file.', {'Key': Key, 'Bucket': Bucket})
                os.remove(finalFilePath)
                os.rename(temporaryFilePath, finalFilePath)
                self.logger.info('Renaming file to new path.', {'Key': Key, 'Bucket': Bucket})
        else:
            self.logger.info('Renaming file to new path.', {'Key': Key, 'Bucket': Bucket})
            os.rename(temporaryFilePath, finalFilePath)

    def copyDatabaseContents(self, sourcePath, destinationPath):
        sourceDB = sqlite3.connect(sourcePath)
        destinationDB = sqlite3.connect(destinationPath)
        sourceTables = destinationTables = []

        try:
            with sourceDB:
                cursor = sourceDB.cursor()
                cursor.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';")
                sourceTables = [row[0] for row in cursor.fetchall()]

            with destinationDB:
                cursor = destinationDB.cursor()
                cursor.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';")
                destinationTables = [row[0] for row in cursor.fetchall()]

        except sqlite3.Error as e:
            self.logger.error(f'SQLite error: {e}')

        tablesToCreate = [table for table in sourceTables if table not in destinationTables]

        for table in tablesToCreate:
            cursor = sourceDB.cursor()
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE tbl_name = '{table}' AND sql IS NOT NULL;")
            createTable = cursor.fetchone()
            if createTable:
                self.logger.debug(f'Will create new table {table} with SQL {createTable[0]}')
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
        if hasattr(self, 'specifications'):
            return self.specifications

        parser = csv.reader(requests.get(SPECIFICATION_URL).text.splitlines())
        next(parser)  # Skip header row
        specifications = [
            {'collection': spec[0], 'dataset': spec[1]}
            for spec in parser
            if spec[0]
        ]

        self.specifications = specifications
        return specifications

    def copyFileFromS3(self, Key, Bucket, destinationPath):
        try:
            self.s3Client.download_file(Bucket, Key, destinationPath)
            self.logger.info('Finished copying file', {'Key': Key, 'Bucket': Bucket, 'destinationPath': destinationPath})
        except Exception as error:
            self.logger.error(f'Error copying file: {error}', {'Key': Key, 'Bucket': Bucket, 'destinationPath': destinationPath})

    def updateInspectionFile(self):
        files = os.listdir('/mnt/datasets')
        currentInspections = {}

        self.logger.debug('Found files to process for inspections', {'files': files})

        for file in files:
            if not file.endswith('.json') or file == 'inspect-data-all.json':
                continue

            try:
                with open(f'/mnt/datasets/{file}', 'r', encoding='utf-8') as f:

                    inspection = json.load(f)
                    currentInspections.update(inspection)
            except Exception as error:
                self.logger.error('Failed to parse inspection file', {'inspectionFile': f'/mnt/datasets/{file}'})

        with open('/mnt/datasets/inspect-data-all.json', 'w') as f:
            json.dump(currentInspections, f)

        self.logger.info('Refreshed inspections', {'inspections': list(currentInspections.keys())})



# Usage
# eventId = "event_id"
# collection_sync = CollectionSync(eventId)
# Key = "s3_key"
# Bucket = "3_bucket"
# collection_sync.processObject(Key, Bucket)
