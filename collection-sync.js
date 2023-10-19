const {basename} = require('path');
const {readdir, readFile, writeFile, unlink, mkdir, rename} = require('fs/promises');
const {existsSync} = require('fs');
const {createLogger, format, transports} = require("winston");
const {parse} = require("csv-parse");
const {get} = require("axios");
const {Database} = require("sqlite3");
const {createWriteStream} = require("fs");
const {S3} = require("aws-sdk");

const SPECIFICATION_URL = 'https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv';
const LOG_LEVEL = 'debug';

module.exports = class CollectionSync {
    constructor(eventId) {
        this.s3Client = new S3();
        this.logger = createLogger({
            level: LOG_LEVEL,
            format: format.json(),
            defaultMeta: {service: 'efs-sync-collection', event: eventId},
            transports: [new transports.Console()],
        });
    }

    async processObject({Key, Bucket}) {
        this.logger.info('Processing new object', {Key, Bucket});
        const fileName = basename(Key);

        if (await this.shouldSync(Key)) {
            const temporaryFilePath = `/mnt/datasets/temporary/${fileName}`;
            const finalFilePath = `/mnt/datasets/${fileName}`;

            if (existsSync(temporaryFilePath)) await unlink(temporaryFilePath);

            await this.copyFileFromS3(Key, Bucket, temporaryFilePath);

            await this.checkDatabaseIntegrity(temporaryFilePath);

            await this.moveDatabase(temporaryFilePath, finalFilePath, Key, Bucket);

            if (![
                "digital-land-builder/dataset/digital-land.sqlite3",
                "entity-builder/dataset/entity.sqlite3",
            ].includes(Key)) {
                // TODO: When building entity and digital-land, output json files,
                //   remove this if statement and always copy the json file to ensure
                //   datasette stats are up to date.
                await this.copyFileFromS3(`${Key}.json`, Bucket, `${finalFilePath}.json`);
            }

            await this.updateInspectionFile();
        } else {
            this.logger.info('Object is not subject to sync, skipping.', {Key, Bucket});
        }
    }

    async shouldSync(Key) {
        return [
                "digital-land-builder/dataset/digital-land.sqlite3",
                "entity-builder/dataset/entity.sqlite3",
            ].includes(Key) ||
            !!(await this.getSpecifications())
                .find(cd => Key === `${cd.collection}-collection/dataset/${cd.dataset}.sqlite3`);
    }

    async checkDatabaseIntegrity(databasePath) {
        const db = new Database(databasePath);
        const logger = this.logger;
        await new Promise((resolve, reject) => {
            db.get('pragma quick_check;', function (error, result) {
                if (error || result?.quick_check !== 'ok') {
                    return reject(error || new Error(`Integrity check failed ${result?.quick_check}`));
                }
                logger.info('SQLite integrity check', {result});
                resolve();
            });
        });
        db.close();
    }

    async moveDatabase(temporaryFilePath, finalFilePath, Key, Bucket) {
        if (existsSync(finalFilePath)) {
            // try {
            //     await this.copyDatabaseContents(temporaryFilePath, finalFilePath);
            // } catch (error) {
            //     this.logger.error('Something went wrong syncing the database, falling back.', {Key, Bucket, error});
                try {
                    await unlink(`${finalFilePath}.json`);
                } catch (e) {
                }

                this.logger.info('Deleting old file.', {Key, Bucket});
                await unlink(finalFilePath);
                await rename(temporaryFilePath, finalFilePath);
                this.logger.info('Renaming file to new path.', {Key, Bucket});
            // }
        } else {
            this.logger.info('Renaming file to new path.', {Key, Bucket});
            await rename(temporaryFilePath, finalFilePath);
        }
    }

    async copyDatabaseContents(sourcePath, destinationPath) {
        const sourceDB = new Database(sourcePath);
        const logger = this.logger;
        await new Promise((resolve, reject) => {
            sourceDB.all('SELECT name FROM sqlite_schema WHERE type =\'table\' AND name NOT LIKE \'sqlite_%\';', function (error, tables) {
                const sourceTables = tables.map(table => table.name);
                logger.debug('Found tables in source database.', {tables: sourceTables});

                const destinationDB = new Database(destinationPath);
                destinationDB.all('SELECT name FROM sqlite_schema WHERE type =\'table\' AND name NOT LIKE \'sqlite_%\';', function (error, tables) {
                    const destinationTables = tables.map(table => table.name);
                    logger.debug('Found tables in destination database.', {tables: destinationTables});

                    // TODO: Currently only supports creating new tables,
                    //  if a table schema changes this will not be updated
                    const tablesToCreate = sourceTables.filter(table => !destinationTables.includes(table));
                    logger.debug('Tables to create in the destination database.', {tables: tablesToCreate});

                    Promise.all(tablesToCreate.map(table => new Promise((resolve) => {
                        sourceDB.get(
                            `SELECT sql
                             FROM sqlite_master
                             WHERE tbl_name = '${table}'
                               AND sql IS NOT NULL`,
                            function (error, createTable) {
                                logger.debug(`Will create new table ${table} with SQL ${createTable.sql}`)
                                resolve(createTable.sql);
                            });

                    }))).then((createTablesSqlStatements) => {
                        destinationDB.serialize(function() {
                            for (const createTable of createTablesSqlStatements) {
                                destinationDB.exec(createTable);
                            }
                            for (const destinationTable of destinationTables) {
                                destinationDB.exec(`DELETE FROM ${destinationTable}`);
                            }
                        });
                        const statement = `
                            ATTACH '${sourcePath}' AS src;
                            ${sourceTables.map(table => `INSERT INTO ${table}
                                                         SELECT *
                                                         FROM src.${table};`).join(';')};
                        `;

                        destinationDB.exec(statement, function (error) {
                            logger.debug('Sync copy outcome', {error, changes: this.changes});
                            destinationDB.all('SELECT name FROM sqlite_schema WHERE type =\'table\' AND name NOT LIKE \'sqlite_%\';', function (error, tables) {
                                logger.debug('New tables in destination', {tables});
                            });

                            if (error) return reject(error);
                            resolve();
                        });
                    });
                });
            });
        });
        sourceDB.close();
    }

    async getSpecifications() {
        if (this.specifications) return this.specifications;

        await new Promise((resolve, reject) => {
            const parser = parse({delimiter: ','});
            const records = [];

            parser.on('readable', () => {
                let record;
                while ((record = parser.read()) !== null) {
                    records.push(record);
                }
            });

            parser.on('error', (error) => {
                reject(error);
            });

            parser.on('end', () => {
                const fields = records.shift();
                const collectionField = fields.indexOf('collection');
                const datasetField = fields.indexOf('dataset');

                this.specifications = records
                    .map(spec => ({collection: spec[collectionField], dataset: spec[datasetField]}))
                    .filter(spec => ![""].includes(spec.collection));

                resolve();
            });

            get(SPECIFICATION_URL)
                .then(({data}) => {
                    parser.write(Buffer.from(data));
                    parser.end();
                });
        });

        return this.specifications;
    }

    async copyFileFromS3(Key, Bucket, destinationPath) {
        const DatasetSource = this.s3Client.getObject({Bucket, Key}).createReadStream();
        const DatasetDestination = createWriteStream(destinationPath, {
            flags: 'w',
            encoding: null,
        });

        createWriteStream(destinationPath, {encoding: null});
        await new Promise((resolve, reject) => {
            DatasetSource.on('end', () => {
                DatasetDestination.close();
                this.logger.info(`Finished copying file`, {Key, Bucket, destinationPath});
                resolve();
            });

            DatasetSource.on('error', (error) => {
                DatasetDestination.close();
                this.logger.error(`Error copying file (source)`, {error, Key, Bucket, destinationPath});
                reject(error);
            });

            DatasetDestination.on('error', (error) => {
                DatasetDestination.close();
                this.logger.error(`Error copying file (destination)`, {error, Key, Bucket, destinationPath});
                reject(error);
            });

            DatasetDestination.on('open', () => {
                DatasetSource.pipe(DatasetDestination);
            });
        });
    }

    async updateInspectionFile() {
        const files = await readdir('/mnt/datasets');
        let currentInspections = {};

        this.logger.debug('Found files to process for inspections', {files});

        for (const file of files) {
            if (!file.endsWith('.json') || file === 'inspect-data-all.json') continue;

            // this.logger.debug('Processing file', {file});

            try {
                const fileContents = await readFile(`/mnt/datasets/${file}`, 'utf-8');
                const inspection = JSON.parse(fileContents);
                currentInspections = {...currentInspections, ...inspection};
            } catch (error) {
                this.logger.error('Failed to parse inspection file', {inspectionFile: `/mnt/datasets/${file}`});
            }
        }

        await writeFile('/mnt/datasets/inspect-data-all.json', JSON.stringify(currentInspections))

        this.logger.info('Refreshed inspections', {inspections: Object.keys(currentInspections)});
    }
}
