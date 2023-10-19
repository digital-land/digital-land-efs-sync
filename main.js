const {mkdir, readdir, readFile} = require('fs/promises');
const CollectionSync = require("./collection-sync");
const {rmSync} = require("fs");


module.exports = {
    handler: async (event, context) => {
        const errors = [];
        const collectionSync = new CollectionSync(event.eventId);

        if (event.wipeExistingFiles) (await readdir('/mnt/datasets'))
            .forEach(file => rmSync(`/mnt/datasets/${file}`, {recursive: true}));

        await mkdir('/mnt/datasets/temporary', {recursive: true});

        for (let i in event.Records) {
            const Bucket = event.Records[i].s3.bucket.name;
            const Key = decodeURIComponent(event.Records[i].s3.object.key.replace(/\+/g, ' '));

            try {
                await collectionSync.processObject({Key, Bucket});
            } catch (error) {
                errors.push(error);
            }
        }

        if (errors.length > 0) {
            const error = new Error();
            error.errors = errors;
            throw error;
        }
    },
};
