const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const csv = require('csv-parser');

const bigquery = new BigQuery();
const datasetId = 'cit-41200-scrapbook';
const tableId = 'weather_data';

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dateFile = gcs.bucket(file.bucket).file(file.name);

    dateFile.createReadStream()
        .on('error', (error) => {
            console.error('Error reading CSV file:', error);
        })
        .pipe(csv())
        .on('data', (row) => {
            writeToBQ(row);
        })
        .on('end', () => {
            console.log('End of CSV processing!');
        });
}

async function writeToBQ(row) {
    const rows = [row];

    try {
        await bigquery
            .dataset(datasetId)
            .table(tableId)
            .insert(rows);

        rows.forEach(row => {
            console.log(`Inserted: ${JSON.stringify(row)}`);
        });
    } catch (err) {
        console.error(`Error: ${err}`);
    }
}
