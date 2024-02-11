const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const csv = require('csv-parser');

const bigquery = new BigQuery();

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dateFile = gcs.bucket(file.bucket).file(file.name);

    dateFile.createReadStream()
        .on('error', (error) => {
            console.error('Error reading CSV file:', error);
        })
        .pipe(csv())
        .on('data', (row) => {
            transformAndLoad(row, file.name);
        })
        .on('end', () => {
            console.log('End of CSV processing!');
        });
}

async function transformAndLoad(row, fileName) {
    // Transformation according to instructions
    const numericFields = ['airtemp', 'dewpoint', 'pressure', 'windspeed', 'precip1hour', 'precip6hour'];
    numericFields.forEach(field => {
        if (row[field] === '-999.0') {
            row[field] = null; 
        } else {
            row[field] = parseFloat(row[field]) / 10; // Convert to decimal
        }
    });

    // Transform station identifier from file name
    row['station'] = extractStationId(fileName); 

    // Log the transformed row
    console.log(row);

    // Load the transformed data to BigQuery
    try {
        await insertIntoBigQuery(row);
        console.log('Data loaded into BigQuery successfully.');
    } catch (error) {
        console.error('Error loading data into BigQuery:', error);
    }
}

function extractStationId(filename) {
    const parts = filename.split('-');
    if (parts.length > 1) {
        return parts[0]; 
    }
    return null; 
}

async function insertIntoBigQuery(row) {
    const datasetId = 'weather_etl';
    const tableId = 'weather_data';
    const dataset = bigquery.dataset(datasetId);
    const table = dataset.table(tableId);

    await table.insert(row);
}
