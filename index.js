/**
 * Parses the notification webhook from Adyen (basic auth) and stores the file in a GCP bucket
 * ENV: DOMAIN (default: https://out.adyen.com)
 * ENV: STORAGE_BUCKET
 * ENV: REPORT_USER
 * ENG: PASSWORD
 *
 * @param {Object} req Cloud Function request context.
 * @param {Object} res Cloud Function response context.
 */
exports.processAdyenRequest = (req, res) => {
    const { Storage } = require('@google-cloud/storage');
    const fs = require('fs');
    const https = require('https');

    const STORAGE_BUCKET = process.env.STORAGE_BUCKET || "etl-adyen";

    // Set CORS headers for preflight requests, allows POSTS from DOMAIN or adyen.com for default
    res.set('Access-Control-Allow-Origin', process.env.DOMAIN || "https://out.adyen.com");

    const httpConfig = {
        headers: { 'Authorization': 'Basic ' + Buffer.from(process.env.REPORT_USER + ':' + process.env.PASSWORD).toString('base64') }
    };

    if (req.method === 'POST') {
        const data = req.body;
        console.log("Incoming notification: " + JSON.stringify(data));

        data.notificationItems.map(async(i) => {
            if (Object.keys(i).indexOf("NotificationRequestItem") == -1) {
                return
            } else {
                i = i.NotificationRequestItem
            }

            if (i.eventCode === "REPORT_AVAILABLE" && i.pspReference.indexOf("received_payments_report") > -1) {
                try {
                    console.log("Downloading file: " + i.pspReference);

                    const filename = i.pspReference;
                    const file = fs.createWriteStream(`/tmp/${filename}`);

                    await https.get(i.reason, httpConfig, response => {
                        console.log(`Fetched url ${i.reason} with response '${response.statusCode} ${response.statusMessage}'`);

                        response.pipe(file);

                        file.on('finish', function() {
                            file.close((err) => {
                                if (err)
                                    console.error('Failed to close file: ' + err);
                                else {
                                    console.log(`File ${i.reason} saved successfully`);

                                    console.log(`Uploading file: ${i.pspReference} to storage bucket ${STORAGE_BUCKET}`);
                                    const storage = new Storage();
                                    storage.bucket(STORAGE_BUCKET).upload(`/tmp/${filename}`, {
                                        metadata: {
                                            cacheControl: 'no-cache',
                                        },
                                    });
                                }
                            });
                        });
                    }).on('error', err => {
                        throw err
                    })
                } catch (e) {
                    console.error(e)
                    res.status(400).send("Error downloading from URL")
                }
            }
        });
        res.status(200).send("[accepted]");
    } else {
        res.status(400).send("Invalid request method")
    }
}

/**
 * Parse CSV file to Big Query table on new file in bucket 
 * with processed files (google.storage.object.finalize).
 * 
 * ENV: STORAGE_BUCKET_PROCESSED
 *
 * @param {object} file The Cloud Storage file metadata.
 * @param {object} context The event metadata.
 */
exports.parseCSV = async(fileEvent, context) => {
    const { Storage } = require('@google-cloud/storage');
    const csv = require('csv');

    const STORAGE_BUCKET_PROCESSED = process.env.STORAGE_BUCKET_PROCESSED || "etl-adyen-processed"

    const storage = new Storage();

    console.log(`Processing file '${fileEvent.name}' from bucket '${fileEvent.bucket}'`);

    await storage.bucket(fileEvent.bucket).file(fileEvent.name).createReadStream()
        .pipe(csv.parse({ delimiter: ',', columns: true }))
        .pipe(csv.transform((input) => {
            // Delete these columns for each row so we don't have columns we don't want (e.g. email addresses)
            [
                "Company Account",
                "Merchant Reference",
                "TimeZone",
                "Risk Scoring",
                "Shopper Name",
                "Shopper PAN",
                "Shopper IP",
                "Issuer Name",
                "Issuer Id",
                "Issuer City",
                "Issuer Country",
                "Acquirer Response",
                "Authorisation Code",
                "Shopper Email",
                "Shopper Reference",
                "3D Directory Response",
                "3D Authentication Response",
                "CVC2 Response",
                "AVS Response",
                "Billing Street",
                "Billing House Number / Name",
                "Billing City",
                "Billing Country",
                "Billing Postal Code / ZIP",
                "Billing State / Province",
                "Delivery Street",
                "Delivery House Number / Name",
                "Delivery City",
                "Delivery Postal Code / ZIP",
                "Delivery State / Province",
                "Delivery Country",
                "Acquirer Reference",
                "Payment Method Variant",
                "Raw acquirer response",
                "Reserved4",
                "Reserved5",
                "Reserved6",
                "Reserved7",
                "Reserved8",
                "Reserved9",
                "Reserved10"
            ].forEach(e => {
                delete input[e];
            })

            Object.keys(input).forEach(k => {
                // Replace whitespaces with _ for BQ headers
                if (k !== k.replace(" ", "_")) {
                    input[k.replace(" ", "_")] = input[k];
                    delete input[k];
                }
            })
            return input;
        }))
        .pipe(csv.stringify({ header: true }))
        .pipe(storage
            .bucket(STORAGE_BUCKET_PROCESSED)
            .file(fileEvent.name)
            .createWriteStream()) // Store processed file in new bucket
        .on('finish', () => {
            /*
            Optionally delete the file if it's been succesfully processed
            storage.bucket(fileEvent.bucket).file(fileEvent.name).delete().then((data) => {
                const response = JSON.stringify(data[0]);
                console.log(`Processed and deleted file: "${fileEvent.name}" with response: ${JSON.stringify(response)}`);
                
            })
            */
            console.log(`Finished processing: "${fileEvent.name}"`);
        })
        .on('error', (err) => {
            console.log(err);
        });
}

/**
 * Parse CSV file to Big Query table on new file in bucket 
 * with processed files (google.storage.object.finalize)
 * and delete the original when done.
 * 
 * ENV: BQ_DATASET_ID
 * ENV: BQ_TABLE_ID
 *
 * @param {object} fileEvent The Cloud Storage file metadata.
 * @param {object} context The event metadata.
 */
exports.CSVToBigQuery = async(fileEvent, context) => {
    // Import the Google Cloud client libraries
    const { BigQuery } = require('@google-cloud/bigquery');
    const { Storage } = require('@google-cloud/storage');

    // Instantiate clients
    const bigquery = new BigQuery();
    const storage = new Storage();

    const metadata = {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        schema: {
            fields: [
                { name: 'Currency', type: 'STRING' },
                { name: 'Amount', type: 'FLOAT' },
                { name: 'Type', type: 'STRING' },
                { name: 'Merchant_Account', type: 'STRING' },
                { name: 'Psp_Reference', type: 'INTEGER' },
                { name: 'Payment_Method', type: 'STRING' },
                { name: 'Creation_Date', type: 'DATETIME' },
                { name: 'Shopper_Interaction', type: 'STRING' },
                { name: 'Shopper_Country', type: 'STRING' }
            ],
        },
        fieldDelimiter: ',',
        writeDisposition: 'WRITE_APPEND',
        createDisposition: 'CREATE_IF_NEEDED'
    };

    // Load data from a Google Cloud Storage file into the table
    const [job] = await bigquery
        .dataset(process.env.BQ_DATASET_ID)
        .table(process.env.BQ_TABLE_ID)
        .load(storage.bucket(fileEvent.bucket).file(fileEvent.name), metadata);

    // load() waits for the job to finish
    console.log(`Job ${job.id} completed from file '${fileEvent.name}'.`);

    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
        throw errors;
    } else {
        // If no errors, optionally delete the file from storage
        // storage.bucket(fileEvent.bucket).file(fileEvent.name).delete();
    }
}