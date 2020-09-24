# Adyen Payment Detail Report Connector
This connector allows you to process automatically generated reports from Adyen. It roughly works as follows.
* You set up a report in your Adyen environment to be automatically generated. This will create automatic notifications
* you can set up a webhook in the Adyen environment to send the notifications to your (cloud function) endpoint
* EXTRACT: The endpoint takes in the notification, checks it, and downloads the CSV file from the URL contained in the notification to a bucket
* TRANSFORM: New files in this bucket are processed by a second function and stripped from unnecessary columns and features and then put in a 'processed' bucket
* LOAD: processed files are then loaded into Big Query by a third function that triggers on new files added to the 'processed' bucket

## Steps
### 1. Adyen Notification Webhook
The following is an example of a webhook sent by Adyen as a POST request. Important are the `REPORT_AVAILABLE` value and the "reason" field which will contain the URL to the CSV file (a dummy is included for testing).
```JSON
{
    "live": "true",
    "notificationItems": [{
        "NotificationRequestItem": {
            "amount": {
                "currency": "EUR",
                "value": 0
            },
            "eventCode": "REPORT_AVAILABLE",
            "eventDate": "2020-09-18T12:43:36+02:00",
            "merchantAccountCode": "yourAccountCode",
            "merchantReference": "testMerchantRef1",
            "pspReference": "test_REPORT_AVAILABLE.csv",
            "reason": "https://URL-to-the-report-as.csv",
            "success": "true"
        }
    }]
}
```

### 2. Extract
The first function takes the above as input from a POST request matching the out.adyen.com domain. You can provide a different domain (e.g. localhost for testing) through an environment variable.

The Adyen server expects the value `[accepted]` to acknowledge that the notification is received. Without this response it will retry a few more times.

A special username and password have been created to download the report from the Adyen domain with basic authentication.

### 3. Transform
GCP Storage allows us to create a read and write stream through which we can efficiently pipe our files. Unfortunately removing columns from CSV files can only ever be done row by row. We remove the file when finished succesfully.

### 4. Load
We append our fresh batch of data to an existing Big Query table and delete the old CSV file when succesful in doing so.

## Setup
The setup in GCP contains the following.
* Cloud Function
    * Trigger: HTTP
    * Target Function: `processAdyenRequest`
    * ENV Variables:
        * DOMAIN
        * STORAGE_BUCKET
        * USERNAME
        * PASSWORD
* Cloud Function
    * Trigger: Storage Bucket (Finalize)
    * Target Function: `parseCSV` 
    * ENV Variables:
        * STORAGE_BUCKET_PROCESSED   
* Cloud Function
    * Trigger: Storage Bucket (Finalize)
    * Target Function: `CSVToBigQuery` 
    * ENV Variables:
        * BQ_DATASET_ID
        * BQ_TABLE_ID
* Storage Bucket 1 (raw data)
* Storage Bucket 2 (processed data)
* Big Query Table