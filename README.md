# AWS_Webinar_101
Contains resources and guide for the AWS 101 Webinar

### Problem Statement
Design Nearby - a Neighborhood App
1. Each user belongs to a specific neighborhood (defined by coordinates)
1. Whenever, a new user gets added to a neighborhood, an (email) imitation goes to all existing neighbors.

### Solution Approach -
https://drive.google.com/file/d/1EIzrFLpNKlVHjz3vMgvQMMGZA2P7AY-J/view?usp=sharing

### AWS Resources to Be Used -
1. Lambda
1. Glue
1. DynamoDB
1. S3
1. SNS (Simple Notification Service)
1. IAM
1. Athena (optional)
1. Cloudwatch (optional)
1. API-Gateway (optional)

### Steps
1. Go to aws.amazon.com. Create AWS Account. You might require credit/debit card
1. Switch to Service - DynamoDB. Select region as `Europe (Ireland) eu-west-1`.
1. Create the following tables
```buildoutcfg
Table name: nearby_user
Primary key: EMAIL_ID
```
```buildoutcfg
Table name: nearby_neighborhood
Primary key: ID
```
1. For `nearby_user` table, go to `Indexes` - Create Index
```
Primary key: NEARBY_STATUS
Index name: NEARBY_STATUS-index
```
1. Switch to Service - IAM. Create role for Lambda giving access to S3, DynamoDB, Cloudwatch, Glue
1. Go to S3. Create buckets - `<your-name>-nearby-artifcats` and `<your-name>-nearby-user-data`
1. Under artifacts bucket, create folder `scripts` and upload the jobs under it.
1. Create folder `libraries` and upload `utils.egg` and `nearby.zip` 
1. Switch to Lambda. Create Function - invokeAPIs (generic name since it contains set of APIS performing different tasks). Keep the environment as **Python 2.7**
1. Go to section - "Function code". Choose "code entry type" as "Upload a file from S3". Enter the `Object URL` path for `nearby.zip`
1. Click on Dropdown to left of Test - Configure Test Events
1. Create the following events -

`Name: TestAddNewUser`
```buildoutcfg
{
  "item": "user",
  "user": {
    "NAME": "<name>",
    "LAT": 87,
    "LNG": 106,
    "EMAIL_ID": "<email_id>"
  }
}
``` 

`Name: TestAddNewNeighborhood`
```buildoutcfg
{
  "item": "neighborhood",
  "neighborhood": {
    "id": "1",
    "lat": 85,
    "lng": 105
  }
}
```

1. Run the tests to add user and neighborhood
1. Go to IAM - Crete role for DDB giving access to S3, DynamoDB, Cloudwatch.
1. Switch to Glue. Create Job as per the job names under `jobs/` and copy the code as well.
1. Go to Actions -> Edit Job. Point the `Script Path` and `Python library path` (under *Security config..*) to the scripts and utils.egg in Step#3.
1. Scroll Down. Add the following Keys -
```buildoutcfg
key: --data_bucket
value: nearby-user-data 
```
for both jobs
for UserDataExtractionJob
16. Go to Triggers - Add Trigger to run `InvokeSubscriptionJobs` to run on completition of `UserDataExtractionJob`.
17. Run `UserDataExtractionJob`

