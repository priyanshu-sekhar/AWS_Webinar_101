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
2. Switch to Service - IAM. Create role for Lambda giving access to S3, DynamoDB, Cloudwatch, Glue
3. Switch to Lambda. Create Function - invokeAPIs (generic name since it contains set of APIS performing different tasks). Keep the environment as **Python 2.7**
4. Create folder **utils**. Copy DDBCommonUtils, DateTimeUtils, SNSCommonUtils
5. Copy apis/api_handlers.py code
6. Click on Dropdown to left of Test - Configure Test Events
7. Create the following events -

`Name: TestAddNewUser`
```buildoutcfg
{
  "item": "person",
  "person": {
    "name": "<name>",
    "lat": 87,
    "lng": 106,
    "email_id": "<email_id>",
    "user_id": "2"
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

8. Run the tests to add user and neighborhood
9. Go to S3. Create buckets - `nearby-artifcats` and `nearby-user-data`
10. Under `nearby-artifacts`. Create folder `scripts` and upload the jobs under it.
11. Create folder `libraries` and upload `utils.egg`
12. Go to IAM - Crete role for DDB giving access to S3, DynamoDB, Cloudwatch.
13. Switch to Glue. Create Job as per the job names under `jobs/` and copy the code as well.
14. Go to Actions -> Edit Job. Point the `Script Path` and `Python library path` (under *Security config..*) to the scripts and utils.egg in Step#3.
15. Scroll Down. Add the following Keys -
```buildoutcfg
key: --data_bucket
value: nearby-user-data 
```
for both jobs, and
```buildoutcfg
key: --date_offset
value: 0
```
for UserDataExtractionJob
16. Go to Triggers - Add Trigger to run `InvokeSubscriptionJobs` to run on completition of `UserDataExtractionJob`.
17. Run `UserDataExtractionJob`

