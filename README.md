**Reading multiple CSV files from S3 bucket and store the data in Dynamo DB and retrieve it through web page**

**Backend:**
1. CSV Files Upload to S3  - Required files are uploaded to S3 bucket. 
2. AWS Lambda Function – File upload triggers Lambda function. 
3. Cloud watch – Logs success and error messages.
4. DynamoDB Table – Table is updated with the list of available employees. 

**Frontend:**
API end point created to retrieve the list of available employees from Dynamo DB whenever the static web page hosted on S3 is invoked. 


