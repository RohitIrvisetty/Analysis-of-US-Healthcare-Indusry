## Cmpt 732 Big Data Project
### Team Members: Rovenna Chu, Karthik Srinatha, Rohit Irvisetty
## Running

This repo is developed to run on sfu cluster on any user id's, where the automation script take cares of the entire process
After cloning the repo in cluster use the following commands
```
cd cmpt732-healthinsurance/scripts
bash automate_ksa166.sh 
```
The automation scripts takes care of Data Scrapping, Collecting, Cleaning and Transformation.
After automate_ksa166.sh has completely finished it job

We can now use the following command to copy the transformed data from hdfs to local filesytem and the upload it to s3 bucket for further analysis
```
hdfs dfs -copyToLocal america_insurance/s3/* s3/
```
Steps to be followed after transformed data
1) Create and S3 bucket and a Redshift cluster in AWS account
2) Then upload the transformed s3 folder to the s3 bucket
3) Use the redshift_schemas.sql given in the repo to create the external tables in the redshift, please make sure to change the bucket name of your choice and then create the external tables 
4) Use the external tables created in Redshift in Quicksight for analysis