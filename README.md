# Cmpt732-HealthInsurance

# USA Health Insurance Analytics


## Cmpt 732 Big Data Project


### Team Members: Rovenna Chu, Karthik Srinatha, Rohit Irvisetty


## Description

In this project we aim to perform an exhaustive analysis on the raw American insurance data put up by 'The Centers for Medicare & Medicaid Services (CMS)' on their official website, where we are trying to increase the transparency in the Health Insurance Exchange by depicting the facts and trends by analyzing years of data and also provide facts where Health providing agencies can improve their business strategies which can benefit the customer as well as organization, here we have used Sfu computing cluster and AWS services for this study

## Main Datasets

1) Health Insurance Exchange Public Use Files (Exchange PUFs)

https://www.cms.gov/cciio/resources/data-resources/marketplace-puf


### Supporting Datasets

1) Health searches by US Metropolitan Area, 2005-2017

https://www.kaggle.com/GoogleNewsLab/health-searches-us-county/version/1#

2) Latitudes and Longitudes of American States

https://developers.google.com/public-data/docs/canonical/states_csv

3) Stroke Data

https://www.kaggle.com/fedesoriano/stroke-prediction-dataset


## Tech Stack Used

1) SFU Cluster (Hadoop, HDFS, Spark, SparkML Python): For collecting data, cleaning, ETL and transformation

2) AWS Services:

    a) S3: Used for storing the transformed data.

    b) Redshift: Storing transformed data as external tables

    c) Quicksight: Uses redshift for transformed data and required analytics are displayed

## Analysis Dashboard

Link to Dashboard hosted on aws Quicksight
https://us-west-2.quicksight.aws.amazon.com/sn/accounts/593182245403/dashboards/dbf13b48-bd1f-4c23-8bea-4638d304ce8e

