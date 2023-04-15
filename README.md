# Creating a Data Warehouse ELT S3 Redshift Solution on AWS for the Music Streaming Service Sparkify


## Table of Contents
1. [Project Description](#Project-Description)
2. [Project Motivation](#Project-Motivation)
3. [Setup](#Installation)
4. [Steps/File Descriptions](#File-Descriptions)

## Project Description 

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Motivation

My goal was building an ELT pipeline that, extracts (E) their data from S3, stages (L) them in Redshift, and transforms (T) data into a set of dimensional tables 
for allow the analytics team to continue finding insights into what songs their users are listening to.

## Setup / Requirements

### Requirements - AWS 

* Create IAM user (dwhadmin) in the AWS management console 

### Requirements - Python

* Python(https://www.python.org/downloads/)
* boto3 package (AWS) 
* psycopg2 package (PostgreSQL / Redshift)

## Steps / File Descriptions

0. Add AWS KEY and SECRET of the dwhadmin user into dwh.cfg
1. Run the file step_01_create_aws_cluster.py to create an AWS Redshift data warehouse
2. Add DWH_ENDPOINT and DWH_ROLE_ARN into dwh.cfg 
3. Run the file step_02_show_status_aws_cluster.py to verify the connection to the AWS Redshift cluster
4. Run the file step_03_create_tables to create the tables in AWS Redshift
5. Run the file step_04_etl.py to load data from S3 to staging tables and then to fact and dimensional tables on AWS Redshift
6. Run the file step_05_etl_result.py to view an Example Output from the fact and dimensional tables on AWS Redshift
7. Run the file step_06_delete_aws_cluster.py to delete the Redshift Cluster on AWS

## Project Structure

```
AWS_ELT_Data_Warehouse_S3_2_Redshift_Sparkify/
 ├── dwh.cfg                            Configuration file for setting up S3 sources & Redshift credentials
 ├── README.md                           Documentation of the project
 └── sql_queries.py                      Python file containing all SQL statements for ELT process
 ├── step_01_create_aws_cluster.py       Python script to create an AWS Redshift data warehouse
 ├── step_02_show_status_aws_cluster.py  Python script to verify the connection to the AWS Redshift cluster
 ├── step_03_create_tables.py            Python script to create the tables in AWS Redshift
 ├── step_04_etl.py                      Python script to load data from S3 to staging, fact and dimensional tables in AWS Redshift
 ├── step_05_etl_result.py               Python script to view an Example Output from the fact and dimensional tables on AWS Redshift
 ├── step_06_delete_aws_cluster.py       Python script to delete the Redshift Cluster on AWS
