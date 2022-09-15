# Data Lake with Spark and AWS Elastic Map Reduce
This is Udacity Data Engineering Nano Degree fourth project. Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

As a data engineer, I'm tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to. To run Spark in a cluster I will use AWS Elastic Map Reduce service. 

## Project Datasets

Data is located in a public S3 bucket. Here are the S3 links for each:

### Song Data

````s3://udacity-dend/song_data````

### Log Data 

````s3://udacity-dend/log_data````

### Log Data json path

````s3://udacity-dend/log_json_path.json````

## Schema

For this project a star schema with one **fact** table (songplays) and 4 **dimension** tables (artists, songs, users and time) is used. This schema design will minimize the need for JOIN statements, allowing faster reads of the analytics database. 
![Schema](https://user-images.githubusercontent.com/97537153/189656630-3b5373a3-b989-4480-975a-7958938e607f.png)

## Files

• **etl.py**: reads data from S3, processes that data using PySpark code, and writes them back to S3 as parquet files. This script is executed in AWS EMR cluster.

• **dl.cfg**: contains AWS credentials.

## EMR cluster configurations

In this project I used the following configurations when creating the EMR cluster in AWS management console:

• Release: emr-5.20.0 or later

• Applications: Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0

• Instance type: m3.xlarge

• Number of instance: 3

• EC2 key pair: Proceed without an EC2 key pair or feel free to use one if you'd like

After creating this cluster you need to create a new notebook attached to the EMR cluster. In that notebook we execute PySpark code in **etl.py**
