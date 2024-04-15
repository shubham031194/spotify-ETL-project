# spotify-ETL-project

### Introduction
In this project we will build and ETL (extract - transform - Load) pipeline using the spotify API on AWS. The pipeline will retrive data from the spotify API, trnasform it to desired format and load it to data storage.

### Architecture
![Pipeline Architecture Daigram.](https://github.com/shubham031194/spotify-ETL-project/blob/df9ae4054621b25e9a05e127ec4ae51522fe4eb5/ETL_pipeline.png)

### About Dataset/API
The spotify API is used to retrive data for top 50 global songs [Spotify API Doc](https://developer.spotify.com/documentation/web-api) .
The data contains information about songs, music artists and albums.

### Services Used
1. **S3 (Simple Storage Service):** Amazon Simple Storage Service (Amazon S3) is an object storage service offering industry-leading scalability, data availability, security, and performance.
2. **AWS Lambda:** AWS Lambda is a compute service that runs your code in response to events and automatically manages the compute resources, making it the fastest way to turn an idea into a modern, production, serverless applications.
3. **Cloud Watch:** Amazon CloudWatch is a service that monitors applications, responds to performance changes, optimizes resource use, and provides insights into operational health. By collecting data across AWS resources, CloudWatch gives visibility into system-wide performance and allows users to set alarms, automatically react to changes, and gain a unified view of operational health.
4. **Glue Crawler:** The CRAWLER creates the metadata that allows GLUE and services such as ATHENA to view the S3 information as a database with tables. That is, it allows you to create the Glue Catalog.
5. **Amazon Athena:** Amazon Athena is a serverless, interactive analytics service built on open-source frameworks, supporting open-table and file formats. Athena provides a simplified, flexible way to analyze petabytes of data where it lives.

###Install Packages
```
pip install pandas
pip install spotipy
```

### Project Execuation Flow
Extract Data from spotify API --> Lambda Trigger (every 1 hour) --> Run extract code --> store raw data --> Trigger Transform Funcation --> Transform data and Load It --> Query using Athena.
