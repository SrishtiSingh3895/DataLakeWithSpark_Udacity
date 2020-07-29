# Project Description

This project aims to create a database (**Sparkify**) and building an ETL pipeline to create this database from the data that has been collected. Since the user base and song database is very large, data lake was built to store and process the data. The raw data resided in an Amazon S3 bucket, in a directory of JSON logs of user activity on the app, as well as a directory with JSON metadata of the songs. The main goal of this database is to enable the analysis of this data to get information about user preferences and to build a recommendation system. This was enabled by building a databse designed as a star schema with songplay table as the fact table which contains all the log data and having several dimension tables which store details of users, songs, artists and time the songs were played. The data is extracted from S3, loaded onto the EMR cluster or another S3 bucket and then transformed into a set of facts and dimensiona tables using Spark for their analytics team to continue finding insights in what songs the users are listening to.

# Database Schema

A star schema based design was created for the Sparkify database to have optimized queries to get details of songs played by users. Star schema is suited for this purpose since we have a log file which contains all the metrics and then we can have details about the attributes in the fact table linked to the dimension tables. Moreover the data can be partitioned based on one or more of the attributes and stored on the S3 buckets.

## Star Schema

### Fact table

#### `songplays`

This table was created using the logs that were collected when users played any song.

|*Attributes*|songplay_id|start_time|user_id|level|song_id|artist_id|session_id|location|user_agent|
|--|--|--|--|--|--|--|--|--|--|

Primary Key: songplay_id

### Dimension Tables

#### `users`

This table contains the details of each user.

|*Attributes*|user_id|first_name|last_name|gender|level|
|--|--|--|--|--|--|

Primary Key: user_id

#### `songs`

This table contains the details for each of the songs.

|*Attributes*|song_id|title|artist_id|year|duration|
|--|--|--|--|--|--|

Primary Key: song_id

#### `artists`

The table to store artist details.

|*Attributes*|artist_id|name|location|latitude|longitude|
|--|--|--|--|--|--|

Primary Key: artist_id

#### `time`

This table stores the start time when a song was played and all the attributes associated to that time stamp.

|*Attributes*|start_time|hour|day|week|month|year|weekday|
|--|--|--|--|--|--|--|--|

Primary Key: start_time

# ETL pipeline

The steps followed to build the ELT pipeline are as follows:

- Songs data files were extracted from the S3 bucket and entire data was loaded onto the S3 bucket or the EMR cluster where it has to be processed.

- Log data files were extracted from the S3 bucket and entire data was loaded onto the S3 bucket or the EMR cluster where it has to be processed.

- These data files were then used to create all the tables in the schema.

- Songs as well as log files were used to extract the required data to be stored in songplays table.

- Events log files were used to transform the data into the users and time table.

- Songs data files were used to tranform the data into the songs and artists table.

The data was partitioned based on 'year' and 'artist_id' for the songs table and based on 'year' and 'month' for the time and songplays tables.
Entire databased was created using parquet files.

# Data Analysis

The databse can be used to perform data analysis and gather information about the user preferences.

Example of a few queries that can be performed on the EMR cluster are:

- Analyze the number of users that are paying for the service and the number of users who are using the platform for free.
    - SELECT level, count(*) FROM users GROUP BY level;
    - This gives the output as 82 free users and 22 paid users.
- The top users, songs and artists can also be extracted using the songplays table to get the information about who are the most frequent users, what songs people play most often and the artists whom users listens to the most.
    - SELECT song_id, count(*) AS c FROM songplays GROUP BY song_id ORDER BY c desc LIMIT 10;
    - The above query will give the top 10 frequently played songs.
    - Similarlly, details of users, artists, etc acn also be achieved and analyzed.

# File description

The repository contains the following files/folders:

- dl.cfg
This file contains the IAM configurations needed to access (read and write) the S3 buckets.y

- etl.py
This file can be executed using the "python3 etl.py" command on the workspace or by having a copy of the file on the EMR cluster and then notebook can be used to run all the commands. This file contains the entire ETL pipline required to extract, transform and load the data. Executing this file will read the entire data from the input S3 bucket, processes the data using Spark and then stores all the tables in the database schema to the output S3 bucket.

- README.md
This file contains the description about the project, database, databse schema and the ETL pipeline.

- data/
This folder contains sample data, which was used to test the code.