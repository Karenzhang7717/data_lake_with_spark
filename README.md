# data_lake_with_spark
Using Spark to build an ETL pipeline for a data lake hosted on S3.

# Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
![image](https://github.com/Karenzhang7717/data_lake_with_spark/blob/main/diagram.png)
# Purpose of the project
The purpose of this project is to build ETL pipeline that extracts the data from S3, processes and transforms them using Spark, and loads the data back into S3 as a set of fact and dimensional tables. This will allow Sparkify's analytics team to study insights in what songs their users are listening to.

# Database schema design
![image](https://github.com/Karenzhang7717/data_lake_with_spark/blob/main/star_schema.png)
Using the song and log datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables:
## Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
users - users in the app \
user_id, first_name, last_name, gender, level \
songs - songs in music database \
song_id, title, artist_id, year, duration \
artists - artists in music database \
artist_id, name, location, lattitude, longitude \
time - timestamps of records in songplays broken down into specific units \
start_time, hour, day, week, month, year, weekday

# How to run
Run the etl.py. It extracts data from S3, process and tramsforms the data using Spark, and load them to parquet files into S3.

# Available Data
Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json \
song_data/A/A/B/TRAABJL12903CDCF1A.json \
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

log_data/2018/11/2018-11-12-events.json \
log_data/2018/11/2018-11-13-events.json
