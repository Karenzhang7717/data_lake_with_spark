import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
from pyspark.sql import types

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    sets up a Spark Session.
    :return: the Spark sessin object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, output_path):
    """
    Process song data and generates two dimension tables, songs and artists. Save the parquet files to S3 Bucket.
    :param spark: The Spark session object.
    :param output_path: Output data path.
    :return: None
    """
    # get filepath to song data file
    songSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int()),

    ])

    # read song data file, create a staging_songs table first
    staging_songs = spark.read.json("s3a://udacity-dend/song_data/A/A/*/*.json", schema=songSchema)
    staging_songs = staging_songs.withColumn('song_id', monotonically_increasing_id())

    staging_songs.registerTempTable("staging_songs")

    # dedup songs data and create songs table
    songs_table = spark.sql("""SELECT DISTINCT song_id, title, artist_id, year, duration 
                                FROM 
                                    (SELECT 
                                        song_id, title, artist_id, year, duration, ROW_NUMBER() OVER(PARTITION BY song_id ORDER BY title desc) AS sequence 
                                    FROM 
                                        staging_songs) AS ss
                                     WHERE sequence =1   
                            """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_path + "songs_table")

    # dedup artists data and create artists table
    artists_table = spark.sql("""SELECT DISTINCT artist_id, artist, location, latitude, longitude
                                    FROM 
                                        (SELECT 
                                            artist_id, artist_name as artist, artist_location as location, artist_latitude as latitude, artist_longitude as longitude,
                                            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_name desc) AS sequence
                                            FROM staging_songs) AS ss
                                            WHERE sequence=1
                            """)

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_path + "artists_table/")


def process_log_data(spark, output_path):
    """
    Process log data to get two dimension tables, users and time, as well as one fact table, songplays.
    Save the output as parquet files to S3.
    :param spark: The Spark session object.
    :param output_path: Output data path.
    :return: None
    """
    # read log data file
    staging_events = spark.read.json("s3a://udacity-dend/log_data/*/*/*.json")

    # filter by actions for song plays
    staging_events = staging_events.filter(staging_events.page == "NextSong")

    # create songplay_id column for staging_events
    staging_events = staging_events.withColumn('songplay_id', monotonically_increasing_id())

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000)), types.TimestampType())
    staging_events = staging_events.withColumn("timestamp", get_datetime("ts"))

    # create a TempTable for sql queries
    staging_events.registerTempTable("staging_events")

    # dedup users data and create users table
    users_table = spark.sql("""SELECT DISTINCT user_id, first_name, last_name, gender, level
                                FROM 
                                    (SELECT 
                                            userId as user_id, firstName as first_name, lastName as last_name, gender, level,
                                            ROW_NUMBER() OVER (PARTITION BY userId ORDER BY lastName) AS sequence
                                            FROM staging_events) AS se
                                            WHERE sequence =1 AND user_id IS NOT NULL
                                """)

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_path + "users_table/")

    # dedup time data and create time table
    time_table = spark.sql("""SELECT DISTINCT timestamp FROM staging_events WHERE timestamp IS NOT NULL
                            """)

    # extract columns from timestamp
    time_table = time_table.select(
        col('timestamp').alias('start_time'),
        hour('timestamp').alias('hour'),
        dayofmonth('timestamp').alias('day'),
        weekofyear('timestamp').alias('week'),
        month('timestamp').alias('month'),
        year('timestamp').alias('year'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_path + "time_table/")

    # drop Tempviews for songs and artists in case it existed already
    spark.catalog.dropTempView("songs")
    spark.catalog.dropTempView("artists")

    # read in song data to use for songplays table and create table
    song_df = spark.read.parquet(output_path + 'songs_table/')
    song_df.createTempView("songs")

    # read in artist data and create table
    artist_df = spark.read.parquet(output_path + "artists_table/")
    artist_df.createTempView("artists")

    # extract columns from joined song and log datasets to create songplays table; include year and month to allow parquet partitioning
    songplays_table = spark.sql("""
        SELECT DISTINCT
            se.songplay_id,
            se.timestamp as start_time,
            se.userId as user_id,
            se.level,
            tmp.song_id,
            tmp.artist_id,
            se.sessionId as session_id,
            se.location,
            se.userAgent as user_agent,
            year(se.timestamp) as year,
            month(se.timestamp) as month
        FROM staging_events se
        LEFT JOIN (SELECT 
            s.song_id, a.artist_id, s.title, s.duration, a.artist FROM songs s
            INNER JOIN artists a ON s.artist_id = a.artist_id) AS tmp
        ON
            se.song = tmp.title AND
            se.artist = tmp.artist
        WHERE se.page='NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode("overwrite").parquet(output_path + 'songplays_table/')


def main():
    spark = create_spark_session()
    output_path = "s3://s3-bucket-kzzz777/"

    process_song_data(spark, output_path)
    process_log_data(spark, output_path)


