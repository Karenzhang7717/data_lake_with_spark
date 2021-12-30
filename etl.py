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

def process_song_data(spark, input_data, output_path):
    """
    Process song data and generates two dimension tables, songs and artists. Save the parquet files to S3 Bucket.
    :param spark: The Spark session object.
    :param input_data: Input data path.
    :param output_path: Output data path.
    :return: None
    """
    # get filepath to song data file
    songSchema = R([
    Fld("num_songs",Int()),
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_name",Str()),
    Fld("title",Str()),
    Fld("duration",Dbl()),
    Fld("year",Int()),

    ])

    # read song data file
    df_song = spark.read.json("s3a://udacity-dend/song_data/A/*/*/*.json",schema=songSchema)

    # extract columns to create songs table
    song_columns = ["title", "artist_id", "year", "duration"]

    # extract columns to create songs table
    songs_table = df_song.select(song_columns).dropDuplicates().withColumn("song_id", monotonically_increasing_id())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_path+"songs_table")

    # extract columns to create artists table
    artists_table = df_song.selectExpr('artist_id', 'artist_name as artist', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude')

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_path+"artists_table/")

def process_log_data(spark, input_data, output_path):
    """
    Process log data to get two dimension tables, users and time, as well as one fact table, songplays.
    Save the output as parquet files to S3.
    :param spark: The Spark session object.
    :param input_data: Input data path.
    :param output_path: Output data path.
    :return: None
    """
    # read log data file
    df_log = spark.read.json("s3a://udacity-dend/log_data/*/*/*.json")

    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == "NextSong")

    # extract columns for users table
    users_table = df_log.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level')
    # get unique not null users
    users_table = users_table.dropDuplicates(subset=['user_id'])
    users_table = users_table.where(col('userId').isNotNull())
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_path+"users_table/")

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000)), types.TimestampType())
    df_log = df_log.withColumn("timestamp", get_datetime("ts"))

    # extract columns to create time table
    time_table = df_log.select(
                    col('timestamp').alias('start_time'),
                    hour('timestamp').alias('hour'),
                    dayofmonth('timestamp').alias('day'),
                    weekofyear('timestamp').alias('week'),
                    month('timestamp').alias('month'),
                    year('timestamp').alias('year'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_path + "time_table/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_path + 'songs_table/')

    # read in artist data
    artist_df = spark.read.parquet(output_path + "artists_table/")

    #create songplay_id column for df_log
    df_log = df_log.withColumn('songplay_id', monotonically_increasing_id())

    #create Tempview tables for running sql queries in Hive
    df_log.createTempView("staging_events")
    song_df.createTempView("songs")
    artist_df.createTempView("artists")

    # extract columns from joined song and log datasets to create songplays table; include year and month to allow parquet partitioning
    songplays_table = spark.sql("""
        SELECT
            se.songplay_id,
            se.start_time,
            se.userId as user_id,
            se.level,
            s.song_id,
            s.artist_id,
            se.sessionId as session_id,
            se.location,
            se.userAgent as user_agent,
            year(l.start_time) as year,
            month(l.start_time) as month
        FROM staging_events se
        LEFT JOIN (songs s
            INNER JOIN artists ON song_df.artist_id = artists.artist_id) AS tmp
        ON
            se.song = tmp.title AND
            se.artist = tmp.artist
        WHERE se.page='NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode("overwrite").parquet(output_path + 'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_path = "s3://s3-bucket-kzzz777/"

    process_song_data(spark, input_data, output_path)
    process_log_data(spark, input_data, output_path)


