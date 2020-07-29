import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

# Uncomment for local zipped data files
# from zipfile import ZipFile

# with ZipFile('data/log-data.zip', 'r') as zip_ref:
#     zip_ref.extractall('data/log-data/')

# with ZipFile('data/song-data.zip', 'r') as zip_ref:
#     zip_ref.extractall('data/')

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: This function creates a spark session.
    Parameters:
    @return: spark       : Spark Session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function reads the song_data from S3 and processes it by using
                 Spark and then loads the resulting tables onto S3 in parquet format.
    Parameters:
            @input: spark - Spark Session
            @input: input_data - location of song_data files
            @input: output_data - S3 location where ouput files are stored
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs/", 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                              'artist_longitude').withColumnRenamed('artist_name', 'name') \
                              .withColumnRenamed('artist_location', 'location') \
                              .withColumnRenamed('artist_latitude', 'latitude') \
                              .withColumnRenamed('artist_longitude', 'longitude')
    
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Description: This function reads the log_data from S3 and processes it by using
                 Spark and then loads the resulting tables onto S3 in parquet format.
    Parameters:
            @input: spark - Spark Session
            @input: input_data - location of song_data files
            @input: output_data - S3 location where ouput files are stored
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
                        .withColumnRenamed('userId', 'user_id') \
                        .withColumnRenamed('firstName', 'first_name') \
                        .withColumnRenamed('lastName', 'last_name')
    
    users_table = users_table.dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour('timestamp'))
    df = df.withColumn('day', dayofmonth('timestamp'))
    df = df.withColumn('week', weekofyear('timestamp'))
    df = df.withColumn('month', month('timestamp'))
    df = df.withColumn('year', year('timestamp'))
    df = df.withColumn('weekday', dayofweek('timestamp'))
    
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_table = time_table.dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + "time/", 'overwrite')

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.artist == song_df.artist_name, how = 'left') \
                        .select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent') \
                        .withColumn('songplay_id', monotonically_increasing_id()) \
                        .withColumnRenamed('userId', 'user_id') \
                        .withColumnRenamed('sessionId', 'session_id') \
                        .withColumnRenamed('userAgent', 'user_agent') \
                        .withColumn('year', year('start_time')) \
                        .withColumn('month', month('start_time'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + 'songplays/', 'overwrite')


def main():
    """
    Description: This function creates the session and then processes song_data and log_data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-srishti/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()