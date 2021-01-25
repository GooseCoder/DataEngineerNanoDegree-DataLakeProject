import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    It creates or loads an Spark session, it adds the spark entrypoints and the hadoop framework.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads song_data from S3 and processes it by extracting the songs and artist data 
    into specific dataframes they represent tables in the target star schema model. 
    These tables are then again loaded back to S3 into the output folder of the output_data parameter.
    
    Parameters:
    :param spark: spark session
    :param input_data: input data path
    :param output_data: output data path
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_df = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
        .dropDuplicates(["song_id"])

    # write songs table to parquet files partitioned by year and artist
    songs_df.write.mode("overwrite") \
        .partitionBy("year", "artist_id") \
        .parquet(os.path.join(output_data, "songs"))

    print("songs parquet files stored.")
    
    # extract columns to create artists table
    artists_df = df[
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ].dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_df.write.parquet(os.path.join(output_data, "artists"), "overwrite")
    print("artists parquet files stored.")

def process_log_data(spark, input_data, output_data):
    """
    Load data from the song_data files, extract columns
    from the songs and artist data and stores them in parquet 
    format on the output folder.
    Parameters
        :param spark: spark session
        :param input_data: input data path
        :param output_data: output data path
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_df = df['userId', 'firstName', 'lastName', 'gender', 'level'] \
        .dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_df.write.mode("overwrite").parquet(os.path.join(output_data, "users"))
    print("users parquet files stored.")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("dt", get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_df = df.select(
        col('dt').alias('start_time'),
        hour('dt').alias('hour'),
        dayofmonth('dt').alias('day'),
        weekofyear('dt').alias('week'),
        month('dt').alias('month'),
        year('dt').alias('year') 
    ).dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_df.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(os.path.join(output_data, "time"))
    
    print("time parquet files stored.")
    
    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    df = df.join(songs_df, songs_df.title == df.song)

    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    df = df.join(artists_df, artists_df.artist_name == df.artist)

    df = df.join(time_df, time_df.start_time == df.dt)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = df.select(
        col('start_time'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month'),
        col('userId').alias('user_id'),
        col('level').alias('level')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(os.path.join(output_data, "song_plays"))
    
    print("song_plays parquet files stored.")


def main():
    """
    The main function executes the whole application
    - Start the Spark session
    - Extract songs and events data from S3
    - Transform it into a star schema 
    - Store the result into S3 in Parquet format
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://goose-sparkify-dend/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
