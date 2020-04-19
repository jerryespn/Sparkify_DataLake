# Sparkify - Data Engineer Nanodegree program SQL Data Model Project
# By JGEL
# April 2020

# Library Import
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# Reading configuration file for AWS
config = configparser.ConfigParser()
config.read('conf/dl.cfg')

# AWS access keys taken from configuration file
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# SparkContext Initialize
def create_spark_session():
    '''
    Initialize the SparkSession for the application, includes AWS package to use it
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# Process Song Function
def process_song_data(spark, input_data, output_data):
    '''
    This function extracts and process the song and artist data from S3 into DataFrames,
    then transforms, and loads back into S3.
    :input_data -> S3 Bucket where data comes
    :output_data -> S3 Bucket where data is stored
    '''
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")
    
    # extract columns to create songs table
    song_table = spark.sql ("SELECT DISTINCT song_id, title as song_title, artist_id, year, duration FROM songs")
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/song/songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artist_table = spark.sql ("SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM songs")
    
    # write artists table to parquet files
    artist_table.write.parquet(path = output_data + "/artist/artists.parquet", mode = "overwrite")

# Process Log Function
def process_log_data(spark, input_data, output_data):
    '''
    This function extracts and process the log data from S3 into DataFrames,
    :input_data -> S3 Bucket where data comes
    :output_data -> S3 Bucket where data is stored
    '''
    # get filepath to log data file
    log_data = input_data + "/log_data/*"

    # read log data file
    df2 = spark.read.json(log_data)
    df2.createOrReplaceTempView("staging_events")
    
    # filter by actions for song plays
    # Filtering by NextSong
    filtered_songplays_query = spark.sql ("SELECT *, cast(ts/1000 as Timestamp) AS timestamp FROM staging_events WHERE page = 'NextSong'")
    # Staging Events Filtered and time formated
    filtered_songplays_query.createOrReplaceTempView("staging_events")

    # extract columns for users table    
    users_table = spark.sql ("""SELECT a.userId, a.level, a.firstName, a.lastName, a.gender FROM staging_events a INNER JOIN (SELECT userId, MAX(ts) AS TS FROM staging_events GROUP BY userId, page) b ON a.userId = b.userId AND a.ts = b.ts""")
    # Extracting unique users
    clean_users_table = users_table.dropDuplicates(['userId','level'])

    # write users table to parquet files
    clean_users_table.write.parquet(path = output_data + "/users/users.parquet", mode = "overwrite")

    # extract columns to create time table
    time_table = spark.sql("""SELECT DISTINCT timestamp AS start_time, hour(timestamp) AS hour, day(timestamp) AS day, weekofyear(timestamp) AS week, month(timestamp) AS month, year (timestamp) AS year, weekday(timestamp) AS weekday FROM staging_events""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "/time/time.parquet", mode = "overwrite")

    # read in song data to use for songplays table
    s_df = spark.read.parquet(output_data + "/song/*")
    s_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT a.timestamp AS start_time, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, a.location, a.userAgent, year(a.timestamp) AS year, month(a.timestamp) AS month FROM staging_events AS a INNER JOIN songs AS b ON a.song = b.song_title""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = output_data + "/songplays/songplays.parquet", mode = "overwrite")


def main():
    """
    Main Function where the etl process is executed, every function is called from here
    """
    # Initiate Spark Session
    spark = create_spark_session()
    
    # Data files
    # Root Data Path
    # Uncomment below line for AWS S3
    #input_data = "s3a://udacity-dend"
    # Uncomment below line for local files
    input_data = "data"

    # Warehouse
    # Root WH
    # Uncomment below line for AWS S3
    #output_data = "s3a://jerryespn-project-out"
    # Uncomment below line for local files
    output_data = "spark-warehouse"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()