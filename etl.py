import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Function that creates Spark session in AWS EMR cluster."""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
     """Extract necessary data from json song_data files 
        to songs and artists dimensional tables.

    Args:
        spark: spark session
        input_data: directory to read data from S3 bucket
        output_data: directory to write output parquet files """
        
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    #Create temp view for spark.sql
    df.createOrReplaceTempView("df")

    # extract columns to create songs table
    songs_table = spark.sql("""
                                    SELECT DISTINCT song_id, 
                                                    title, 
                                                    artist_id, 
                                                    artist_name,
                                                    year, 
                                                    duration
                                    FROM df
                            """)
    
    
    songs_table.write.partitionBy("year", "artist_name").mode('overwrite').parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                  SELECT DISTINCT artist_id, 
                                          artist_name as name, 
                                          artist_location as location, 
                                          artist_latitude as latitude, 
                                          artist_longitude as longitude
                                  FROM df
                              """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    """Extract necessary data from json log_data files 
        to users dimensional table and songplays fact table.

    Args:
        spark: spark session
        input_data: directory to read data from S3 bucket
        output_data: directory to write output parquet files """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    #Create temp view for spark.sql
    df.createOrReplaceTempView("df")

    # extract columns for users table    
    users_table = spark.sql("""SELECT DISTINCT userid,
                                               firstName,
                                               lastName,
                                               gender,
                                               level
                            
                               FROM df
                           """)
   
    users_table = users_table.dropDuplicates(subset = ['userid'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users.parquet')


   # create timestamp column from original timestamp column
    def to_datetime(ms):
        dt = datetime.fromtimestamp(ms/1000.0)
        return dt
    get_timestamp = udf(lambda ms: to_datetime(ms),TimestampType())
    df = df.withColumn('datetime',get_timestamp(col('ts')))
    
    #Replace sql view
    df.createOrReplaceTempView("log_data")
   
    # extract columns to create time table
    time_table = spark.sql("""SELECT DISTINCT datetime as timestamp,
                                              hour(datetime) as hour,
                                              dayofmonth(datetime) as day,
                                              weekofyear(datetime) as week,
                                              month(datetime) as month,
                                              year(datetime) as year
                              FROM log_data""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + 'time.parquet')

    # read in song data to use for songplays table
    song_data = spark.read.json('data/song-data/*/*/*/*.json')
    
    #spark sql temp view for song_data
    song_data.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT DISTINCT  datetime,
                                                    year(datetime) as year,
                                                    month(datetime) as month,
                                                    userid, 
                                                    level, 
                                                    song_id, 
                                                    artist_id, 
                                                    sessionid, 
                                                    location, 
                                                    useragent
                                    FROM log_data
                                    JOIN song_data
                                    ON log_data.artist = song_data.artist_name 
                                    AND log_data.song = song_data.title 
                                    AND log_data.length = song_data.duration
                                    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + 'songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
