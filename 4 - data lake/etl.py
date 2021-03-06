import configparser
from datetime import datetime
import os
import json
from re import MULTILINE
from logger import setup_logger

from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType


import logging
logger = logging.getLogger('etl')


SONG_SCHEMA_PATH = "schema/song_data.json"



SONG_DATA = "song_data"
LOG_DATA = "log_data"


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("sparkify-etl") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def get_schema(config_file_path=None):
    if config_file_path is None:
        return StructType(
            [
                StructField("num_songs", IntegerType(), True),
                StructField("artist_id", StringType(), True),
                StructField("artist_latitude", FloatType(), True),
                StructField("artist_longitude", FloatType(), True),
                StructField("artist_location", StringType(), True),
                StructField("artist_name", StringType(), True),
                StructField("song_id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("duration", FloatType(), True),
                StructField("year", IntegerType(), True)
            ])

    return get_schema_from_config_file(config_file_path)

def get_schema_from_config_file(config_file_path):
    with open(config_file_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
        return StructType.fromJson(schema)




def process_song_data(spark, input_data, output_data):
    song_data = f"{input_data}/{SONG_DATA}/*/*/*/*.json"
    song_data_schema = get_schema()

    logger.info('spark is reading json files from s3...')

    df = spark.read.option("recursiveFileLookup", "true").json(song_data, multiLine=True, schema=song_data_schema)

    logger.info('size of RDD is %d' % df.count())
    
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()

    logger.info('size of RDD is %d' % songs_table.count())

    songs_table.write.partitionBy("year", "artist_id").parquet(f"{output_data}/songs", mode="overwrite")

    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates() 
    
    artists_table.write.partitionBy("artist_id").parquet(f"{output_data}/artists", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    # log_data =

    # read log data file
    # df = 
    
    # filter by actions for song plays
    # df = 

    # extract columns for users table    
    # artists_table = 
    
    # write users table to parquet files
    # artists_table

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    # time_table = 
    
    # write time table to parquet files partitioned by year and month
    # time_table

    # read in song data to use for songplays table
    # song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    # songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    # songplays_table
    pass


def main():
    setup_logger()
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify-parquet/"
    
    process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
