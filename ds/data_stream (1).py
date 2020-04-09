import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', StringType()),
    StructField('original_crime_type_name', StringType()),
    StructField('report_date', StringType()),
    StructField('call_date', StringType()),
    StructField('offense_date', StringType()),
    StructField('call_time', StringType()),
    StructField('call_date_time', StringType()),
    StructField('disposition', StringType()),
    StructField('address', StringType()),
    StructField('city', StringType()),
    StructField('state', StringType()),
    StructField('agency_id', StringType()),
    StructField('address_type', StringType()),
    StructField('common_location', StringType()),
    StructField('city', StringType())
    
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'com.udacity.ds.proj2.crimes') \
        .option('startingOffsets', 'latest') \
        .option('maxOffsetsPerTrigger', 200) \
        .load()
        

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS string)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select('original_crime_type_name', 
                                          'disposition', 
                                          psf.to_timestamp('call_date_time', 'yyyy-MM-ddTHH:mm:ss.SSS').alias('call_timestamp'))
    
    # count the number of original crime type
    agg_df = distinct_table \
        .withWatermark("call_timestamp", "1 days") \
        .groupBy(
            psf.window('call_timestamp', '1 hours'),
            distinct_table.original_crime_type_name
        ).count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .outputMode('update') \
        .format('console') \
        .trigger(processingTime='30 seconds') \
        .start()
        
        
    # TODO attach a ProgressReporter
    #query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = os.path.join(os.path.dirname(__file__), "radio_code.json")
    
    print(f'radio code json filepath {radio_code_json_filepath}')
    
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)
    radio_code_df.printSchema()

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = distinct_table.join(radio_code_df, 'disposition') \
        .writeStream \
        .outputMode('update') \
        .format('console') \
        .trigger(processingTime='5 seconds') \
        .start()


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config('spark.ui.port', 3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
