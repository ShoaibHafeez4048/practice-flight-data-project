from pyspark.sql import SparkSession
from pyspark.sql import functions as f 
import argparse

spark = SparkSession.builder.appName('airflow1').getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument('--PROJECT_ID', required=True)
parser.add_argument('--BUCKET_NAME', required=True)
parser.add_argument('--ENV_NAME', required=True)
parser.add_argument('--DATASET_NAME', required=True)
parser.add_argument('--TABLE_NAME', required=True)

args = parser.parse_args()

# Reading the csv files
flight_df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(f'gs://{args.BUCKET_NAME}/data/source/{args.ENV_NAME}/flight_booking.csv')

flight_df.write \
    .format("bigquery") \
    .option("table", f"{args.PROJECT_ID}.{args.DATASET_NAME}.{args.TABLE_NAME}") \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()