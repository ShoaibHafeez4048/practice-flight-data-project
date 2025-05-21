from pyspark.sql import SparkSession
from pyspark.sql import functions as f 
from airflow.models import Variable

spark = SparkSession.builder.appName('airflow1').getOrCreate()

variable_dict = Variable.get("Env_vars", deserialize_json=True)
PROJECT_ID = variable_dict["PROJECT_ID"]
BUCKET_NAME = variable_dict["BUCKET_NAME"]
DATASET_NAME = variable_dict["DATASET_NAME"]
TABLE_NAME = variable_dict["TABLE_NAME"]
ENV_NAME = variable_dict["ENV_NAME"]

# Reading the csv files
flight_df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(f'gs://{BUCKET_NAME}/data/source/{ENV_NAME}/flight_booking.csv')

flight_df.write \
    .format("bigquery") \
    .option("table", f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}") \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()