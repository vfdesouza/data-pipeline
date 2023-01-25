from module import Module
import os
from pyspark.sql import SparkSession
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')

obj = Module()

client_bigquery = obj.created_client_bigquery()

# path_to_folder = 'temparq'
# obj.downloat_to_bucket(path_to_folder, BUCKET_NAME)

# spark = SparkSession \
#     .builder \
#     .appName("data-pipeline") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()

# df = spark.read.parquet('./temparq/yellow_tripdata_2022_07.parquet')

# df = pd.read_parquet('./temparq/yellow_tripdata_2022_07.parquet')


# table_id = 'my-project-datapipeline-375319.dataset_test.table_test'

# schema = bigquery.SchemaField(df, "STRING", mode="REQUIRED"),
# table = bigquery.Table(table_id, schema=schema)

# table = client_bigquery.create_table(table)
# print('Process concluded!')