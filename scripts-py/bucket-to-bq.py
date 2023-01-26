from module import Module
import os
from pyspark.sql import SparkSession
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')

obj = Module()

# path_to_folder = 'temparq'
# obj.downloat_to_bucket(path_to_folder, BUCKET_NAME)

# spark = SparkSession \
#     .builder \
#     .appName('data-pipeline') \
#     .config('spark.some.config.option', 'some-value') \
#     .getOrCreate()

# df = spark.read.parquet('./temparq/yellow_tripdata_2022_07.parquet')

df = pd.read_parquet('./temparq/yellow_tripdata_2022_07.parquet').astype(str)

credentials = obj.get_credentials()
table_schema = [{'name': column, 'type': 'STRING'} for column in df.columns]

df.to_gbq(
    destination_table='dataset_test.table_test',
    project_id='my-project-datapipeline-375319',
    if_exists='replace',
    progress_bar=True,
    table_schema=table_schema,
    credentials=credentials)

print('Process concluded!')