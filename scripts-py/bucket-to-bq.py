from module import Module
import os

# from pyspark.sql import SparkSession
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# libs to remove
# import pandas_gbq
# from google.cloud import bigquery
# from google.oauth2.service_account import Credentials

obj = Module()

BUCKET_NAME = os.getenv("BUCKET_NAME")
DATA_SET_RAW = os.getenv("DATA_SET_RAW")
PROJECT_NAME = os.getenv("PROJECT_NAME")
CREDENTIALS = obj.get_credentials()

obj.downloat_to_bucket("files-temp", BUCKET_NAME)

list_files_parquet = os.listdir("./files-temp/")
print(f"Identified files for upload: {list_files_parquet}\n")

for file_parquet in list_files_parquet:

    file_to_path = f"./files-temp/{file_parquet}"

    df = pd.read_parquet(file_to_path).astype(str)
    table_name = file_parquet.split(".")[0]

    print(f"Starting upload of dataframe {table_name}")

    table_schema = [{"name": column, "type": "STRING"} for column in df.columns]

    df.to_gbq(
        destination_table=f"{DATA_SET_RAW}.{table_name}",
        project_id=PROJECT_NAME,
        if_exists="replace",
        progress_bar=True,
        table_schema=table_schema,
        credentials=CREDENTIALS,
    )

    os.remove(file_to_path)

    print(
        f"Upload completed of dataframe {file_parquet}.\nFile removed from directory!\n"
    )

print("Process concluded!")
# commit test
