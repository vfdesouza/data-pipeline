from google.cloud import storage
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from pathlib import Path
from time import sleep
import requests as r
import os


class Module:

    def __init__(self, credentials='./creds/creds.json'):
        self.credentials = credentials

    def get_credentials(self, credentials='./creds/creds.json'):
        return Credentials.from_service_account_file(credentials)

    def created_client(self):
        # inicializacao do client gcp com as credenciais IAM identificadas
        return storage.Client.from_service_account_json(self.credentials)

    def created_client_bigquery(self):
        return bigquery.Client.from_service_account_json(self.credentials)

    def initialize_bucket(self, bucket_name=str):
        # inicializacao do storage_client
        storage_client = self.created_client()

        # inicializacao do bucket indicado
        bucket = storage_client.get_bucket(bucket_name)

        return bucket

    def upload_to_bucket(self, bucket=str, blob_name=str, path_to_file=str) -> None:
        """
        bucket: nome do bucket que o arquivo deverá ser salvo
        blob_name: nome para o arquivo que será salvo
        path_to_file: caminho do arquivo a ser salvo no bucket 
        """

        # instanciando um blob
        blob = bucket.blob(blob_name)

        # função responsável por enviar o arquivo para o gcp
        blob.upload_from_filename(path_to_file)

        return print(f'Download of file {blob_name} completed successfully.')

    def downloat_to_bucket(self, path_to_folder=str, bucket_name=str) -> None:
        """
        path_to_folder: nome da pasta em que os arquivos serão salvos
        bucket_name: nome do bucket que os arquivos estão para ser baixados
        """

        # verifica se a indicada no parâmetro existe, se não, cria a pasta
        if os.path.exists(path_to_folder) == False:
            Path(path_to_folder).mkdir(parents=True, exist_ok=True)

        storage_client = self.created_client()

        prefix = ['yellow/', 'green/', 'fhv/', 'fhvhv/']

        destination_folder = path_to_folder

        for directory in prefix:
            blobs_in_bucket = storage_client.list_blobs(
                bucket_name, prefix=directory)
            for blob in blobs_in_bucket:
                file = blob.name.split("/")[-1]
                if file != '':
                    blob.download_to_filename(f'{destination_folder}/{file}')

    # função responsável pelas requisições a API que os arquivos serão baixados
    def retry_request(self, url=str) -> dict:
        count_attempt = 0

        while True:
            try:
                response = r.get(url)
                if response.status_code != 200:
                    print(
                        f'Status code not equal to 200: {response.status_code}.')
                response.raise_for_status()
                break
            except:
                if count_attempt > 50:
                    raise Exception('Failed to extract data in 50 retries.')

                print(f'Retry number: {count_attempt}. Waiting...')
                count_attempt += 1
                sleep(4)

        return response
