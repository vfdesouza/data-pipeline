from google.cloud import storage
from pathlib import Path


class Module:

    def __init__(self, credentials='./creds/creds.json'):
        self.credentials = credentials

    def created_client(self):
        # inicializacao do client gcp com as credenciais IAM identificadas
        return storage.Client.from_service_account_json(self.credentials)

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

        blob = bucket.blob(blob_name)

        blob.upload_from_filename(path_to_file)

        return print('Upload sucess')

    def download_to_bucket(self, bucket=str, blob_name=str, path_folder=str) -> None:
        """
        bucket: nome do bucket que o arquivo deverá ser salvo
        blob_name: nome do arquivo que será baixado do bucket 
        path_folder: nome da pasta que deverá ser criada para que o arquivo seja salvo
        """

        blob = bucket.blob(blob_name)

        Path(path_folder).mkdir(parents=True, exist_ok=True)

        blob.download_to_filename(f'{path_folder}/{blob.name}')

        return print('Upload sucess')
