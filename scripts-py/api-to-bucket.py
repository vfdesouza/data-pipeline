from module import Module
from time import sleep
import os
from dotenv import load_dotenv
load_dotenv()

obj = Module()

BUCKET_NAME = os.getenv('BUCKET_NAME')

bucket = obj.initialize_bucket(bucket_name=BUCKET_NAME)

color = ['yellow', 'green', 'fhv', 'fhvhv']
month = ['07', '08', '09', '10', '11', '12']

base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/%s_tripdata_2022-%s.parquet'

for c in color:
    for m in month:
        url = str(base_url % (c, m))
        filename = f'{c}_tripdata_2022_{m}.parquet'

        response = obj.retry_request(url)

        with open('./files-temp/' + filename, 'wb') as file:
            file.write(response.content)
            file.close()

        sleep(3)

        obj.upload_to_bucket(
            blob_name=f'{c}/{filename}',
            path_to_file=f'./files-temp/{filename}',
            bucket=bucket)

        path = os.path.join('./files-temp/', filename)
        os.remove(path)

print('Process concluded!')
#commit teste
