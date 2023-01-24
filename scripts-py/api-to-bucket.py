from module import Module
from time import sleep
import requests as r
import os

obj = Module()

bucket_name = 'bucket-quickstart_my-project-datapipeline-375319'
bucket = obj.initialize_bucket(bucket_name=bucket_name)

color = ['yellow', 'green', 'fhv', 'fhvhv']
month = ['07', '08', '09', '10', '11', '12']

base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/%s_tripdata_2022-%s.parquet'

list_args = []
for c in color:
    for m in month:
        url = str(base_url % (c, m))
        filename = f'{c}_tripdata_2022_{m}.parquet'

        # obj.download_to_file(url=url, path='./temparq/', filename=filename)

        try:
            response = r.get(url, allow_redirects=True)
        except r.exceptions.RequestException as e:
            print(e)

        with open('./temparq/' + filename, 'wb') as file:
            file.write(response.content)
            file.close()

        sleep(3)

        obj.upload_to_bucket(
            blob_name=f'{c}/{filename}',
            path_to_file=f'./temparq/{filename}',
            bucket=bucket)

        path = os.path.join('./temparq/', filename)
        os.remove(path)

print('Process concluded!')
