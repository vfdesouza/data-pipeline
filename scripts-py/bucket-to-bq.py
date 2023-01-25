from module import Module
import os
import base64
from dotenv import load_dotenv
load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')

obj = Module()

path_to_folder = ''
obj.downloat_to_bucket(path_to_folder, BUCKET_NAME)

print('Process concluded!')
