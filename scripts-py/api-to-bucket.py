from module import Module

obj = Module()

bucket_name = ''
bucket = obj.initialize_bucket(bucket_name=bucket_name)

file_to_upload = ''
path_to_file = ''

obj.upload_to_bucket(
  blob_name=file_to_upload,\
  path_to_file=path_to_file,\
  bucket=bucket)

print('Process concluded!')