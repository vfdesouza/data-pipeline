from module import Module

obj = Module()

bucket_name=''
blob_name=''
path_folder=''

bucket = obj.initialize_bucket(bucket_name=bucket_name)

obj.download_to_bucket(bucket=bucket,\
                      blob_name=blob_name,\
                      path_folder=path_folder)

print('Process concluded!')