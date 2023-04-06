# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 11:37:08 2023

@author: ASUS
"""

import os
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

storage_connection_string = '<storage connection string>'
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

# Create a container
container_id = '<container id>'
container_client = blob_service_client.create_container(container_id)
# container_client = blob_service_client.get_container_client(container_id)
print(container_client)

# List containers
all_containers = blob_service_client.list_containers(include_metadata=True)
for container in all_containers:
    print(container)


# Upload a blob to a container
blob_root_directory = 'dbt'
working_dir = os.getcwd()
file_directory = os.walk(working_dir + '/dbt')
for folder in file_directory:
    for file in folder[-1]:
        
        try:
            file_path = os.path.join(folder[0], file)      
            blob_path = '{0}{1}'.format(
                blob_root_directory,
                file_path.replace(r"C:\Users\Ready_(Azure Storage) Blob Management/dbt", '')
            )
            blob_obj = blob_service_client.get_blob_client(container=container_id, blob=blob_path)
            with open(file_path, mode='rb') as file_data:
                blob_obj.upload_blob(file_data)
        except ResourceExistsError as e:
            print('Blob (file object) {0} already exists.'.format(file))
            continue
        except Exception as e:            
            raise Exception(e)


# List blobs (file objects) in a given container
blobs = container_client.list_blobs()
# blobs = container_client.list_blobs(name_starts_with='dbt')
for blob in blobs:
    print(blob['name'])
    print(blob['container'])
    print(blob['snapshot'])
    print(blob['version_id'])
    print(blob['is_current_version'])
    print(blob['blob_type'])
    print(blob['blob_tier'])
    print(blob['metadata'])
    print(blob['creation_time'])
    print(blob['last_modified'])
    print(blob['last_accessed_on'])
    print(blob['size'])
    print(blob['deleted'])
    print(blob['deleted_time'])
    print(blob['tags'])    


# Download a blob
file_object_path = 'dbt/2. Build dbt projects/1. Build your DAG/Exposures dbt Developer Hub.pdf'
file_downloaded = os.path.join(working_dir, 'Exposures dbt Developer Hub.pdf')

with open(file_downloaded, mode='wb') as download_file:
    download_file.write(container_client.download_blob(file_object_path).readall())

"""
# Delete a blob (subfolder in this example)
blobs = container_client.list_blobs()
for blob in blobs:
    if blob.name.startswith('dbt/List of commands/'):
        container_client.delete_blob(blob.name)
        print('Blob {0} deleted'.format(blob.name))

# Delete a container
container_client.delete_container()
"""