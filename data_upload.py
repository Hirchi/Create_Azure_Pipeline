# coding: utf-8

'''
FILE: data_upload.py
DESCRIPTION:
    This example shows how to perform common filesystem-like operations on a
    container. This includes uploading and downloading files to and from the
    container with an optional prefix, listing files in the container both at
    a single level and recursively, and deleting files in the container either
    individually or recursively.

    To run this sample, provide the name of the storage container to operate on
    as the script argument (e.g. `python3 directory_interface.py my-container`).
    This sample expects that the `AZURE_STORAGE_CONNECTION_STRING` environment
    variable is set. It SHOULD NOT be hardcoded in any code derived from this
    sample.
  USAGE: python blob_samples_directory_interface.py CONTAINER_NAME
    Set the environment variables with your own values before running the sample:
    1) AZURE_STORAGE_CONNECTION_STRING - the connection string to your storage account
'''

import os
import sys

from azure.storage.blob import BlobServiceClient

class DirectoryClient:
  def __init__(self, connection_string, container_name):
    service_client = BlobServiceClient.from_connection_string(connection_string)
    self.client = service_client.get_container_client(container_name)

  def upload(self, source, dest):
    '''
    Upload a file or directory to a path inside the container
    '''
    if (os.path.isdir(source)):
      self.upload_dir(source, dest)
    else:
      self.upload_file(source, dest)

  def upload_file(self, source, dest):
    '''
    Upload a single file to a path inside the container
    '''
    print(f'Uploading {source} to {dest}')
    with open(source, 'rb') as data:
      self.client.upload_blob(name=dest, data=data)

  def upload_dir(self, source, dest):
    '''
    Upload a directory to a path inside the container
    '''
    prefix = '' if dest == '' else dest + '/'
    prefix += os.path.basename(source) + '/'
    for root, dirs, files in os.walk(source):
      for name in files:
        dir_part = os.path.relpath(root, source)
        dir_part = '' if dir_part == '.' else dir_part + '/'
        file_path = os.path.join(root, name)
        blob_path = prefix + dir_part + name
        self.upload_file(file_path, blob_path)

# get the connection string from the environment
try:
  CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']
except KeyError:
  print('AZURE_STORAGE_CONNECTION_STRING must be set')
  sys.exit(1)

# get the container name from the command line
try:
  CONTAINER_NAME = sys.argv[1]
except IndexError:
  print('usage: python data_upload.py CONTAINER_NAME  # after the AZURE_STORAGE_CONNECTION_STRING environment variable is set')
  print('error: the following arguments are required: CONTAINER_NAME')
  sys.exit(1)

# Instantiate the DirectoryClient
client = DirectoryClient(CONNECTION_STRING, CONTAINER_NAME)

# Upload the files from stocks_data directory to Azure blob container
client.upload_dir('stocks_data', '')
