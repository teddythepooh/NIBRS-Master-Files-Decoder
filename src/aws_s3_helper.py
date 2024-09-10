import boto3
from botocore.client import BaseClient #https://stackoverflow.com/questions/51118767/what-is-boto3-clients3-returning
import pandas as pd
from io import BytesIO, StringIO

class AWS_S3:
    '''
    For all methods that contain an object_name arg, assume that it is the name of the object in your S3 bucket.
    For all methods that contain a bucket_name arg, assume that it is the name of the S3 bucket.
    '''
    
    def __init__(self, credentials_config: dict):
        '''
        credentials_config: a dictionary with region_name, aws_access_key_id, and aws_secret_access_key keys
        
        You can obtain the latter two keys by creating an IAM user with access to your S3 Bucket;
        I provisioned mine with AdministratorAccess. You can also use your root user credentials,
        but AWS S3 cautions against that practice as do other online sources.
        '''
        self.credentials_config = credentials_config
        
    def _is_valid_credentials_config(self) -> bool:
        return set(self.credentials_config.keys()) == {"region_name", "aws_access_key_id", "aws_secret_access_key"}
    
    def create_s3_client(self) -> BaseClient:
        if self._is_valid_credentials_config():
            client = boto3.client("s3", **self.credentials_config)
            return client
        else:
            raise KeyError("Instance has invalid credentials_config.")

    def print_objects_in_s3_bucket(self, 
                                  client: BaseClient, 
                                  bucket_name: str,
                                  print_full_dict: bool = False) -> None:
        '''
        return_full_dict: whether or not to print the full nested dictionary from 
        client.list_objects_v2(Bucket = bucket_name) or just the object names + storage size
        
        prints all the object names contained in specified bucket
        '''
        full_dict = client.list_objects_v2(Bucket = bucket_name)
        
        if print_full_dict:
            print(full_dict)
        else:
            try:
                for object in full_dict["Contents"]:
                    file_name = object["Key"]
                    file_size_as_mb = round(object["Size"] / (1000 * 1000), 2)
                    print(f"File Name: {file_name}, Size: {file_size_as_mb} MB")
            except KeyError:
                raise KeyError(f"No objects found in {bucket_name}.")

    
    def upload_table_to_s3_bucket(self, 
                                  client: BaseClient,
                                  table: pd.DataFrame, 
                                  how: str, 
                                  bucket_name: str,
                                  object_name: str) -> None:
        '''
        table: a pandas table
        how: 'csv' or 'parquet,' depending on the desired file type

        uploads a pandas table in-memory onto an S3 bucket, either as a 
        parquet file (if how is 'parquet') or csv file (if how is 'csv')
        '''
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_fileobj.html
        # https://stackoverflow.com/questions/53416226/how-to-write-parquet-file-from-pandas-dataframe-in-s3-in-python
        
        if how == "parquet":
            out_buffer = BytesIO()
            table.to_parquet(out_buffer, index = False, engine = "pyarrow")
        elif how == "csv":
            out_buffer = StringIO()
            table.to_csv(out_buffer, index = False)
        else:
            raise ValueError("Invalid 'how' value: only 'csv' and 'parquet' are allowed.")
        
        out_buffer.seek(0)
        
        client.upload_fileobj(out_buffer, Bucket = bucket_name, Key = object_name)
        
    def upload_file_to_s3_bucket(self, 
                                 client: BaseClient,
                                 file: str,
                                 bucket_name: str,
                                 object_name: str) -> None:
        '''
        file: path to file to be uploaded onto an s3 bucket
        
        uploads a file onto an S3 bucket as a file called object_name
        '''
        client.upload_file(Filename = file, Bucket = bucket_name, Key = object_name)
