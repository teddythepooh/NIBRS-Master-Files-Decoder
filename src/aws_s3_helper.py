import boto3
from botocore.client import BaseClient #https://stackoverflow.com/questions/51118767/what-is-boto3-clients3-returning
import pandas as pd
from io import BytesIO, StringIO

class AWS_S3:
    def __init__(self, credentials_config: dict):
        '''
        credentials_config: a dictionary with region_name, aws_access_key_id, and aws_secret_access_key keys
        
        You can obtain the latter two by creating an IAM user with access to your S3 Bucket;
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
            raise KeyError("Invalid self.credentials_config.")
    
    def upload_table_to_s3_bucket(self, 
                                  table: pd.DataFrame, 
                                  how: str, 
                                  client: BaseClient, 
                                  bucket_name: str,
                                  object_name: str) -> None:
        '''
        uploads a pandas table in-memory onto an S3 bucket, either as a 
        parquet file (if how is 'parquet') or csv file (if how is 'csv')
        '''
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_fileobj.html
        
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
                                 file: str,
                                 client: BaseClient,
                                 bucket_name: str,
                                 object_name: str) -> None:
        '''
        uploads a file onto an S3 bucket as a file called object_name
        '''
        client.upload_file(Filename = file, Bucket = bucket_name, Key = object_name)