import boto3
from botocore.client import BaseClient
from botocore.exceptions import UnknownServiceError
import s3fs
import pandas as pd
import polars as pl
from io import BytesIO, StringIO

# https://stackoverflow.com/questions/53416226/how-to-write-parquet-file-from-pandas-dataframe-in-s3-in-python
# https://stackoverflow.com/questions/75115246/with-python-is-there-a-way-to-load-a-polars-dataframe-directly-into-an-s3-bucke

import warnings
warnings.filterwarnings(
    "ignore", 
    message = "Polars found a filename" #https://stackoverflow.com/questions/75690784/polars-for-python-how-to-get-rid-of-ensure-you-pass-a-path-to-the-file-instead
    )

class AWSBase:
    def __init__(self, region_name: str, aws_access_key_id: str, aws_secret_access_key: str):
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        
    def _create_credentials_dict(self) -> dict:
        credentials = {
            "region_name": self.region_name,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key
        }
        
        return credentials
        
    def _create_client(self, service: str) -> BaseClient:
        try:
            client = boto3.client(service, **self._create_credentials_dict())
            return client
        except UnknownServiceError:
            raise ValueError(f"Service '{service}' is invalid. AWS client could not be created.")

class AmazonS3(AWSBase):
    def __init__(self, region_name: str, aws_access_key_id: str, aws_secret_access_key: str):
        super().__init__(region_name, aws_access_key_id, aws_secret_access_key)

    @staticmethod
    def _build_s3_uri(bucket_name: str, object_name: str) -> str:
        return f"s3://{bucket_name}/{object_name}"
    
    def _build_s3fs_file_system(self) -> s3fs.S3FileSystem:
        '''
        allows me to ingest files from my s3 bucket
        '''
        return s3fs.S3FileSystem(key = self.aws_access_key_id, secret = self.aws_secret_access_key)

    def read_table_from_s3_bucket(self, bucket_name: str, object_name: str) -> pl.DataFrame:
        '''
        ingests a parquet file from s3 bucket as a polars dataframe
        '''
        if object_name.endswith("parquet"):
            s3_uri = AmazonS3._build_s3_uri(bucket_name = bucket_name, object_name = object_name)
            fs = self._build_s3fs_file_system()

            with fs.open(s3_uri, "rb") as file:
                out_table = pl.read_parquet(file)

            return out_table
        else:
            raise ValueError("This method only supports reading parquet tables at this time.")

    def print_objects_in_s3_bucket(self, bucket_name: str, print_full_dict: bool = False) -> None:
        '''
        print_full_dict: whether or not to print the full nested dictionary from 
        client.list_objects_v2(Bucket = bucket_name) or just the object names + storage size
        
        prints all the object names in s3 bucket
        '''
        full_dict = self._create_client("s3").list_objects_v2(Bucket = bucket_name)
        
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
        if how == "parquet":
            out_buffer = BytesIO()
            table.to_parquet(out_buffer, index = False, engine = "pyarrow")
        elif how == "csv":
            out_buffer = StringIO()
            table.to_csv(out_buffer, index = False)
        else:
            raise ValueError("Invalid 'how' value: only 'csv' and 'parquet' are allowed.")
        
        out_buffer.seek(0)
        
        self._create_client("s3").upload_fileobj(out_buffer, Bucket = bucket_name, Key = object_name)
        
    def upload_file_to_s3_bucket(self, 
                                 file: str,
                                 bucket_name: str,
                                 object_name: str) -> None:
        '''
        file: path to file to be uploaded onto an s3 bucket
        
        uploads a file onto an S3 bucket as a file called object_name
        '''
        self._create_client("s3").upload_file(Filename = file, Bucket = bucket_name, Key = object_name)
