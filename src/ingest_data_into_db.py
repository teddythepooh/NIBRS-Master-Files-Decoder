import polars as pl
import psycopg2
import argparse

from utils import general_utils, AWS_S3

def main(args: argparse.Namespace):
    config = general_utils.load_yaml(args.private_config_file)
    AWS_S3_tool = AWS_S3(config["credentials"])
    
    test = AWS_S3_tool.read_table_from_s3_bucket(bucket_name = config["bucket_name"], 
                                                 object_name = "administrative_segment_2022.parquet")
    
    print(test)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--private_config_file", "-pc")
    
    args = parser.parse_args()
    
    main(args)
    