import polars as pl
import psycopg2
import argparse

from utils import general_utils, AWS_S3, Postgres

def main(args: argparse.Namespace):
    aws_config = general_utils.load_yaml(args.aws_config)
    postgres_config = general_utils.load_yaml(args.postgres_config)
    
    aws_s3_tool = AWS_S3(aws_config["credentials"])
    postgres_tool = Postgres(postgres_config["postgresql"])
    
    test_table = aws_s3_tool.read_table_from_s3_bucket(bucket_name = aws_config["bucket_name"], 
                                                       object_name = "administrative_segment_2022.parquet")
    
    sqlalchemy_engine = postgres_tool.create_sqlalchemy_engine()
    
    test_table.write_database(table_name="raw.administrative_segment", if_table_exists = "replace", connection = sqlalchemy_engine)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--aws_config", "-aws", help = ".yaml file with bucket_name and credentials keys")
    parser.add_argument("--postgres_config", "-postgres", help = ".yaml file with postgresql key, under which exists credentials and schemas keys")
    
    args = parser.parse_args()
    
    main(args)
    