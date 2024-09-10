try:
    from src import utils
    from src import AWS_S3
except ImportError:
    raise Exception("Run this as a module, like python -m tests.list_objects_in_s3_bucket")

def main():
    config = utils.load_yaml("configuration/private_config.yaml")
    credentials = config.get("credentials")
    
    aws_s3_tool = AWS_S3(credentials)
    
    s3_client = aws_s3_tool.create_s3_client()
    
    aws_s3_tool.print_objects_in_s3_bucket(client = s3_client, bucket_name = config["bucket_name"])
    aws_s3_tool.print_objects_in_s3_bucket(client = s3_client, bucket_name = config["bucket_name"], return_full_dict = True)
    
    print(aws_s3_tool.__doc__)
    
if __name__ == "__main__":
    main()