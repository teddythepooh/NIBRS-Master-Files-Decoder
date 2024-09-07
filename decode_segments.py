import argparse
import logging
from pathlib import Path

from time import perf_counter

from src import utils
from src import NIBRSDecoder
from src import AWS_S3

def main(args: argparse.Namespace) -> None:
    output_dir: Path
    logger: logging.Logger
    
    start = perf_counter()
    output_dir, logger = utils.create_output_dir_and_logger(output_dir_str = args.output_dir, log_file = f"{Path(__file__).stem}.log")
        
    data_year = Path(args.nibrs_master_file).name[0:4]
    
    if not data_year.isdigit():
        logger.warning("Double check args.nibrs_master_file. This text file's file name must be "
                       "prefixed with the year (e.g., 2022).")
        
    logger.info(f"Decoding {args.segment_name}...")
    
    col_specs_config = utils.load_yaml(args.config_file)
    
    nibrs_processor_tool = NIBRSDecoder(args.nibrs_master_file, col_specs_config)
    
    out_table = nibrs_processor_tool.decode_segment(args.segment_name)
    
    out_name = f"{args.segment_name}_{data_year}.parquet"
    
    logger.info("Exporting...")
    if args.to_aws_s3:
        logger.info("to_aws_s3 was toggled: sending decoded segment directly to s3 bucket...")
        private_config = utils.load_yaml(args.private_config_file)
        
        AWS_S3_tool = AWS_S3(private_config["credentials"])
        s3_client = AWS_S3_tool.create_s3_client()
        
        AWS_S3_tool.upload_table_to_s3_bucket(table = out_table,
                                              how = "parquet",
                                              client = s3_client,
                                              bucket_name = private_config["bucket_name"],
                                              object_name = out_name)
    else:
        out_table.to_parquet(output_dir.joinpath(out_name))
    
    end = perf_counter()
    
    logger.info(f"Done. Total run time: {round((end - start) / 60, 2)} minutes")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", "-o")
    parser.add_argument("--nibrs_master_file", "-f", help = "path to the NIBRS fixed-length, ASCII text file")
    parser.add_argument("--config_file", "-c", help = ".yaml file with 'segment_level_codes' key, plus any segments of interest as keys")
    parser.add_argument("--segment_name", "-s", help = "segment of interest to decode; it must be present as key in config_file")
    
    parser.add_argument("--to_aws_s3",
                        help = ("if toggled, the decoded segment won't be exported to output_dir and instead be "
                                "uploaded to an s3 bucket using configuration from private_config_file"),
                        action = "store_true")
    parser.add_argument("--private_config_file", "-pc", 
                        help = (".yaml file with 'bucket_name' and 'credentials' keys, where 'credentials' "
                                "must be a key:value pair of the required credentials to interact with "
                                "AWS S3 in case -aws is toggled"),
                        required = False)
    
    args = parser.parse_args()
    
    main(args)
