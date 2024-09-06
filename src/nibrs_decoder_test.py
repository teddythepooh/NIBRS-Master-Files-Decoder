import argparse, logging
from pathlib import Path
from time import perf_counter

import utils
from nibrs_decoder import NIBRSDecoder

def main(args: argparse.Namespace):
    output_dir: Path
    logger: logging.Logger
    
    start = perf_counter()
    output_dir, logger = utils.create_output_dir_and_logger(output_dir_str = args.output_dir, log_file = f"{Path(__file__).stem}.log")
    
    logger.info(f"Decoding {args.segment_name}...")
    
    col_specs_config = utils.load_yaml(args.config_file)
    
    nibrs_processor_tool = NIBRSDecoder(args.nibrs_master_file, col_specs_config)
    
    out_table = nibrs_processor_tool.decode_segment(args.segment_name)
    
    out_table.to_parquet(output_dir.joinpath(f"{args.segment_name}.parquet"))
    
    end = perf_counter()
    
    logger.info(f"Done. Total run time: {round((end - start) / 60, 2)} minutes")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir")
    parser.add_argument("--nibrs_master_file", "-f", help = "path to the NIBRS fixed-length, ASCII text file")
    parser.add_argument("--config_file", help = ".yaml file with segment_level_codes key, plus any segments of interest as keys")
    parser.add_argument("--segment_name", help = "segment of interest to decode; it must be present as key in config_file")
    
    args = parser.parse_args()
    
    main(args)
