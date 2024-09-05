import pandas as pd, numpy as np
import argparse, logging, yaml
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
    
    test = nibrs_processor_tool.decode_segment(args.segment_name)
    
    test.to_csv(output_dir.joinpath("test.csv"), index = False)
    
    end = perf_counter()
    
    logger.info(f"Done. Total run time: {round((end - start) / 60, 2)} minutes")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", default = "output")
    parser.add_argument("--nibrs_master_file", "-f", default = "raw_data/2022_NIBRS_NATIONAL_MASTER_FILE_ENC.txt")
    parser.add_argument("--config_file", default = "configuration/col_specs.yaml")
    parser.add_argument("--segment_name", default = "administrative_segment")
    
    args = parser.parse_args()
    
    main(args)
