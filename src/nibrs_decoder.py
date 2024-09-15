import pandas as pd
from io import StringIO

class NIBRSDecoder:
    def __init__(self, nibrs_master_file: str, col_specs: dict):
        '''
        nibrs_master_file: the path to the NIBRS fixed-length, ASCII text file for some year
        
        col_specs: a dictionary that defines the segment names' levels, along with their
        column widths and column names, which should look something like in the .yaml file
        
        ------------------- an example of col_specs (as a .yaml file)
            segment_level_codes:
                 administrative_segment: '01'

            administrative_segment:
                col1: [start1, end1]
                col2: [start2, end2]
        -------------------
        
        In the NIBRS data, the segment 'level' (a 2-character alphanumeric sequence) is how we can 
        delineate which lines belong to which segment in the aforementioned text file. For example, 
        all lines that start with '01' are the so-called administrative segment data.
        '''
        self.nibrs_master_file = nibrs_master_file
        self.col_specs = col_specs
        
        if "segment_level_codes" not in self.col_specs.keys():
            raise KeyError("Invalid col_specs. It must have a segment_level_codes key.")
        
    def _view_all_segment_level_codes(self) -> None:
        '''
        prints all available segment codes defined in self.col_specs["segment_level_codes"]
        '''
        for segment, code in self.col_specs["segment_level_codes"].items():
            print(f"{segment} : {code}")
    
    def _get_code_for_segment(self, segment_name: str) -> str:
        try:
            return self.col_specs.get("segment_level_codes")[segment_name]
        except KeyError:
            raise KeyError("no code for {segment_name} found in col_specs")
        
    def get_col_specs_for_segment(self, segment_name: str) -> tuple:
        col_specs_config = self.col_specs[segment_name]
        
        return tuple(tuple(i) for i in col_specs_config.values())
    
    def get_col_names_for_segment(self, segment_name: str) -> list:
        return list(self.col_specs[segment_name].keys())
    
    def decode_segment(self, segment_name: str) -> pd.DataFrame:
        '''
        this opens self.nibrs_master_file; filters for lines that start with 
        self._get_code_for_segment(segment_name); and produces a pandas table 
        based on the segment's column widths & names as defined in self.col_specs
        '''
        segment_code = self._get_code_for_segment(segment_name)
        col_specs = self.get_col_specs_for_segment(segment_name)
        col_names = self.get_col_names_for_segment(segment_name)
        
        with open(self.nibrs_master_file, "r") as file:
            filtered_lines = (line for line in file if line.startswith(segment_code))
            
            segment_as_text = StringIO()
            for line in filtered_lines:
                segment_as_text.write(line)
            
            segment_as_text.seek(0) # to reset the segment_as_text's pointer to the very beginning after populating it with filtered_lines
            
            out_table = pd.read_fwf(segment_as_text, colspecs = col_specs, names = col_names)

        return out_table
