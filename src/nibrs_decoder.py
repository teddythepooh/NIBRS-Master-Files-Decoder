import pandas as pd
from io import StringIO

class NIBRSDecoder:
    def __init__(self, nibrs_master_file: str, col_specs: dict):
        self.nibrs_master_file = nibrs_master_file
        self.col_specs = col_specs
        
        if "segment_level_codes" not in self.col_specs.keys():
            raise KeyError("Invalid col_specs dict. It must have a segment_level_codes key.")
        
    def _view_all_segment_level_codes(self) -> None:
        for segment, code in self.col_specs["segment_level_codes"].items():
            print(f"{segment} : {code}")
    
    def _get_code_for_segment(self, segment_name: str) -> str:
        try:
            return self.col_specs.get("segment_level_codes")[segment_name]
        except KeyError:
            raise KeyError("no code for {segment_name} found in col_specs")
        
    def create_col_specs(self, segment_name: str) -> tuple:
        col_specs_config = self.col_specs[segment_name]
        
        return tuple(tuple(i) for i in col_specs_config.values())
    
    def decode_segment(self, segment_name: str) -> pd.DataFrame:
        segment_code = self._get_code_for_segment(segment_name)
        
        with open(self.nibrs_master_file, 'r') as file:
            filtered_lines = [line for line in file if line.startswith(segment_code)]
        
        col_specs = self.create_col_specs(segment_name)
        col_names = list(self.col_specs[segment_name].keys())

        filtered_text = '\n'.join(filtered_lines)
        out_table = pd.read_fwf(StringIO(filtered_text), colspecs = col_specs, names = col_names)
        
        return out_table
