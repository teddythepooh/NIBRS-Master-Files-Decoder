#!/bin/bash

raw_data_dir=raw_data
output_dir=output

data_year=2022
nibrs_zipped_file=$raw_data_dir/nibrs-$data_year.zip
nibrs_mf=$raw_data_dir/${data_year}_NIBRS_NATIONAL_MASTER_FILE_ENC.txt

if [[ ! -f $nibrs_mf ]]; then
    unzip $nibrs_zipped_file -d $raw_data_dir
fi

src=src/

echo '(test) Decoding administrative segment...'
python $src/test.py \
    --output_dir=$output_dir \
    --nibrs_master_file=$nibrs_mf \
    --config_file=configuration/col_specs.yaml \
    --segment_name=administrative_segment
