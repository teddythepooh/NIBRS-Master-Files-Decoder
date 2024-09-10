raw_data_dir=raw_data
output_dir=output

data_year=2022
nibrs_zipped_file=$raw_data_dir/nibrs-$data_year.zip

if [[ ! -f $nibrs_zipped_file ]]; then
    echo "No NIBRS zipped file found in $raw_data_dir. Aborting shell script..."
    exit 1
fi

nibrs_mf=$raw_data_dir/${data_year}_NIBRS_NATIONAL_MASTER_FILE_ENC.txt

if [[ ! -f $nibrs_mf ]]; then
    unzip $nibrs_zipped_file -d $raw_data_dir
fi

src=src/

segment_prefixes=(
    administrative 
    offense
    arrestee
    victim
)

nibrs_segments=()
for prefix in ${segment_prefixes[@]}; do
    nibrs_segments+=(${prefix}_segment)
done

for segment in ${nibrs_segments[@]}; do
    echo "Decoding $segment..."
    python decode_segments.py \
        --output_dir=$output_dir \
        --nibrs_master_file=$nibrs_mf \
        --config_file=configuration/col_specs.yaml \
        --private_config_file=configuration/private_config.yaml \
        --segment_name=$segment \
        --to_aws_s3
done

wait

echo "Done. All segments have been decoded."