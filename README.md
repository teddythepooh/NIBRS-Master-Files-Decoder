# NIBRS Master Files Decoder 

## Requirements
- Python 3.11.3; `requirements.txt`
- PostgreSQL

## Instructions
In the command line, navigate to `$dir`: the parent directory of this pipeline, as in the path of this README. Then,
1. Download the NIBRS zipped file locally (i.e., `nibrs-2022.zip`), then store it in `$dir/raw_data/`.
2. Activate virtual environment.
3. Run `bash main.sh`.

Through the FBI Crime Data Explorer (CDE), the FBI releases National Incident-Based Reporting System (NIBRS) data every year dating back to 1991. This is the most comprehensive and centralized resource for *incident-level* public safety data, including but not limited to crime; arrest; and victim segments for over 13,000 law enforcement agencies across the United States. For better or worse, the data is in a fixed-length ASCII text format: every n characters is a distinct column, with the first x < n characters delineating between the different data segments. There exists one ASCII text file per year. The column width specifications can be found in the FBI CDE, specifically the help file in the screenshot below (see the yellow circle): they are enumerated in a poorly-scanned document from 1995. From 2023 onward, the data for which is to be released some time in fall 2023, the FBI has slightly modernized and released the documentation as a proper .pdf entitled "2023.0 NIBRS Technical Specification:" https://le.fbi.gov/informational-tools/ucr/ucr-technical-specifications-user-manuals-and-data-tools. **My goals are to**
1. **develop a tool to seamlessly decode \& extract NIBRS data segments beginning with 2022 and beyond;**
2. **make the mechanism publicly available for others;**
3. (in-process) **host the resulting data in an AWS S3 bucket;**
4. (in-process) **then leverage AWS Lambda to process and harmonize the data onto Amazon Redshift.**

![Screenshot 2024-09-05 011353](https://github.com/user-attachments/assets/6a2cb0be-3eb4-43df-893a-8c4768189c79)

### AWS Resources
This section is where I will store my AWS learning resources for future reference
1. AWS S3 pricing: https://aws.amazon.com/s3/pricing/
2. IAM user vs role: https://stackoverflow.com/questions/46199680/difference-between-iam-role-and-iam-user-in-aws
3. AWS S3 intro: https://www.youtube.com/watch?v=tfU0JEZjcsg
4. how to upload pandas tables as parquet files to AWS S3 bucket: https://stackoverflow.com/questions/53416226/how-to-write-parquet-file-from-pandas-dataframe-in-s3-in-python
