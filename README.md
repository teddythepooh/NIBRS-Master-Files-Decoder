# NIBRS Master Files Decoder 

## Requirements
- Python 3.11; `requirements.txt`
- PostgreSQL

Through the FBI Crime Data Explorer (CDE), the FBI releases National Incident-Based Reporting System (NIBRS) data every year dating back to 1991. This is the most comprehensive and centralized resource for *incident-level* public safety data, including but not limited to crime; arrest; and victim segments for over 13,000 law enforcement agencies across the United States. For better or worse, the data is in a fixed-length ASCII text format: every n characters is a distinct column, with the first x < n characters delineating between the different data segments. There exists one ASCII text file for each year. The column width specifications can be found in the FBI CDE, specifically the help file in the screenshot below (see the yellow circle): they are enumerated in a poorly-scanned document from 1995. From 2023 onward, the data for which is to be released some time in fall 2023, the FBI has modernized and released the documentation as a proper .pdf entitled "2023.0 NIBRS Technical Specification:" https://le.fbi.gov/informational-tools/ucr/ucr-technical-specifications-user-manuals-and-data-tools. My goals are to
1. develop a tool to seamlessly decode \& extract NIBRS data segments beginning with 2022 and beyond;
2. make the mechanism publically available for others;
3. then harmonize the resulting data into a local PostgreSQL database for storage, processing, and analyses.

![Screenshot 2024-09-05 011353](https://github.com/user-attachments/assets/6a2cb0be-3eb4-43df-893a-8c4768189c79)
