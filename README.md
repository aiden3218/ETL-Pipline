# ETL pipeline
This is a python code that merges several data files together into one while verifying data under a requirements document and generating a final report

the code will:

Normalize column names
Merge datasets into a unified DataFrame (loop)
Enforce schema format defined in a JSON configuration (loop)
Apply data quality validations (range checks, regex validation) (loop)
Export a clean, production-ready dataset
Generate structured data quality reports: types, missing values, ranges, outliers

input files: data.json, data.csv
output files: output.json, report.json