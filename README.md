DRAGEN Log File Processor – Summary Report Generator

Overview

get_Sum.py is a Python script designed to process DRAGEN log files and generate a structured summary report. It automates the identification of failed (NOK) and successful (OK) samples, aggregates error messages, and produces a final CSV report.

Features

✅ Counts and Lists Log Files

Identifies files matching .nok.details.tsv and .ok.details.tsv.

✅ Processes DRAGEN Logs

Extracts sample IDs and categorizes them as Ok or Not Ok.

✅ Error Analysis

Reads .nok.summary.tsv and .nok.details.tsv to identify failed tests.

Aggregates error messages for each sample.

✅ Report Generation

Merges results from multiple subdirectories.

Saves structured data in a CSV file.

```bash
python get_Sum.py --input_dir "/path/to/logs/" --pattern "IN*/" --output_file "final_report.csv"
```
