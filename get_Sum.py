#Script to process DRAGEN log files and generate a summary report
#made by isky20
import pandas as pd
import os
import glob
from pathlib import Path
import argparse

def count_files(pattern):
    """Count the number of files matching the given pattern."""
    try:
        return len(glob.glob(pattern))
    except Exception as e:
        print(f"Error counting files: {e}")
        return 0


def list_files(pattern):
    """List the files matching the given pattern."""
    try:
        files = glob.glob(pattern)
        files = [os.path.basename(f).split("_", 1)[0] for f in files]
        return files
    except Exception as e:
        print(f"Error listing files: {e}")
        return []


def process_dragen_logs(directory):
    """Process DRAGEN logs and generate a summary DataFrame."""
    data_nok = []  # List for NOK files
    data_ok = []   # List for OK files
    
    for root, _, files in os.walk(directory):
        instance = os.path.basename(os.path.dirname(root))  # Extract INSTANCE-...
        
        for file in files:
            if file.endswith(".nok.details.tsv"):
                id_sample = file.split("_")[0]  # Extract ID sample
                data_nok.append([id_sample, instance])

            elif file.endswith(".ok.details.tsv"):
                id_sample = file.split("_")[0]
                data_ok.append([id_sample, instance])
    
    # Create DataFrames
    df_nok = pd.DataFrame(data_nok, columns=["QBB_ID", "Not Ok"])
    df_ok = pd.DataFrame(data_ok, columns=["QBB_ID", "Ok"])
    
    # Merge DataFrames
    merged_df = pd.merge(df_nok, df_ok, on="QBB_ID", how="left")
    merged_df['Ok'] = merged_df['Ok'].fillna('There is no file')


    combined = merged_df.groupby(['QBB_ID', 'Ok'], as_index=False).agg({
        'Not Ok': lambda x: ', '.join(x)  
    })

    #print(merged_df.query("QBB_ID =='QPHI1041988'"))

    # Return the combined DataFrame
    return combined


def main(dir_path, csv_path):
    """Process DRAGEN logs in a given directory and return the results."""
    tsv_dir = os.path.join(dir_path, "TSV")
    last_name = os.path.basename(os.path.normpath(dir_path))
    nok_sample = count_files(os.path.join(tsv_dir, "*.nok.details.tsv"))

    try:
        if nok_sample == 0:
            raise ValueError(f"The samples in {last_name} are PASS. Exiting...")

        notok_files_details = list(Path(tsv_dir).rglob("*.nok.details.tsv"))
        notok_files_summary = list(Path(tsv_dir).rglob("*.nok.summary.tsv"))
        
        notOK = {}
        for details, summary in zip(notok_files_details, notok_files_summary):
            # Adjust summary
            df_summary = pd.read_csv(summary, sep="\t")
            df_summary.columns = df_summary.columns.str.replace("_STATUS", "", regex=True).str.lower()
            df_summary = df_summary.T
            df_summary.reset_index(inplace=True)
            df_summary.columns = ['TEST_NAME', 'status']
            df_summary['status'] = df_summary['status'].fillna('not ok')

            # Adjust details
            df_details = pd.read_csv(details, sep="\t").iloc[:, [0, 1, -2, -1, 2]]
            df_details = df_details.query("SUBTEST_STATUS == 'not ok'")

            df_details = df_details.merge(df_summary[['TEST_NAME', 'status']], on='TEST_NAME', how='left')
            df_details = df_details[(df_details['SUBTEST_STATUS'] == 'not ok') & (df_details['status'] == 'not ok')]

            notOK[str(details).replace(".nok.details.tsv", "")] = df_details

        combined_df = pd.concat(notOK.values(), ignore_index=True).iloc[:, [0, 1, 3]]
        combined = combined_df.groupby(['QBB_ID', 'DROPBOX_UUID'], as_index=False).agg({'SUBTEST_MESSAGE': lambda x: ', '.join(x.astype(str).unique())})
        result = pd.merge(combined, csv_path, on="QBB_ID", how="left")

    except ValueError as e:
        print(e)
        return
    except Exception as e:
        print(f"Error processing files: {e}")
        return
    
    print(f"Error samples in {last_name}")

    return result


def generate_report(input_dir, pattern, output_file):
    """Generate the final report by processing multiple subdirectories based on the pattern."""
    # Create the full pattern for subdirectories
    subdirs_pattern = os.path.join(input_dir, pattern)
    directories = glob.glob(subdirs_pattern)
    
    # Step 1: Process the DRAGEN logs
    csv = process_dragen_logs(input_dir)
    
    # Step 2: Process each directory and generate results
    data_dict = {}
    for dir_path in directories:
        basename = os.path.basename(os.path.normpath(dir_path))
        data_dict[basename] = main(dir_path, csv)

    # Combine all results
    combined_df = pd.concat(data_dict.values(), ignore_index=True)
    
    #group by id , uuid and ok 

    combined = combined_df.groupby(["QBB_ID", "DROPBOX_UUID"]).agg({
    "Not Ok": lambda x: ';'.join(x.astype(str).unique()),
    "SUBTEST_MESSAGE": lambda x: ';'.join(x.astype(str).unique())
    }).reset_index()

    adj_csv = csv.iloc[:, [0, 1]].groupby(["QBB_ID"]).agg({"Ok": lambda x: ';'.join(x.astype(str).unique())})
    combined = pd.merge(combined,adj_csv, on= "QBB_ID", how="outer")

    # Save the final report as CSV
    combined.to_csv(output_file, index=False)
    print(f"The final report is printed as {output_file}")


if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser(description="Process DRAGEN log files and generate a summary report.")
    
    # Input arguments
    parser.add_argument("--input_dir", required=True, help="Base directory containing the DRAGEN logs.")
    parser.add_argument("--pattern", required=True, help="Pattern for subdirectories within the input directory (e.g., 'IN*/').")
    parser.add_argument("--output_file", required=True, help="Output file name for the final report (e.g., 'final_report.csv').")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Generate the report
    generate_report(args.input_dir, args.pattern, args.output_file)

    ##python script.py --input_dir "/gpfs/ngsdata/QPHI_OMICS/WGS/QPHI_LOGS/SEQ_LOGS/DRAGEN-VALIDATOR/v1/" --pattern "IN*/" --output_file "final_report.csv"
