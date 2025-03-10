#Script to process DRAGEN log files and generate a summary report
#made by isky20

import pandas as pd
import os
from pathlib import Path
import glob

import make_index as mi


def count_files(pattern):
    try:
        return len(glob.glob(pattern))
    except Exception as e:
        print(f"Error counting files: {e}")
        return 0

def list_files(pattern):
    try:
        files = glob.glob(pattern)
        files = [os.path.basename(f).split("_", 1)[0] for f in files]
        return files
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

def process_dragen_logs(directory):
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
    
    combined = merged_df.groupby(['QBB_ID', 'Ok'], as_index=False).agg({
        'Not Ok': lambda x: ', '.join(x)  
    })
    # Save to CSV
    return combined


def main(dir_path, csv_path):
    tsv_dir = os.path.join(dir_path, "TSV")
    last_name = os.path.basename(os.path.normpath(dir_path))
    nok_sample = count_files(os.path.join(tsv_dir, "*.nok.details.tsv*"))

    try:
        if nok_sample == 0:
            raise ValueError(f"The samples in {last_name} are PASS. Exiting...")

        notok_files_details = list(Path(tsv_dir).rglob("*.nok.details.tsv"))

        notok_files_summary = list(Path(tsv_dir).rglob("*.nok.summary.tsv"))
        
        notOK = {}
        for details, summary in zip(notok_files_details, notok_files_summary):
            #adjust summary
            df_summary = pd.read_csv(summary, sep="\t")
            df_summary.columns = df_summary.columns.str.replace("_STATUS", "", regex=True).str.lower()
            df_summary = df_summary.T
            df_summary.reset_index(inplace=True)
            df_summary.columns = ['TEST_NAME', 'status']

            #adjust details
            df_details = pd.read_csv(details, sep="\t").iloc[:, [0, 1 ,-2, -1,2]]
            df_details = df_details.query("SUBTEST_STATUS == 'not ok'")
            

            df_details = df_details.merge(df_summary[['TEST_NAME', 'status']], on='TEST_NAME', how='left')
            df_details = df_details[(df_details['SUBTEST_STATUS'] == 'not ok') & (df_details['status'] == 'not ok')]


            notOK[str(details).replace(".nok.details.tsv", "")] = df_details


        

        combined_df = pd.concat(notOK.values(), ignore_index=True).iloc[:, [0,1,3]]
        combined = combined_df.groupby(['QBB_ID', 'DROPBOX_UUID'], as_index=False).agg({'SUBTEST_MESSAGE': lambda x: ', '.join(x)})
        result = pd.merge(combined, csv_path, on="QBB_ID", how="left")

    except ValueError as e:
        print(e)
        return
    except Exception as e:
        print(f"Error processing files: {e}")
        return
    
    print(f"error samples in {last_name}")

    return result



# run
directory = "/gpfs/ngsdata/QPHI_OMICS/WGS/QPHI_LOGS/SEQ_LOGS/DRAGEN-VALIDATOR/v1/"
directories = glob.glob("/gpfs/ngsdata/QPHI_OMICS/WGS/QPHI_LOGS/SEQ_LOGS/DRAGEN-VALIDATOR/v1/IN*/")

#step 1
csv = process_dragen_logs(directory)
#step 2
data_dict = {}

for i in directories:
    basename = os.path.basename(os.path.normpath(i)) 
    data_dict[basename] = main(i, csv)

combined_df = pd.concat(data_dict.values(), ignore_index=True)
combined_df.to_csv("final_report.csv", index= False)

print(f"The final report is printed as final_report.csv")





# How to run the script:
# Save this file as dragen_validator.py
# Open a terminal and navigate to the script's directory
# Run the script using the following command:
# for folder in  /gpfs/ngsdata/QPHI_OMICS/WGS/QPHI_LOGS/SEQ_LOGS/DRAGEN-VALIDATOR/v1/IN*/;do python3 make_report.py $folder sampleNok_become_ok.csv; done
