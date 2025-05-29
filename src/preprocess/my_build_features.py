import src.common.tools as tools
import src.data.dataio as dataio
import os
import glob
import pandas as pd
import re

def recode_ach_level(val):
    if pd.isna(val):
        return None
    if val == "Not Proficient":
        return 0
    match = re.search(r"[1-5]", str(val))
    if match:
        return int(match.group())
    return None

def clean(csv):
    base_name = os.path.basename(csv).lower()
    df = pd.read_csv(csv)
    #1. turn into a df
    #2. put into /interim
    #3. do whatever cleaning was done previously in jupyter notebook
    

    if "curtest" in base_name:
        #Missing ids
        if 'mastid' in df.columns:
            df = df.dropna(subset=['mastid'])
        # Renaming
        if 'ACH_LEVEL' in df.columns:
            df = df.rename(columns={'ACH_LEVEL': 'ach_level'})
        if 'COLLECTION_CODE' in df.columns:
            df = df.rename(columns={'COLLECTION_CODE': 'collection_code'})
        if 'TEST_ID' in df.columns:
            df = df.rename(columns={'TEST_ID': 'test_id'})
        if 'EXEMPTION_CODE' in df.columns:
            df = df.rename(columns={'EXEMPTION_CODE': 'exemption_code'})
        if 'SCORE' in df.columns:
            df = df.rename(columns={'SCORE': 'score'})
        if 'ACCOMM_LIST' in df.columns:
            df = df.rename(columns={'ACCOMM_LIST': 'accomm_list'})
            
        if 'admindt' in df.columns:
            df.drop(columns='admindt',inplace=True)
        if 'test_schl' in df.columns:
            df.drop(columns='test_schl',inplace=True)
        if 'test_lea' in df.columns:
            df.drop(columns='test_lea',inplace=True)
        if 'collection_code' in df.columns:
            df.drop(columns='collection_code',inplace=True)
        if 'ach_level' in df.columns:
            df.dropna(subset=['ach_level'],inplace=True)
            df['ach_level'] = df['ach_level'].apply(recode_ach_level)
            df.dropna(subset=['ach_level'], inplace=True)
            df['ach_level'] = df['ach_level'].astype(int)
            
            #4 combine them
            
            ##DO FOR ALL OTHER ONES



if __name__ == "__main__":
    config = tools.load_config()
    raw_data_path = config["datarawdirectory"]
    interim_data_path = config["datainterimdirectory"]

    os.makedirs(interim_data_path, exist_ok=True)

    csv_files = glob(os.path.join(raw_data_path, "*.csv"))

    for csv_file in csv_files:
        print(f"Processing: {csv_file}")
        cleaned_df = clean(csv_file)

        base_name = os.path.basename(csv_file)
        save_path = os.path.join(interim_data_path, base_name)
        cleaned_df.to_csv(save_path, index=False)
        print(f"Saved cleaned file to: {save_path}")