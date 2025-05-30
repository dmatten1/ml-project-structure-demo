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
    elif "collegeboard" in base_name:
        # map typo’d names → correct names
        rename_map = {
        'sat_erw_score_hc': 'sat_ebrw_score_hc',
        'sat_erw_score_mr': 'sat_ebrw_score_mr',
        'sat_ctsh_ss_mc':   'sat_ctsh_ss_hc',
        'ital':           'italgr'
        }

        # --- 1) Build “first-seen” list of all columns ---
        seen = []
        for yr in years:
            hdr = pd.read_csv(
            data_dir / f"collegeboard{str(yr)[-2:]}pub.csv",
            nrows=0
            )
        cols = (
        hdr.columns
           .str.strip()
           .str.lower()
           .str.replace(" ", "_")
           .map(lambda c: rename_map.get(c, c))
        )
        for c in cols:
            if c not in seen:
                seen.append(c)

        all_cols = seen  # preserves 2018 order, then new 2019 fields, etc.

        # --- 2) Read & clean each year’s data, align to all_cols ---
        dfs = []
        for yr in years:
            df = pd.read_csv(
            data_dir / f"collegeboard{str(yr)[-2:]}pub.csv",
            low_memory=False
            )
        # normalize original column names
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )
        # fix known typos
        df = df.rename(columns=rename_map)
        # drop any duplicate columns just in case
        df = df.loc[:, ~df.columns.duplicated()]
        # reindex so every DF has exactly all_cols (missing→NaN)
        df = df.reindex(columns=all_cols)
        # tag the year
        df['year'] = yr

        dfs.append(df)

        # --- 3) Concatenate into one clean DataFrame ---
        cb = pd.concat(dfs, ignore_index=True)

        cb['lea'] = cb['lea'].astype(str).str.strip()

        # find all the float columns
        float_cols = cb.select_dtypes(include='float').columns
        cb[float_cols] = cb[float_cols].round().astype('Int64')

        date_cols = [c for c in cb.columns 
             if c in ['birthdt', 'grad_date'] 
             or c.endswith('_dt')]

        # convert
        for col in date_cols:
            cb[col] = pd.to_datetime(cb[col], errors='coerce')
    
        cat_cols = [
            'sex',       # e.g. M/F
            'ethnic',    # your coded ethnicity groups
            'blang',     # bilingual flag
            'lea',       # local education agency code
            'instname'   # school name
            ]

        for col in cat_cols:
            if col in cb.columns:
                cb[col] = cb[col].astype('category')
        
        essay_cols = ['sat_er_hc', 'sat_er_mr', 'sat_ea_hc', 'sat_ea_mr', 'sat_ew_hc', 'sat_ew_mr']
        cb = cb.drop(columns=[c for c in essay_cols if c in cb.columns])

        cb['schlcode'] = cb['schlcode'].astype(str).str.zfill(3)
        cb['unique_identifier'] = (
            cb['lea'].astype(str)
            + '-'
            + cb['schlcode'].astype(str)
            )

            
            #4 combine them into a master
            
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