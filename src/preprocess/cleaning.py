import os
import pandas as pd
from glob import glob
import re
from sklearn.preprocessing import MultiLabelBinarizer
from dateutil.parser import parse

raw_data_path = "data/raw"

def fast_parse_dates(series):
    # Try general parse first (fastest)
    parsed = pd.to_datetime(series, errors='coerce')

    # Optionally: fallback to common format if still missing
    fallback = pd.to_datetime(series, format='%m/%d/%Y', errors='coerce')
    parsed = parsed.fillna(fallback)

    return parsed.dt.year
    
# Step 1: Get all CSV files
csv_files = [
    f for f in glob(os.path.join(raw_data_path, "*.csv"))
]

# Step 2: Group by prefix (gpa vs collegeboard)
gpa_files = [f for f in csv_files if os.path.basename(f).startswith("gpa")]
collegeboard_files = [f for f in csv_files if os.path.basename(f).startswith("collegeboard")]
curtest_files = [f for f in csv_files if os.path.basename(f).startswith("curtest")]
demographics_files = [f for f in csv_files if os.path.basename(f).startswith("mergedemo")]
transcript_files = [f for f in csv_files if os.path.basename(f).startswith("transcripts")]
masterbuild_files = [f for f in csv_files if os.path.basename(f).startswith("masterbuild")]

# Step 3: Load and clean each group
def clean_transcripts(file_list):
    dfs = [pd.read_csv(f) for f in file_list]
    tr_master = pd.concat(dfs, ignore_index=True)
     
    tr_master = tr_master[["mastid", "lea", "schlcode", "GRADE","FINAL_MARK", "ACADEMIC_LEVEL_DESC"]]
    tr_master = tr_master.map(lambda x: x[2:-1] if isinstance(x, str) and x.startswith('b') else x)
    tr_master.dropna(inplace=True)
    tr_master["ACADEMIC_LEVEL_DESC"] = tr_master["ACADEMIC_LEVEL_DESC"].apply(lambda x: x[4:])
    
    return tr_master
     
     
def clean_group_gpa(file_list):
    # Read all CSVs in the file list into DataFrames
    dfs = [pd.read_csv(f) for f in file_list]

    # Concatenate into one DataFrame
    gpa_master = pd.concat(dfs, ignore_index=True)

    # Decode any string that looks like a byte string (e.g., "b'some value'")
    gpa_master = gpa_master.map(lambda x: x[2:-1] if isinstance(x, str) and x.startswith("b'") and x.endswith("'") else x)
    # Combine the columns, prioritizing non-null values in 'bound_for'
    gpa_master['bound_for_combined'] = gpa_master['bound_for'].combine_first(gpa_master['BOUND_FOR'])

    # Drop the original columns
    gpa_master.drop(columns=['bound_for', 'BOUND_FOR'], inplace=True)

    # Rename to a standard name (optional)
    gpa_master.rename(columns={'bound_for_combined': 'bound_for'}, inplace=True)


    gpa_master.drop(columns=['DIPLOMA_MET','UNWEIGHTED_RANK_DATE','WEIGHTED_RANK_DATE','diploma_type','DIPLOMA_TYPE'], inplace=True)


    gpa_master['entry_year'] = fast_parse_dates(gpa_master['NINTHGRADEENTRY'])
    gpa_master['grad_year'] = fast_parse_dates(gpa_master['DIPLOMA_ISSUED'])


    gpa_master.drop(columns=['NINTHGRADEENTRY','DIPLOMA_ISSUED'],inplace=True)
    gpa_master.dropna(inplace=True)
    gpa_master = gpa_master[(gpa_master['gpa_unweighted'] <= 4) & (gpa_master['gpa_weighted'] <= 6)]
    gpa_master = gpa_master[(gpa_master['entry_year'] < 2030)]      
    
    return gpa_master

def clean_group_demographics(file_list):
    common_cols = None
    for path in sorted(file_list):
        # Read just the header row
        hdr = pd.read_csv(path, nrows=0)
        
        # Normalize: strip whitespace, lowercase, replace spaces with underscores
        cols = (
            hdr.columns
               .str.strip()
               .str.lower()
               .str.replace(" ", "_")
        )
        
        if common_cols is None:
            # On the very first file, initialize common_cols to all of its normalized columns
            common_cols = list(cols)
        else:
            # For each subsequent file, keep only those names that are still in common_cols
            cols_set = set(cols)
            common_cols = [c for c in common_cols if c in cols_set]
    
    # Now 'common_cols' is a list of normalized column names that appear in every file,
    # in the order they first showed up in the first CSV’s header.

    # 2) Load each DataFrame a second time, normalize its columns, then subset to common_cols
    cleaned_dfs = []
    for path in sorted(file_list):
        df = pd.read_csv(path)
        
        # Normalize this file’s column names exactly the same way
        df.columns = (
            df.columns
               .str.strip()
               .str.lower()
               .str.replace(" ", "_")
        )
        
        # Subset to only the columns in the intersection
        df = df.reindex(columns=common_cols)
        
        cleaned_dfs.append(df)
    
    # 3) Concatenate all cleaned DataFrames into one master table
    demographics_master = pd.concat(cleaned_dfs, ignore_index=True)
    
    demographics_master['mastid'] = (
    demographics_master['mastid']
    .round(0)
    .astype(int)
    )
    
    demographics_master['reporting_year'] = (
    demographics_master['reporting_year']
    .round(0)
    .astype(int)
    )
    
    # 4) Ensure `lea` and `schlcode` are strings (for concatenation)
    demographics_master['lea'] = demographics_master['lea'].astype(str).str.strip()
    demographics_master['schlcode'] = demographics_master['schlcode'].astype(str).str.zfill(3)

    # 5) Create `unique_identifier` by concatenating `lea` and `schlcode`
    demographics_master['unique_identifier'] = (
        demographics_master['lea'] + '-' + demographics_master['schlcode']
    )
    demographics_master.dropna(inplace=True)
    return demographics_master
    

    
def clean_group_collegeboard(file_list):
    cleaned_dfs = []

    # 1) discover the union of columns, in order of appearance
    rename_map = {
        'sat_erw_score_hc': 'sat_ebrw_score_hc',
        'sat_erw_score_mr': 'sat_ebrw_score_mr',
        'sat_ctsh_ss_mc':   'sat_ctsh_ss_hc',
        'ital':             'italgr'
    }
    all_cols = []
    for path in sorted(file_list):
        hdr = pd.read_csv(path, nrows=0)
        cols = (
            hdr.columns
               .str.strip()
               .str.lower()
               .str.replace(" ", "_")
               .map(lambda c: rename_map.get(c, c))
        )
        for c in cols:
            if c not in all_cols:
                all_cols.append(c)

    # 2) load & clean each file, align to all_cols
    for path in sorted(file_list):
        df = pd.read_csv(path, low_memory=False)

        # normalize & rename
        df.columns = (
            df.columns
               .str.strip()
               .str.lower()
               .str.replace(" ", "_")
        )
        df.rename(columns=rename_map, inplace=True)

        # drop dupes, align to the same column set
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.reindex(columns=all_cols)

        cleaned_dfs.append(df)

    # 3) concatenate & post‐process
    collegeboard_master = pd.concat(cleaned_dfs, ignore_index=True)

    # strip whitespace in LEA
    if 'lea' in collegeboard_master:
        collegeboard_master['lea'] = collegeboard_master['lea'].astype(str).str.strip()

    # round floats → nullable Int
    float_cols = collegeboard_master.select_dtypes(include='float').columns
    collegeboard_master[float_cols] = (
        collegeboard_master[float_cols]
        .round()
        .astype('Int64')
    )

    # parse dates
    for col in collegeboard_master:
        if col in ['birthdt', 'grad_date'] or col.endswith('_dt'):
            collegeboard_master[col] = pd.to_datetime(
                collegeboard_master[col], errors='coerce'
            )

    # categorical flags
    for col in ['sex', 'ethnic', 'blang', 'lea', 'instname']:
        if col in collegeboard_master:
            collegeboard_master[col] = collegeboard_master[col].astype('category')

    # drop any SAT‐essay columns
    for essay_col in [
        'sat_er_hc','sat_er_mr',
        'sat_ea_hc','sat_ea_mr',
        'sat_ew_hc','sat_ew_mr'
    ]:
        if essay_col in collegeboard_master:
            collegeboard_master.drop(columns=essay_col, inplace=True)

    # zero‐pad school code & build unique id
    if 'schlcode' in collegeboard_master:
        collegeboard_master['schlcode'] = (
            collegeboard_master['schlcode']
            .astype(str)
            .str.zfill(3)
        )
    if {'lea','schlcode'}.issubset(collegeboard_master.columns):
        collegeboard_master['unique_identifier'] = (
            collegeboard_master['lea'].astype(str)
            + '-'
            + collegeboard_master['schlcode']
        )

    return collegeboard_master



def clean_group_curtest(file_list):
    cleaned_dfs = []

    for file in file_list:
        print("doing" + file)
        df = pd.read_csv(file)
        df = df.map(lambda x: x[2:-1] if isinstance(x, str) and x.startswith("b'") and x.endswith("'") else x)
        # Drop rows with missing mastid
        if 'mastid' in df.columns:
            df = df.dropna(subset=['mastid'])

        # Rename columns
        df.rename(columns={
            'ACH_LEVEL': 'ach_level',
            'COLLECTION_CODE': 'collection_code',
            'TEST_ID': 'test_id',
            'EXEMPTION_CODE': 'exemption_code',
            'SCORE': 'score',
            'ACCOMM_LIST': 'accomm_list'
        }, inplace=True)

        # Drop unnecessary columns
        for col in ['admindt', 'test_schl', 'test_lea', 'collection_code']:
            if col in df.columns:
                df.drop(columns=col, inplace=True)

        # Handle ach_level
        if 'ach_level' in df.columns:
            df.dropna(subset=['ach_level'], inplace=True)
            df['ach_level'] = df['ach_level'].apply(recode_ach_level)
            df.dropna(subset=['ach_level'], inplace=True)
            df['ach_level'] = df['ach_level'].astype(int)

        cleaned_dfs.append(df)

    # Combine all cleaned DataFrames
    curtest_master = pd.concat(cleaned_dfs, ignore_index=True)

    # Filter mastids with only one LEA
    lea_counts = curtest_master.groupby('mastid')['lea'].nunique()
    consistent_mastids = lea_counts[lea_counts == 1].index
    curtest_master = curtest_master[curtest_master['mastid'].isin(consistent_mastids)]

    # Filter out unwanted test_id prefixes
    curtest_master = curtest_master[~curtest_master['test_id'].str.startswith(('X', 'A', 'RD3'))]

    # Add year and testid_year
    curtest_master['year'] = pd.to_datetime(curtest_master['testdt']).dt.year
    curtest_master['testid_year'] = curtest_master['test_id'] + "_" + curtest_master['year'].astype(str)

    # Filter testid_years with >= 2000 entries
    valid_combos = curtest_master['testid_year'].value_counts()
    valid_combos = valid_combos[valid_combos >= 2000].index
    curtest_master = curtest_master[curtest_master['testid_year'].isin(valid_combos)]

    # Add percentile rank
    # Convert scores to numeric (strings will become NaN)
    curtest_master['score'] = pd.to_numeric(curtest_master['score'], errors='coerce')

    # Now apply the ranking
    curtest_master['percentile'] = curtest_master.groupby(['test_id', 'year'])['score'] \
        .transform(lambda x: x.rank(pct=True) * 100)

        # One-hot encode accomm_list
    curtest_master['accomm_list'] = (
        curtest_master['accomm_list']
        .fillna('')
        .str.split(',')
        .apply(lambda items: [item.strip() for item in items if item.strip()])
    )
    mlb = MultiLabelBinarizer()
    accom_dummies = pd.DataFrame(
        mlb.fit_transform(curtest_master['accomm_list']),
        columns=mlb.classes_,
        index=curtest_master.index
    )
    curtest_master = pd.concat([curtest_master.drop(columns=['accomm_list']), accom_dummies], axis=1)

    # More column cleanup
    if 'pc' in curtest_master.columns:
        curtest_master = curtest_master.drop(columns='pc')
    if 'exemption_code' in curtest_master.columns:
        curtest_master = curtest_master[curtest_master['exemption_code'] != 'D']
        curtest_master.drop(columns='exemption_code', inplace=True)

    if 'TRA' in curtest_master.columns and 'RAL' in curtest_master.columns:
        curtest_master['TRA'] = curtest_master[['TRA', 'RAL']].max(axis=1)
        curtest_master.drop(columns='RAL', inplace=True)

    curtest_master = curtest_master.drop_duplicates()

    # Final pivot label
    curtest_master['year_test'] = curtest_master['year'].astype(str) + '_' + curtest_master['test_id'].astype(str)

    return curtest_master

def clean_masterbuild(file_list):
    dfs = [pd.read_csv(f) for f in file_list]

    # Concatenate into one DataFrame
    masterbuild_master = pd.concat(dfs, ignore_index=True)
    return masterbuild_master
    
def recode_ach_level(val):
    if pd.isna(val):
        return None
    if val == "Not Proficient":
        return 0
    match = re.search(r"[1-5]", str(val))
    if match:
        return int(match.group())
    return None
    
    
    
    
    
##need a master for each group and a save

gpa_master = clean_group_gpa(gpa_files)
collegeboard_master = clean_group_collegeboard(collegeboard_files)
curtest_master = clean_group_curtest(curtest_files)
transcript_master = clean_transcripts(transcript_files)
masterbuild_master = clean_masterbuild(masterbuild_files)
#no real masterbuild cleaning here -- its in masterbuild.ipynb locally

#Save 
gpa_master.to_csv("data/processed/gpa_master.csv", index=False)
collegeboard_master.to_csv("data/processed/collegeboard_master.csv", index=False)
curtest_master.to_csv("data/processed/curtest_master.csv", index=False)
transcript_master.to_csv("data/processed/transcript_master.csv", index=False)
masterbuild_master.to_csv("data/processed/masterbuild_master.csv", index=False)
