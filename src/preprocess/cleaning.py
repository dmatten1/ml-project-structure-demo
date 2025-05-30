import os
import pandas as pd
from glob import glob
import re
from sklearn.preprocessing import MultiLabelBinarizer

raw_data_path = "data/raw"

# Step 1: Get all CSV files that contain either 'curtest' or 'collegeboard'
csv_files = [
    f for f in glob(os.path.join(raw_data_path, "*.csv"))
]

# Step 2: Group by prefix (gpa vs collegeboard)
gpa_files = [f for f in csv_files if os.path.basename(f).startswith("gpa")]
collegeboard_files = [f for f in csv_files if os.path.basename(f).startswith("collegeboard")]
curtest_files = [f for f in csv_files if os.path.basename(f).startswith("curtest")]

# Step 3: Load and clean each group

def clean_group_gpa(file_list):
    
    return df;

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
        df = pd.read_csv(file)

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

#Save 
gpa_master.to_csv("data/processed/gpa_master.csv", index=False)
collegeboard_master.to_csv("data/processed/collegeboard_master.csv", index=False)