import src.common.tools as tools
import src.data.dataio as dataio
import os
import glob
import pandas as pd
import re



def clean(csv):
    config = tools.load_config()
    base_name = os.path.basename(csv).lower()
    df = pd.read_csv(csv)
    
    if "curtest" in base_name:
        curtest_master = df
        # Step 1: Count unique LEA values per mastid
        lea_counts = curtest_master.groupby('mastid')['lea'].nunique()

        # Step 2: Keep mastids with only one unique LEA
        consistent_mastids = lea_counts[lea_counts == 1].index

        # Step 3: Filter the original DataFrame
        curtest_master = curtest_master[curtest_master['mastid'].isin(consistent_mastids)]
        curtest_master = curtest_master[~curtest_master['test_id'].str.startswith('X')] #very few of these tests
        curtest_master = curtest_master[~curtest_master['test_id'].str.startswith('A')] #dont want ACTs
        curtest_master = curtest_master[~curtest_master['test_id'].str.startswith('RD3')] #dont want ACTs
        curtest_master['year'] = pd.to_datetime(curtest_master['testdt']).dt.year
        curtest_master['testid_year'] = curtest_master['test_id'] + "_" + curtest_master['year'].astype(str)
        #cleaning for different tests
        counts = curtest_master['testid_year'].value_counts()

        # Identify combos with at least 2000 entries
        valid_combos = counts[counts >= 2000].index

        # Keep only rows with valid combos
        curtest_master = curtest_master[curtest_master['testid_year'].isin(valid_combos)]
        curtest_master['percentile'] = curtest_master.groupby(['test_id', 'year'])['score'] \
            .transform(lambda x: x.rank(pct=True) * 100)
        #accomodation fixing
        # 1. Fill NaNs and split comma-separated values into lists
        curtest_master['accomm_list'] = (
            curtest_master['accomm_list']
            .fillna('')
            .str.split(',')
            .apply(lambda items: [item.strip() for item in items if item.strip()])
        )

        # 2. Use MultiLabelBinarizer to one-hot encode
        from sklearn.preprocessing import MultiLabelBinarizer

        mlb = MultiLabelBinarizer()
        accom_dummies = pd.DataFrame(
            mlb.fit_transform(curtest_master['accomm_list']),
            columns=mlb.classes_,
            index=curtest_master.index
        )

        # 3. Join one-hot columns back to the original DataFrame
        curtest_master = pd.concat([curtest_master.drop(columns=['accomm_list']), accom_dummies], axis=1)
        curtest_master = curtest_master.drop(columns='pc')
        curtest_master = curtest_master[curtest_master['exemption_code'] != 'D']
        curtest_master.drop(columns="exemption_code",inplace=True)
        curtest_master['TRA'] = curtest_master[['TRA', 'RAL']].max(axis=1)
        curtest_master.drop(columns='RAL',inplace=True)
        curtest_master = curtest_master.drop_duplicates()
        #Pivot
        # Step 1: Make sure 'year' exists
        curtest_master['year'] = pd.to_datetime(curtest_master['testdt']).dt.year

        # Step 2: Create a unique label for each test attempt
        curtest_master['year_test'] = (
            curtest_master['year'].astype(str) + '_' +
            curtest_master['test_id'].astype(str)
        )

        # Step 3: Pivot table with percentiles
        pivot_percentile = curtest_master.pivot_table(
            index='mastid',
            columns='year_test',
            values='score', #could change from percentile to score?????
            aggfunc='first' 
        )

        # Step 4: Clean up
        pivot_percentile.columns.name = None
        pivot_percentile.reset_index(inplace=True)
        
        #now resave it!
        cleaned_df = pivot_percentile
        output_name = "curtest_master.csv"
        output_path = os.path.join(config["datacleandirectory"], output_name)
        cleaned_df.to_csv(output_path, index=False)
        print(f"Saved cleaned file to {output_path}")
        
    #PUT OTHER FILES HERE
        
        
        
        
        
        
   