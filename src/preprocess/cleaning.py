import os
import pandas as pd
from glob import glob

# Step 1: Get all CSV files that contain either 'curtest' or 'collegeboard'
csv_files = [
    f for f in glob(os.path.join(raw_data_path, "*.csv"))
    if "curtest" in os.path.basename(f) or "collegeboard" in os.path.basename(f)
]

# Step 2: Group by prefix (gpa vs collegeboard)
gpa_files = [f for f in csv_files if os.path.basename(f).startswith("gpa")]
collegeboard_files = [f for f in csv_files if os.path.basename(f).startswith("collegeboard")]
curtest_files = [f for f in csv_files if os.path.basename(f).startswith("curtest")]

# Step 3: Load and clean each group

def clean_group_gpa(file_list):
    
    return df;

def clean_group_collegeboard(file_list):
    ;
    
def clean_group_curtest(file_list)

gpa_master = clean_group(gpa_files)
collegeboard_master = clean_group(collegeboard_files)

# Step 4: Save (optional)
os.makedirs("data/processed", exist_ok=True)
gpa_master.to_csv("data/processed/gpa_master.csv", index=False)
collegeboard_master.to_csv("data/processed/collegeboard_master.csv", index=False)