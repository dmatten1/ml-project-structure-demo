import os
from glob import glob
import pandas as pd
import src.common.tools as tools
from collections import defaultdict

def group_files_by_keyword(file_list, keywords):
    """
    Groups file paths by keyword (e.g. 'curtest', 'transcripts').
    Returns a dict: { keyword: [file1.csv, file2.csv, ...] }
    """
    grouped = defaultdict(list)
    for file in file_list:
        base_name = os.path.basename(file).lower()
        for keyword in keywords:
            if keyword in base_name:
                grouped[keyword].append(file)
                break
    return grouped

def combine_and_save(file_group, output_dir, keyword):
    """
    Combines all files in file_group and saves them as one CSV.
    """
    dfs = []
    for file in file_group:
        print(f"Reading: {file}")
        df = pd.read_csv(file)
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    out_path = os.path.join(output_dir, f"{keyword}_combined.csv")
    combined_df.to_csv(out_path, index=False)
    print(f"Saved combined {keyword} to: {out_path}")

if __name__ == "__main__":
    config = tools.load_config()
    cleaned_data_path = config["datacleandirectory"]
    combined_output_path = config["datacombineddirectory"]
    os.makedirs(combined_output_path, exist_ok=True)

    # Set your keywords here
    keywords = ["curtest", "transcripts","attendance","grad","collegeboard","mergedemo","mastdrop","mastsusp"]

    all_cleaned_files = glob(os.path.join(cleaned_data_path, "*.csv"))
    grouped_files = group_files_by_keyword(all_cleaned_files, keywords)

    for keyword, file_group in grouped_files.items():
        combine_and_save(file_group, combined_output_path, keyword)
