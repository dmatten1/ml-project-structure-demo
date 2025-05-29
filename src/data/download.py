import src.common.tools as tools
import pandas as pd
import os
from glob import glob

def load_sas_to_df(filepath: str) -> pd.DataFrame:
    """
    Load a .sas7bdat file into a pandas DataFrame.
    """
    return pd.read_sas(filepath, format='sas7bdat')

if __name__ == "__main__":
    config = tools.load_config()

    # Adjust this path depending on how the drive is mounted
    smb_mount_path = config["mountedremotepath"]  # e.g., '/Volumes/data_plus_student' or 'Z:\\'

    # Find all .sas7bdat files
    sas_files = glob(os.path.join(smb_mount_path, "*.sas7bdat"))

    for sas_file in sas_files:
        print(f"Processing: {sas_file}")
        df = load_sas_to_df(sas_file)

        # Save to CSV
        base_name = os.path.splitext(os.path.basename(sas_file))[0]
        csv_path = os.path.join(config["datarawdirectory"], f"{base_name}.csv")
        df.to_csv(csv_path, index=False)
        print(f"Saved CSV to: {csv_path}")
