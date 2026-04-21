import pandas as pd

def extract_data(file_name):
    return pd.read_csv(
        file_name,
        encoding="latin1",
        on_bad_lines="skip"
    )
