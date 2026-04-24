import pandas as pd 

# ----- Functions to use in the main etl pipeline process ----

def extract_data(path:str):

    df = pd.read_parquet(path)

    return df


def export_data(df:str, path:str):

    df.to_parquet(path)

