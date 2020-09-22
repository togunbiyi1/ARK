import pandas as pd


def set_indexes(df):
    new_columns = df.columns.values
    new_columns[0] = 'Name'
    df.columns = new_columns
    df.set_index('Name', inplace=True)
    df.index.name = None
    return df