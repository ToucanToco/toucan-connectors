import pandas as pd


def append_dataframes(df: "pd.DataFrame", second_df: "pd.DataFrame") -> "pd.DataFrame":
    if pd.__version__[0] == "2":
        return df.concat(second_df)
    else:
        return df.append(second_df)
