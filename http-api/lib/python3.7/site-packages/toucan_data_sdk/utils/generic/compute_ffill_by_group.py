from toucan_data_sdk.utils.helpers import check_params_columns_duplicate


def compute_ffill_by_group(df, id_cols, reference_cols, value_col):
    """
    Compute ffill with groupby. There is a performance issue with a simple
    groupby/fillna (2017/07)
    - `id_cols` are the columns id to group,
    - `reference_cols` are the other columns used to order,
    - `value_col` is the name of the column to fill,

    Args:
        df (pd.DataFrame):
        id_cols (list(str)):
        reference_cols (list(str)):
        value_col (str):
    """
    check_params_columns_duplicate(id_cols + reference_cols + [value_col])
    df = df.sort_values(by=id_cols + reference_cols)
    df = df.set_index(id_cols)
    df['fill'] = 1 - df[value_col].isnull().astype(int)
    df['fill'] = df.groupby(
        level=list(range(0, len(id_cols) - 1))
    )['fill'].cumsum()
    df[value_col] = df[value_col].ffill()
    df.loc[df['fill'] == 0, value_col] = None
    del df['fill']
    return df.reset_index()
