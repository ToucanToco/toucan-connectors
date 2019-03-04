from toucan_data_sdk.utils.helpers import (
    check_params_columns_duplicate,
    ParamsValueError
)


def compute_cumsum(
    df,
    id_cols,
    reference_cols,
    value_cols,
    new_value_cols=None,
    cols_to_keep=None
):
    """
    Compute cumsum for a group of columns.
    - `id_cols` are the columns id to create each group,
    - `reference_cols` are the columns to order the cumsum,
    - `value_cols` are the columns to cumsum,
    - `new_value_cols` are the new columns with the result cumsum
    - `cols_to_keep` are other column to keep in the dataframe. This option can
     be used if there is only one row by group [id_cols + reference_cols]

    For example :

    MONTH  DAY NAME  VALUE  X
     1      1    A      1  lo
     2      1    A      1  lo
     2     15    A      1  la
     1     15    B      1  la

    The function `compute_cumsum` with the arguments :
            id_cols=['NAME']
            reference_cols=['MONTH','DAY']
            cumsum_cols=['VALUE']
            cols_to_keep=['X']
    give as a result :


    NAME  MONTH  DAY  X  VALUE
     A     1      1  lo      1
     A     2      1  la      2
     A     2     15  lo      3
     B     1     15  la      1


    Args:
        df (pd.DataFrame):
        id_cols (list(str)):
        reference_cols (list(str)):
        value_cols (list(str)):
        new_value_cols (list(str)):
        cols_to_keep (list(str)):
    """
    if cols_to_keep is None:
        cols_to_keep = []

    if new_value_cols is None:
        new_value_cols = value_cols
    if len(value_cols) != len(new_value_cols):
        raise ParamsValueError('`value_cols` and `new_value_cols` needs '
                               'to have the same number of elements')

    check_params_columns_duplicate(id_cols + reference_cols + cols_to_keep + value_cols)

    levels = list(range(0, len(id_cols)))

    df = df.groupby(id_cols + reference_cols + cols_to_keep).sum()
    df[new_value_cols] = df.groupby(level=levels)[value_cols].cumsum()

    return df.reset_index()
