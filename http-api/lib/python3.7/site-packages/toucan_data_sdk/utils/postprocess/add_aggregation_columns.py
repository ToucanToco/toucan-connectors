from typing import Dict, List, Union

Agg = Dict[str, str]  # dict of size 1: mapping colomn -> aggregation function


def add_aggregation_columns(
        df, *,
        group_cols: Union[str, List[str]],
        aggregations: Dict[str, Agg]
):
    """
    df =
       year  group libelle      mois  value  total
    0  2018  e-EKO       a  20180101      3      3
    1  2018  e-EKO       b  20180101      0      3
    2  2018  e-EKO       c  20180101      4      7
    3  2018    DOQ       q  20180101     33     33
    4  2018    DOQ       b  20180101     54     87
    5  2018     DO       q  20180101     13     13
    6  2018     DO       c  20180101     14     27

    df = groupby_append(
        df,
        ['mois', 'group'],
        {
            'max_total': {'total': 'max'},
            'sum_total': {'total': 'sum'},
            'mean_value': {'value': 'mean'}
        }
    )

    df =
       year  group libelle      mois  value  total  max_total  sum_total  mean_value
    0  2018  e-EKO       a  20180101      3      3          7         13    2.333333
    1  2018  e-EKO       b  20180101      0      3          7         13    2.333333
    2  2018  e-EKO       c  20180101      4      7          7         13    2.333333
    3  2018    DOQ       q  20180101     33     33         87        120   43.500000
    4  2018    DOQ       b  20180101     54     87         87        120   43.500000
    5  2018     DO       q  20180101     13     13         27         40   13.500000
    6  2018     DO       c  20180101     14     27         27         40   13.500000
    """
    group = df.groupby(group_cols)
    for new_col, aggs in aggregations.items():
        assert len(aggs) == 1
        (col, agg), *_ = aggs.items()
        df[new_col] = group[col].transform(agg)
    return df
