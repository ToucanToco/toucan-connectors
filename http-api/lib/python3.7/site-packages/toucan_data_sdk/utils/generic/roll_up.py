import pandas as pd


def roll_up(df, levels, groupby_vars, extra_groupby_cols=[],
            var_name='type', value_name='value', agg_func='sum',
            drop_levels=None):
    """
    Move the hierarchy from the columns name to the rows (like a melt).
    Add higher level hierarchy information with pandas aggregation function.

    One DatFrame per level (all will be concatenated in the end), group by
    levels, apply aggregation function on groupby_vars. Add two extra columns:
    value_name and var_name, like a pandas melt.

    Args:
        df (DataFrame): DataFrame to work on...
        levels (list): Hierarchy. The order is important, from the top level
            to the lower level.
        groupby_vars (list): Columns to select from the group by (apply
            aggregation function to)
        extra_groupby_cols (list): Add to columns to group by each time.
        var_name (str): Same as a pandas melt() var_name
        value_name (str): Same as a pandas melt() value_name
        agg_func (str): pandas aggregation function to apply to the groupby.
        drop_levels (list): the names of the levels that may you want to discard
            from the output

    Returns:
        DataFrame:

    """
    dfs = list()
    groupby_cols_cpy = list(levels)
    levels_cpy = list(levels)
    levels_cpy.reverse()
    if drop_levels is None:
        drop_levels = []
    previous_level = None
    for top_level in levels_cpy:
        # Aggregation
        gb_df = getattr(
            df.groupby(groupby_cols_cpy + extra_groupby_cols)[groupby_vars],
            agg_func)().reset_index()

        # Melt-like columns
        gb_df[var_name] = top_level
        gb_df[value_name] = gb_df[top_level]
        dfs.append(gb_df)
        if previous_level in drop_levels:
            del dfs[-2]
        previous_level = top_level

        # Remove one level each time in the groupby: lowest level column needs
        # a groupby with every levels, the next level needs every one except
        # the lowest, etc. until the top level column that needs only itself
        # inside the groupby.
        groupby_cols_cpy.pop()
    return pd.concat(dfs, sort=False).reset_index()
