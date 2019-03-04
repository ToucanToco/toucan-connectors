import pandas as pd


def waterfall(df, date, value, start, end, upperGroup,  # noqa:C901
              insideGroup=None, filters=None):
    """
    Return a line for each bars of a waterfall chart, totals, groups, subgroups.
    Compute the variation and variation rate for each line.

    Args:
        upperGroup (dict)
            - id: name of the column that contains upperGroups unique IDs
            - label: not required, text displayed under each upperGroups bars,
                     using ID when it's absent
            - groupsOrder: not required, order of upperGroups
        insideGroup (dict)
            - id: name of the column that contains insideGroups unique IDs
            - label: not required, text displayed under each insideGroups bars,
                     using ID when it's absent
            - groupsOrder: not required, order of insideGroups
        date (str): name of the column that id the period of each lines
        value (str): name of the column that contains the vaue for each lines
        start (dict):
            - label: text displayed under the first master column
            - date: value in the date col that id lines for the first period
        end (dict):
            - label: text displayed under the last master column
            - date: value in the date col that id lines for the second period
        filters (list or str) : list of column to filters on
        # fillValues (bool): (case when false is not implemented)
    """

    if len(df) == 0:
        return df

    if filters is not None:
        if isinstance(filters, str):
            filters = [filters]

        def sub_waterfall(df):
            wa_df = waterfall(df, date, value, start, end, upperGroup, insideGroup)
            for filters_col in filters:
                wa_df[filters_col] = df[filters_col].values[0]
            return wa_df

        # filters df into a list of sub_df
        list_of_sub_df = [df[(df[filters].values == i).all(axis=1)]
                          for i in df[filters].drop_duplicates().values]

        return pd.concat([sub_waterfall(df) for df in list_of_sub_df], sort=False)

    groups = {
        'upperGroup': {
            'type': 'parent',
            'id':  'upperGroup',
            'order': {
                'by': ['upperGroup_order', 'groups'],
                'ascending': [True, True]
            },
            'obj': upperGroup
        }
    }
    if insideGroup is not None:
        groups['insideGroup'] = {
            'type': 'child',
            'id': 'insideGroup',
            'order': {
                'by': ['type', 'insideGroup_order', 'label'],
                'ascending': [False, True, True]
            },
            'obj': insideGroup
        }
    # prepare the dataframe with standard column names
    df = _compute_rename(df, date, value, groups)

    agg_conf = {'value': sum}
    agg_conf.update({f'{col}_label': 'first' for col in groups.keys()})
    agg_conf.update({f'{col}_order': 'first' for col in groups.keys()})
    df = df.groupby(list(groups.keys()) + ['date']).agg(agg_conf).reset_index()

    df_start, df_end = _compute_start_end(df, start, end)

    df = _compute_value_diff(df, start, end, groups)

    middle = _compute_upper_group(df)
    if insideGroup is not None:
        middle = pd.concat([middle, _compute_inside_group(df)])

    ret = _compute_order(df_start, df_end, middle, groups)

    return ret


def _compute_rename(df, date, value, groups):
    df = df.rename(columns={
        date: 'date',
        value: 'value'
    })
    for g_name, g in groups.items():
        df = df.rename(columns={g['obj']['id']: g_name})
        if 'label' not in g['obj']:
            df[f'{g_name}_label'] = df[g_name]
        else:
            df.rename(columns={g['obj']['label']: f'{g_name}_label'}, inplace=True)
        if 'groupsOrder' not in g['obj']:
            df[f'{g_name}_order'] = pd.np.nan
        else:
            df.rename(columns={g['obj']['groupsOrder']: f'{g_name}_order'}, inplace=True)
    return df


def _compute_start_end(df, start, end):
    """
    Compute two dataframes with value for start and end
    Args:
        totals(dataframe):

    Returns: Dataframe, Dataframe

    """
    result = {}
    time_dict = {'start': start, 'end': end}
    totals = df.groupby('date').agg({'value': sum}).reset_index()
    for time_name, time in time_dict.items():
        if not totals[totals['date'] == time['id']].empty:
            value = totals.loc[
                totals['date'] == time['id'], 'value'
            ].values[0]
        else:
            value = 0
        result[time_name] = pd.DataFrame([{
            'value': value,
            'label': time['label'],
            'groups': time['label']
        }])
    return result['start'], result['end']


def _compute_value_diff(df, start, end, groups):
    """
    Compute diff value between start and end
    Args:
        df(dataframe):

    Returns: Dataframe

    """
    start_values = df[df['date'] == start['id']].copy()
    end_values = df[df['date'] == end['id']].copy()

    merge_on = []
    for key, group in groups.items():
        merge_on = merge_on + [key, f'{key}_label', f'{key}_order']

    df = start_values.merge(end_values,
                            on=merge_on,
                            how='outer',
                            suffixes=('_start', '_end'), )

    # necessary before calculating variation
    df[['value_start', 'value_end']] = df[['value_start', 'value_end']].fillna(0)
    df['value'] = df['value_end'] - df['value_start']
    df.drop(['date_start', 'date_end', 'value_end'], axis=1, inplace=True)
    df.rename(columns={'upperGroup': 'groups'}, inplace=True)
    return df


def _compute_inside_group(df):
    """
    Compute inside Group
    Args:
        df(dataframe):

    Returns: Dataframe

    """
    inside_group = df.copy()
    inside_group['type'] = 'child'
    inside_group['variation'] = inside_group['value'] / inside_group[
        'value_start']
    inside_group.drop(['upperGroup_label', 'insideGroup', 'value_start'],
                      axis=1, inplace=True)
    inside_group.rename(columns={'insideGroup_label': 'label'},
                        inplace=True)
    return inside_group


def _compute_upper_group(df):
    """
    Compute upperGroup
    Args:
        df (Dataframe):

    Returns: Dataframe

    """
    upper_group = df.groupby(['groups']).agg({
        'value': sum,
        'value_start': sum,
        'upperGroup_label': 'first',
        'upperGroup_order': 'first'
    }).reset_index()
    upper_group['type'] = 'parent'
    upper_group['variation'] = upper_group['value'] / upper_group[
        'value_start']
    upper_group.drop(['value_start'], axis=1, inplace=True)
    upper_group.rename(columns={'upperGroup_label': 'label'}, inplace=True)
    return upper_group


def _compute_order(df_start, df_end, df_middle, groups):
    order = {'by': [], 'ascending': []}
    for key, group in groups.items():
        order['by'] = order['by'] + group['order']['by']
        order['ascending'] = order['ascending'] + group['order']['ascending']

    df_middle = df_middle.sort_values(**order)

    ret = pd.concat([df_start, df_middle, df_end])

    for key, elt in groups.items():
        cond = ret['type'] == elt['type']
        ret.loc[cond, 'order'] = ret.loc[cond, f'{key}_order']
        ret.drop([f'{key}_order'], axis=1, inplace=True)
    return ret
