import pandas as pd


def date_requester_generator(start_date, end_date, frequency,
                             format='%Y-%m-%d', granularities=None,
                             others_format=None, times_delta=None):
    """
    Return a DataFrame containing at least 3 columns :
    - "DATE" : Label of date
    - "DATETIME" : Date in datetime dtype
    - "GRANULARITY" : Granualrity of date

    Arguments
    #########

    Mandatory :
    -----------
    - start_date (str) : start date in %Y-%m%d format
    - end_date (str): end date in %Y-%m%d format
    - frequency (str) : http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

    Optional :
    ----------
    - format: format of the date. !!! only use if granularity is None
        NB : same format in Toucan Toco
        Example : '%d/%m/%Y'
        https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
    - granularities (dict):
        - keys : name of the granularity
        - values (str): Format of the granularity.
            NB : same format in Toucan Toco
            Example : '%d/%m/%Y'
            https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
    - others_format (dict) : Add new columns for each key
        - key (str) : name of the column
        - values (str): format of the granularity.
            NB : same format in Toucan Toco
            Example : '%d/%m/%Y'
            https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
    - times_delta (dict) : Add new columns for each key
        - key (str) : name of the column
        - values (str): time delta
            Examples : '+1 day' '+3 day' '-4 month'
    """

    granularities = granularities or {'date': format}
    others_format = others_format or {}
    times_delta = times_delta or {}

    # Base DataFrame
    columns_list = ['DATE', 'DATETIME', 'GRANULARITY', *others_format, *times_delta]
    result_df = {col_name: [] for col_name in columns_list}

    # Generate the range
    date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)

    for granularity_name, granularity_format in granularities.items():
        date_range_label = date_range.strftime(granularity_format)
        a = list(set(date_range_label))
        index_unique = list(set([a.index(x) for x in date_range_label]))
        date_range_datetime = date_range[index_unique]
        date_range_label = date_range_label.unique()

        result_df['DATE'] += list(date_range_label)
        result_df['DATETIME'] += list(date_range_datetime)
        result_df['GRANULARITY'] += [granularity_name] * len(date_range_label)

        for col_name, other_format in others_format.items():
            result_df[col_name] += list(date_range_datetime.strftime(other_format))

        for col_name, time_delta in times_delta.items():
            result_df[col_name] += list((date_range_datetime + pd.Timedelta(time_delta))
                                        .strftime(granularity_format))

    return pd.DataFrame(result_df)
