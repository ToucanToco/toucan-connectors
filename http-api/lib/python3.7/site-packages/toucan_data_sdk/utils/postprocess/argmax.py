def argmax(df, column):
    df = df[df[column] == df[column].max()]
    return df


def argmin(df, column):
    df = df[df[column] == df[column].min()]
    return df
