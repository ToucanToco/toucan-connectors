import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pandas import DataFrame, Series


INTERNAL_SEP = str(uuid.uuid1())


def _first_valid_value(serie: "Series") -> Any:
    first_valid_index = serie.first_valid_index()
    return serie[first_valid_index] if first_valid_index is not None else None


def json_to_table(df: "DataFrame", columns: str | list[str], sep: str = ".") -> "DataFrame":
    """
    Flatten JSON into a table shape. Add lines for each element of a nested array.
    Add columns for each keys of a nested object / dict.

    ### Parameters

    *mandatory*
    - `columns` (*list*) : topmost level key containing nested objects
    *optional :*
    - `sep` (*str*) : separator used to build nested objects path in final output column names
                      (default is `.`)
    """
    from pandas import json_normalize

    if isinstance(columns, str):  # support for a single column name as a string
        columns = [columns]

    merge_on = [c for c in df.columns if not isinstance(_first_valid_value(df[c]), list | dict)]
    if merge_on == []:
        raise ValueError("Data should have at least one column with simple data type (not list or dict)")

    data = df.to_dict(orient="records")  # json_normalize takes python objects as input
    ret_data = df.copy()

    for col in columns:
        serie = df[col]
        first_valid_value = _first_valid_value(serie)

        if not isinstance(first_valid_value, list | dict):
            continue

        elif isinstance(first_valid_value, dict):  # creates new columns
            df_nz = json_normalize(data=data, sep=INTERNAL_SEP)

        elif isinstance(first_valid_value, list):  # creates new lines
            df_nz = json_normalize(
                data=data,
                meta=merge_on,  # type:ignore[arg-type]
                record_path=col,
                record_prefix=f"{col}{INTERNAL_SEP}",
                sep=INTERNAL_SEP,
            )

        # which columns were added ?
        new_cols = [c for c in df_nz.columns if c.startswith(f"{col}{INTERNAL_SEP}")]

        # which columns still need to be processed ?
        compound_types_cols = [c for c in new_cols if isinstance(_first_valid_value(df_nz[c]), list | dict)]

        ret_data = (
            df_nz[[c for c in df_nz.columns if c in merge_on or c in new_cols]]
            .rename(columns={c: c.replace(INTERNAL_SEP, sep) for c in df_nz.columns})
            .merge(ret_data)
        )

        if compound_types_cols != []:
            ret_data = json_to_table(df=ret_data, columns=[c.replace(INTERNAL_SEP, sep) for c in compound_types_cols])

    return ret_data
