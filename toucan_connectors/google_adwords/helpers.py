from typing import List

operators = [
    'EqualTo',
    'Contains',
    'ContainsAll',
    'ContainsAny',
    'ContainsIgnoreCase',
    'DoesNotContain',
    'GreaterThan',
    'GreaterThanOrEqualTo',
    'DoesNotContainIgnoreCase',
    'In',
    'LessThan',
    'LessThanOrEqualTo',
    'ContainsNone',
    'NotIn',
    'NotEqualTo',
    'StartsWith',
    'StartsWithIgnoreCase',
]


def apply_filter(query_builder, filter_dict: dict):
    for k, v in filter_dict.items():
        if v['operator'] in operators:
            getattr(query_builder.Where(k), v['operator'])(v['value'])
    return query_builder


def clean_columns(col_str: str) -> List[str]:
    # split col_str
    splitted = col_str.split(',')
    # lower the first letter
    splitted = [s.strip()[0].lower() + s.strip()[1:] for s in splitted]
    return splitted
