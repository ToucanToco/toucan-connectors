from typing import List


def apply_filter(query_builder, filter_dict: dict):
    for k, v in filter_dict.items():
        if v['operator'] == 'EqualTo':
            query_builder.Where(k).EqualTo(v['value'])
        elif v['operator'] == 'Contains':
            query_builder.Where(k).Contains(v['value'])
        elif v['operator'] == 'ContainsAll':
            query_builder.Where(k).ContainsAll(v['value'])
        elif v['operator'] == 'ContainsAny':
            query_builder.Where(k).ContainsAny(v['value'])
        elif v['operator'] == 'ContainsIgnoreCase':
            query_builder.Where(k).ContainsIgnoreCase(v['value'])
        elif v['operator'] == 'DoesNotContain':
            query_builder.Where(k).DoesNotContain(v['value'])
        elif v['operator'] == 'GreaterThan':
            query_builder.Where(k).GreaterThan(v['value'])
        elif v['operator'] == 'GreaterThanOrEqualTo':
            query_builder.Where(k).GreaterThanOrEqualTo(v['value'])
        elif v['operator'] == 'DoesNotContainIgnoreCase':
            query_builder.Where(k).DoesNotContainIgnoreCase(v['value'])
        elif v['operator'] == 'In':
            query_builder.Where(k).In(v['value'])
        elif v['operator'] == 'LessThan':
            query_builder.Where(k).LessThan(v['value'])
        elif v['operator'] == 'LessThanOrEqualTo':
            query_builder.Where(k).LessThanOrEqualTo(v['value'])
        elif v['operator'] == 'ContainsNone':
            query_builder.Where(k).ContainsNone(v['value'])
        elif v['operator'] == 'NotIn':
            query_builder.Where(k).NotIn(v['value'])
        elif v['operator'] == 'NotEqualTo':
            query_builder.Where(k).NotEqualTo(v['value'])
        elif v['operator'] == 'StartsWith':
            query_builder.Where(k).StartsWith(v['value'])
        elif v['operator'] == 'StartsWithIgnoreCase':
            query_builder.Where(k).StartsWithIgnoreCase(v['value'])
    return query_builder


def clean_columns(col_str: str) -> List[str]:
    # split col_str
    splitted = col_str.split(',')
    # lower the first letter
    splitted = [s.strip()[0].lower() + s.strip()[1:] for s in splitted]
    return splitted
