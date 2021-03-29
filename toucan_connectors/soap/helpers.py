def is_list_response(response):
    return isinstance(response, list)


def is_nested_list(response):
    # detects if the response is list of list which is a valid response type
    # like [[a, b, c, d]]
    return len(response) == 1 and isinstance(response[0], list) if len(response[0]) > 0 else False


def is_dict_of_lists(response):
    # detects if the response is in the dict of list format
    # e.g [{col1:[value, ...], col2:[value, ...]}]
    return (
        isinstance(response[0], dict) and isinstance(list(response[0].values())[0], list)
        if len(response[0]) > 0
        else False
    )
