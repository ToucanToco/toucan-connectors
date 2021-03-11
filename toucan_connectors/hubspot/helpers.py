def has_next_page(data):
    return 'paging' in data and 'next' in data['paging']


def has_next_page_legacy(data):
    return 'hasMore' in data and data['hasMore'] is True
