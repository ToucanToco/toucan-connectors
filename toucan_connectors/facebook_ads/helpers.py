def has_next_page(data: dict) -> bool:
    if 'paging' not in data:
        return False

    return 'next' in data['paging']
