class InvalidPEMFormat(Exception):
    def __init__(self):
        super().__init__('PEM format not understood')


def _sanitize_spaces_pem(pem_data: str) -> str:
    it = enumerate(pem_data)
    # Can't use tuples in walrus assignments :-(
    # Skipping the header between --- and letting it untouched
    while next(it)[1] == '-':
        continue
    while next(it)[1] != '-':
        continue
    while (tup := next(it))[1] == '-':
        continue

    data_start = tup[0]
    # Gathering data between header and footers
    while (tup := next(it))[1] != '-':
        continue

    data_end = tup[0]
    # Raw header + data with ' ' replaced with '\n' + raw footer
    return (
        pem_data[:data_start]
        + pem_data[data_start:data_end].replace(' ', '\n')
        + pem_data[data_end:]
    )


def sanitize_spaces_pem(pem_data: str) -> str:
    """Sanitizes PEM files in the format passed by the frontend (with spaces intead of \n)
    to a valid format understood by the ssl module"""
    try:
        return _sanitize_spaces_pem(pem_data)
    except StopIteration as exc:
        raise InvalidPEMFormat from exc
