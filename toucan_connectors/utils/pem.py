from typing import Iterable


def _fix_pem(raw: str) -> Iterable[str]:
    in_header = False
    prev_char = None
    for c in raw:
        if c == '-' and prev_char != '-':
            in_header = not in_header
        if in_header:
            yield c
        else:
            yield '\n' if c == ' ' else c
        prev_char = c


def sanitize_spaces_pem(pem_data: str) -> str:
    """Sanitizes PEM files in the format passed by the frontend (with spaces intead of \n)
    to a valid format understood by the ssl module"""
    result = ''.join(_fix_pem(pem_data))
    return result if result.endswith('\n') else result + '\n'
