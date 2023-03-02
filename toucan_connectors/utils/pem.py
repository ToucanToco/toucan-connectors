class InvalidPEMFormat(Exception):
    def __init__(self):
        super().__init__('PEM format not understood')


def _sanitize_spaces_pem(pem_data: str) -> str:
    pem_data = pem_data.replace('-----BEGIN CERTIFICATE-----', '-----BEGIN_CERTIFICATE-----')
    pem_data = pem_data.replace('-----END CERTIFICATE-----', '-----END_CERTIFICATE-----')
    pem_data = pem_data.replace(' ', '\n')
    pem_data = pem_data.replace('-----BEGIN_CERTIFICATE-----', '-----BEGIN CERTIFICATE-----')
    pem_data = pem_data.replace('-----END_CERTIFICATE-----', '-----END CERTIFICATE-----')

    return pem_data


def sanitize_spaces_pem(pem_data: str) -> str:
    """Sanitizes PEM files in the format passed by the frontend (with spaces intead of \n)
    to a valid format understood by the ssl module"""
    try:
        return _sanitize_spaces_pem(pem_data)
    except StopIteration as exc:
        raise InvalidPEMFormat from exc
