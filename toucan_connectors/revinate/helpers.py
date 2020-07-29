"""Modules containing the helper functions for the Revinate connector"""
import hashlib
import hmac
from typing import Dict


def build_headers(api_key, api_secret: str, username, timestamp) -> Dict[str, str]:
    """
    Takes a Revinate api_key, api_secret, username and the current timestamp in POSIX epochs and generates a headers for a request
    """
    # need to encode this key for HMAC, cf. https://docs.python.org/3/library/hmac.html
    api_secret = api_secret.encode()

    message = f'{username}{timestamp}'.encode()

    revinate_porter_encoded = hmac.new(
        key=api_secret, msg=message, digestmod=hashlib.sha256
    ).hexdigest()

    return {
        'X-Revinate-Porter-Username': username,
        'X-Revinate-Porter-Timestamp': timestamp,
        'X-Revinate-Porter-Key': api_key,
        'X-Revinate-Porter-Encoded': revinate_porter_encoded,
    }
