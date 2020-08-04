from toucan_connectors.revinate.helpers import build_headers


def test_build_headers():
    """
    It should build headers with a specific format

    cf. https://porter.revinate.com/documentation
    """
    headers = build_headers(
        api_key='d87b88ceefbcf9d2b2adfb2bbbde1234',
        api_secret='b7536617fa4a1a9e3c7a707abcde866771570cb8c9a28401abcde755b48be6cb',
        username='test_user@revinate.com',
        timestamp='1420765002',
    )

    expected_result = {
        'X-Revinate-Porter-Username': 'test_user@revinate.com',
        'X-Revinate-Porter-Timestamp': '1420765002',
        'X-Revinate-Porter-Key': 'd87b88ceefbcf9d2b2adfb2bbbde1234',
        'X-Revinate-Porter-Encoded': '753b2b64fc91ab26484d67b67eeae4f5588ce4d5b107346308a7421246c8fff8',
    }

    assert headers == expected_result
