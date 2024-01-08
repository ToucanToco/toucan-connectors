# This is a temporary file to make tests, to be delete (transfered into the real test file)
from toucan_connectors.http_api.tarek_http_api_connector import HttpAPIConnector2, HttpAPIDataSource2



def test_e2e():
    con_params = {'name': 'open_data_paris', 'baseroute': 'https://opendata.paris.fr/api/v2'}
    ds_params = {
        'domain': 'books',
        'name': 'open_data_paris',
        'url': '/catalog/datasets/les-arbres-plantes/records',
        'filter': '.records[].record.fields',
        'pagination': {
            'type': 'offset',
            'pagination_informations': {
                'request_offset_label': 'offset',
                'request_limit_label': 'limit',
                'request_limit_value': 100,
                'response_count_jq': '.total_count'                
            } 
        }
    }

    con = HttpAPIConnector2(**con_params)
    ds = HttpAPIDataSource2(**ds_params)

    print(ds)
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    df = con.get_df(ds)
    print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
    print(df)
    assert df.shape == (4200, 14)


test_e2e()