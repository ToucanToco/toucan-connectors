import requests


class Client:
    def __init__(self, base_url, project_id, username, password):
        self.base_url = base_url[:-1] if base_url.endswith('/') else base_url
        self.project_id = project_id

        data_get = {'username': username, 'password': password, 'loginMode': 1}
        r = requests.post(f'{self.base_url}/auth/login', data=data_get)
        r.raise_for_status()
        self.token = r.headers['X-MSTR-AuthToken']
        self.cookies = dict(r.cookies)

    @property
    def headers(self):
        return {
            'X-MSTR-AuthToken': self.token,
            'Accept': 'application/json',
            'X-MSTR-ProjectID': self.project_id,
        }

    def query(self, dataset: str, id: str, viewfilter: dict, offset: int, limit: int) -> dict:
        url = f'{self.base_url}/{dataset}/{id}/instances'
        params = {'offset': str(offset), 'limit': str(limit)}
        data = {'viewFilter': viewfilter} if viewfilter else None

        r = requests.post(url, params=params, json=data, headers=self.headers, cookies=self.cookies)
        r.raise_for_status()

        return r.json()

    def list_objects(self, types, id: str, offset: int = 0, limit: int = 100):
        uri = f'{self.base_url}/searches/results'
        params = {'type': types, 'offset': str(offset), 'limit': str(limit)}
        if id:
            params['name'] = id

        r = requests.get(uri, params=params, headers=self.headers, cookies=self.cookies)
        r.raise_for_status()

        return r.json()

    def report(self, id: str, viewfilter: dict = None, offset: int = 0, limit: int = 100) -> dict:
        return self.query('reports', id, viewfilter, offset, limit)

    def cube(self, id: str, viewfilter: dict = None, offset: int = 0, limit: int = 100) -> dict:
        return self.query('cubes', id, viewfilter, offset, limit)
