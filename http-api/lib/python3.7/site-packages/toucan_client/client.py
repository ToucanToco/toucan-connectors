from copy import deepcopy

import requests


def build_requests_kwargs(kwargs, extra_kwargs):
    requests_kwargs = deepcopy(kwargs)
    requests_kwargs.update(extra_kwargs)

    if "stage" in requests_kwargs:
        requests_kwargs['params'] = requests_kwargs.get('params', {})
        requests_kwargs['params'].update({'stage': requests_kwargs.pop('stage')})

    return requests_kwargs


class ToucanClient:
    """
    Small client for sending requests to a Toucan Toco backend.

    >>> # Example: Fetch etl config
    >>> client = ToucanClient('https://api.some.project.com')
    >>> small_app = client['my-small-app']
    >>> etl_config = small_app.config.etl.get()
    >>>
    >>> # Example: send a post request with some json data
    >>> response = small_app.config.etl.put(json={'DATA_SOURCE': ['example']})
    >>> # response.status_code equals 200 if everything went well
    """

    def __init__(self, base_route, _path=None, **kwargs):
        self._base_route = base_route.strip().rstrip('/')
        self._requests_kwargs = kwargs
        self._path = tuple(_path) if _path else ()

    def __getitem__(self, key):
        new_path = self._path + (key,)
        return ToucanClient(self._base_route, new_path, **self._requests_kwargs)

    def __getattr__(self, key):
        return self[key]  # forward to __getitem__

    def __call__(self, **kwargs):
        method = self._path[-1]
        method_func = getattr(requests, method)
        url = '/'.join((self._base_route,) + self._path[:-1])
        requests_kwargs = build_requests_kwargs(self._requests_kwargs, kwargs)
        return method_func(url, **requests_kwargs)
