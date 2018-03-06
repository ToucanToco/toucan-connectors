from abc import ABCMeta


class AbstratConnector(metaclass=ABCMeta):
    def __init__(self, **kwargs):
        if 'name' not in kwargs:
            raise MissingConnectorName
        self.name = kwargs['name']

        for required_arg in self._get_required_args():
            if required_arg not in kwargs:
                raise MissingConnectorOption(self, required_arg)
            setattr(self, required_arg, kwargs[required_arg])

        try:
            for optional_arg in self._get_optional_args():
                if optional_arg in kwargs:
                    setattr(self, optional_arg, kwargs[optional_arg])
        except NotImplementedError:
            pass

    def _get_required_args(self):
        raise NotImplementedError

    def _get_optional_args(self):
        raise NotImplementedError

    def query(self, query, fields={}):
        raise NotImplementedError

    def is_connected(self):
        raise NotImplementedError


class MissingConnectorName(Exception):
    pass


class MissingConnectorOption(Exception):
    pass