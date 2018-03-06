# coding: utf-8


class Connector(object):
    """
    Base class for back-end connectors

    Each subclass must implement _get_required_args method for this parent constructor to validate them

    Args:
        name (string): name of the connector
    """

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


class MissingConnectorOption(Exception):
    """ Raised when an option is missing when instantiating a connector """

    def __init__(self, connector, option):
        self.option = option
        self.msg = 'The connector "{}" misses the {} option'.format(connector.name, option)

    def __str__(self):
        return repr(self.msg)


class MissingConnectorName(Exception):
    """ Raised when a connector has no given name """


class InvalidDataProvider(Exception):
    """ Raised when a data provider doesn't exist or isn't registered """

    def __init__(self, name):
        self.name = name
        self.msg = 'No data provider named "{}"'.format(self.name)

    def __str__(self):
        return repr(self.msg)


class InvalidDataProviderSpec(Exception):
    """ Raised when a data provider configuration is invalid """


class InvalidDataProviderType(InvalidDataProviderSpec):
    """ Raised when no connector exists for a type of data provider """


class InvalidDataFrameQueryConfig(Exception):
    """ Raised when the get_df config has not the expected arguments"""
