from abc import ABCMeta, abstractmethod

import pandas as pd


class MandatoryParameter:
    def __init__(self, class_type):
        self.class_type = class_type

    def __set__(self, instance, value):
        assert isinstance(value, self.class_type), f'Expected {self.class_type}'
        instance.__dict__[self.name] = value

    def __set_name__(self, owner, name):
        self.name = name

    def __repr__(self):
        return f'{self.__class__.__name__}<{self.class_type.__name__}>'


class OptionalParameter:
    def __init__(self, class_type, default_value):
        self.class_type = class_type
        self.default_value = default_value

    def __set__(self, instance, value):
        if value != self.default_value:
            assert isinstance(value, self.class_type), f'Expected {self.class_type}'
        instance.__dict__[self.name] = value

    def __set_name__(self, owner, name):
        self.name = name

    def __repr__(self):
        return f'{self.__class__.__name__}<{self.class_type.__name__}>({self.default_value})'


class Base:
    def __init_subclass__(cls):
        for name, class_type in cls.__annotations__.items():
            try:
                default_value = getattr(cls, name)
                if not isinstance(default_value, (MandatoryParameter, OptionalParameter)):
                    checker = OptionalParameter(class_type, default_value)
                    cls.__annotations__[name] = (class_type, default_value)
                else:
                    checker = default_value
            except AttributeError:
                checker = MandatoryParameter(class_type)
            checker.__set_name__(cls, name)
            setattr(cls, name, checker)

    def __init__(self, **kwargs):
        ann = self.__annotations__
        optional_args = {k: v for k, v in ann.items() if isinstance(v, tuple)}
        assert set(ann) - set(optional_args) <= set(kwargs) <= set(ann), \
            f'Bad parameters. Here are the parameters needed: {ann}'
        for name, type_or_tuple in ann.items():
            if isinstance(type_or_tuple, tuple):
                setattr(self, name, type_or_tuple[1])
        for name, val in kwargs.items():
            setattr(self, name, val)

    def __repr__(self):
        args = ', '.join(f'{name}={repr(getattr(self, name))}' for name in self.__annotations__)
        return f'{type(self).__name__}({args})'


class ToucanDataSource(Base):
    name: str
    domain: str

    def __init_subclass__(cls):
        cls.__annotations__ = {**ToucanDataSource.__annotations__, **cls.__annotations__}
        super().__init_subclass__()


class ToucanConnector(Base, metaclass=ABCMeta):
    name: str

    def __new__(cls, *args, **kwargs):

        if not hasattr(cls, 'type'):
            raise TypeError('Connector has no type')

        return super().__new__(cls, *args, **kwargs)

    @abstractmethod
    def get_df(self, data_source: ToucanDataSource) -> pd.DataFrame:
        """ Method to get a pandas dataframe """

    @classmethod
    def validate(cls, data_source: dict):
        """Validate a data_source for this type of connector """
        try:
            cls.data_source_class(**data_source)
        except AttributeError:
            raise TypeError('Implement validate or set data_source_class attr')
