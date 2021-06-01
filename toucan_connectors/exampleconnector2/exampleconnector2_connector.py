from datetime import date, datetime, time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import pandas as pd
from pydantic import BaseModel, EmailStr, Field, SecretStr, constr, create_model

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Exampleconnector2DataSource(ToucanDataSource):
    query: str


class EnumExample(str, Enum):
    example_1 = 'example_1'
    example_2 = 'example_2'


class SubFieldString(BaseModel):
    sub_field_example_1: str = Field(
        ...,
        title='sub field string 1',
        alias='sub field alias 1',
        description='sub field description 1',
    )

    class Config:
        arbitrary_types_allowed = True


class SubFieldInt(BaseModel):
    sub_field_example_2: int = Field(
        ...,
        title='sub field int 2',
        alias='sub field alias 2',
        description='sub field description 2',
    )

    class Config:
        arbitrary_types_allowed = True


class SubFieldEnum(BaseModel):
    sub_field_enum: EnumExample = Field(
        ...,
        title='sub field enum 1',
        alias='sub field enum alias 1',
        description='sub field enum alias description 1',
    )

    class Config:
        arbitrary_types_allowed = True


class MotherClass:
    string_field_mother_class: str = Field(
        ..., title='field mother class', description='field mother class description'
    )


class Child1Class(MotherClass):
    string_field_child1_class: str = Field(
        ..., title='field child1 class', description='field child1 class description'
    )

    def __init__(self, *args, **kwargs):
        super(Child1Class, self).__init__(*args, **kwargs)


class Child2Class(MotherClass):
    int_field_child2_class: int = Field(
        ..., title='field child2 class', description='field child2 class descrption'
    )

    def __init__(self, *args, **kwargs):
        super(Child2Class, self).__init__(*args, **kwargs)


class SubFieldComplex2(BaseModel):
    sub_field_example_21: str = Field(
        'test', title='sub field string 21', description='sub field string 21 description'
    )
    sub_field_example_22: str = Field(
        'prout', title='sub field int 22', description='sub field int 22 description'
    )

    class Config:
        arbitrary_types_allowed = True
        title = 'SubFieldComplex 2'


class SubFieldComplex(BaseModel):
    sub_field_example_11: int = Field(
        2,
        title='sub field string 11',
    )
    sub_field_example_12: int = Field(
        0,
        title='sub field int 12',
    )
    # sub_field_enum: EnumExample = Field(EnumExample.example_1, title='sub field enum')
    # sub_field_complex_complex: SubFieldComplex2 = Field(None, title='sub field complex 2 test')

    class Config:
        arbitrary_types_allowed = True
        title = 'SubFieldComplex 1'


class Exampleconnector2Connector(ToucanConnector):
    data_source_model: Exampleconnector2DataSource

    data: Union[SubFieldComplex, SubFieldComplex2] = Field(
        None,
        description='JSON object to send in the body of the HTTP request',
    )

    class Config:
        arbitrary_types_allowed = True

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['Exampleconnector2Connector']) -> None:
            ordered_keys = [
                'data',
            ]
            schema['properties'] = {k: schema['properties'][k] for k in ordered_keys}

    def _retrieve_data(self, data_source: Exampleconnector2DataSource) -> pd.DataFrame:
        pass
