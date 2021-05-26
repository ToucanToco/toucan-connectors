string_field: str = Field(
    'String field default value',
    title='String field',
    alias='Public string field name',
    description='String field description',
    placeholder='placeholder'
)

int_field: int = Field(
    5,
    title='Numeric field',
    alias='Public numeric field name',
    description='Numeric field description',
    ge=10,
    le=3,
    placeholder=4
)

boolean_field: bool = Field(
    ...,
    title='checkbox field',
    alias='public boolean field name',
    description='checkbox field description'
)

enum_field_with_default_value_without_checkbox: EnumExample = Field(
    EnumExample.example_1.value,
    title='Enum Example with default value without checkbox',
    alias='Enum Example with default value without checkbox',
    description='Enum Example with default value without checkbox description',
    **{'ui': {
        'checkbox': False
    }}
)

sub_field: SubFieldComplex = Field(
    None,
    title='sub field',
    alias='sub field',
    description='sub field description'
)

dict_field: Dict = Field(
    {
        "test": "toto"
    },
    title='dict field',
    alias='public dict field name',
    description='dict field description'
)

list_field_4: List[SubFieldComplex] = Field(
    ...,
    title='list field 4',
    alias='public list field name 4',
    description='list field description'
)


