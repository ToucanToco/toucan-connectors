tuple_field: Tuple = Field(
    ...,
    title='tuple field 1',
    alias='public tuple field name 1',
    description='tuple field description',
)
tuple_field_2: Tuple[str, str] = Field(
    ...,
    title='tuple field 2',
    alias='public tuple field name 2',
    description='tuple field description',
)
tuple_field_3: Tuple[int, int] = Field(
    ...,
    title='tuple field 3',
    alias='public tuple field name 3',
    description='tuple field description',
)
tuple_field_4: Tuple[int, str] = Field(
    ...,
    title='tuple field 4',
    alias='public tuple field name 4',
    description='tuple field description',
)
tuple_field_5: Tuple[SubFieldString, SubFieldInt] = Field(
    ...,
    title='tuple field 5',
    alias='public tuple field name 5',
    description='tuple field description',
)
