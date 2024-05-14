from pydantic_avro.avro_to_pydantic import avsc_to_pydantic


def test_avsc_to_pydantic_empty():
    pydantic_code = avsc_to_pydantic({"name": "Test", "type": "record", "fields": []})
    assert "class Test(BaseModel):\n    pass" in pydantic_code


def test_avsc_to_pydantic_primitive():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": "string"},
                {"name": "col2", "type": "int"},
                {"name": "col3", "type": "long"},
                {"name": "col4", "type": "double"},
                {"name": "col5", "type": "float"},
                {"name": "col6", "type": "boolean"},
                {"name": "col7", "type": "bytes"},
            ],
        }
    )
    assert (
        "class Test(BaseModel):\n"
        "    col1: str\n"
        "    col2: int = Field(..., ge=-2**31, le=(2**31 - 1))\n"
        "    col3: int\n"
        "    col4: float\n"
        "    col5: float\n"
        "    col6: bool\n"
        "    col7: bytes" in pydantic_code
    )


def test_avsc_to_pydantic_map():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": {"type": "map", "values": "string", "default": {}}},
            ],
        }
    )
    assert "class Test(BaseModel):\n" "    col1: Dict[str, str]" in pydantic_code


def test_avsc_to_pydantic_map_nested_object():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {
                        "type": "map",
                        "values": {"type": "record", "name": "Nested", "fields": [{"name": "col1", "type": "string"}]},
                        "default": {},
                    },
                },
            ],
        }
    )
    assert "class Test(BaseModel):\n" "    col1: Dict[str, Nested]" in pydantic_code
    assert "class Nested(BaseModel):\n" "    col1: str" in pydantic_code


def test_avsc_to_pydantic_map_nested_array():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "array",
                            "items": "string",
                        },
                        "default": {},
                    },
                },
            ],
        }
    )
    assert "class Test(BaseModel):\n" "    col1: Dict[str, List[str]]" in pydantic_code


def test_avsc_to_pydantic_logical():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {"type": "int", "logicalType": "date"},
                },
                {
                    "name": "col2",
                    "type": {"type": "long", "logicalType": "time-micros"},
                },
                {
                    "name": "col3",
                    "type": {"type": "long", "logicalType": "time-millis"},
                },
                {
                    "name": "col4",
                    "type": {"type": "long", "logicalType": "timestamp-micros"},
                },
                {
                    "name": "col5",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
            ],
        }
    )
    assert (
        "class Test(BaseModel):\n"
        "    col1: date\n"
        "    col2: time\n"
        "    col3: time\n"
        "    col4: datetime\n"
        "    col5: datetime" in pydantic_code
    )


def test_avsc_to_pydantic_complex():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {
                        "name": "Nested",
                        "type": "record",
                        "fields": [],
                    },
                },
                {
                    "name": "col2",
                    "type": {
                        "type": "array",
                        "items": "int",
                    },
                },
                {
                    "name": "col3",
                    "type": {
                        "type": "array",
                        "items": "Nested",
                    },
                },
            ],
        }
    )
    assert (
        "class Test(BaseModel):\n"
        "    col1: Nested\n"
        "    col2: List[int]\n"
        "    col3: List[Nested]\n" in pydantic_code
    )

    assert "class Nested(BaseModel):\n    pass\n" in pydantic_code


def test_default():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": "string", "default": "test"},
                {"name": "col2_1", "type": ["null", "string"], "default": None},
                {"name": "col2_2", "type": ["string", "null"], "default": "default_str"},
                {"name": "col3", "type": {"type": "map", "values": "string"}, "default": {"key": "value"}},
                {"name": "col4", "type": "boolean", "default": True},
                {"name": "col5", "type": "boolean", "default": False},
            ],
        }
    )
    assert (
        "class Test(BaseModel):\n"
        '    col1: str = "test"\n'
        "    col2_1: Optional[str] = None\n"
        '    col2_2: Optional[str] = "default_str"\n'
        '    col3: Dict[str, str] = {"key": "value"}\n'
        "    col4: bool = True\n"
        "    col5: bool = False\n" in pydantic_code
    )


def test_enums():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "c1", "type": {"type": "enum", "symbols": ["passed", "failed"], "name": "Status"}},
            ],
        }
    )

    assert "class Test(BaseModel):\n" "    c1: Status" in pydantic_code

    assert "class Status(str, Enum):\n" '    passed = "passed"\n' '    failed = "failed"' in pydantic_code


def test_enums_reuse():
    pydantic_code = avsc_to_pydantic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "c1", "type": {"type": "enum", "symbols": ["passed", "failed"], "name": "Status"}},
                {"name": "c2", "type": "Status"},
            ],
        }
    )

    assert "class Test(BaseModel):\n" "    c1: Status\n" "    c2: Status" in pydantic_code

    assert "class Status(str, Enum):\n" '    passed = "passed"\n' '    failed = "failed"' in pydantic_code


def test_enums_keys_style():
    schema = {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "c1", "type": {"type": "enum", "symbols": ["FirstValue", "secondValue"], "name": "Status"}},
                {"name": "c2", "type": "Status"},
            ],
        }
    no_style = avsc_to_pydantic(schema)

    assert "class Test(BaseModel):\n" "    c1: Status\n" "    c2: Status" in no_style
    assert "class Status(str, Enum):\n" '    FirstValue = "FirstValue"\n' '    secondValue = "secondValue"' in no_style

    snake = avsc_to_pydantic(schema, enum_key_style="snake_case")

    assert "class Test(BaseModel):\n" "    c1: Status\n" "    c2: Status" in snake
    assert "class Status(str, Enum):\n" '    first_value = "FirstValue"\n' '    second_value = "secondValue"' in snake

    snake_upper = avsc_to_pydantic(schema, enum_key_style="snake_case_upper")

    assert "class Test(BaseModel):\n" "    c1: Status\n" "    c2: Status" in snake_upper
    assert "class Status(str, Enum):\n" '    FIRST_VALUE = "FirstValue"\n' '    SECOND_VALUE = "secondValue"' in snake_upper


def test_unions():
    pydantic_code = avsc_to_pydantic(
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {
                    "name": "a_union",
                    "type": [
                        "null",
                        "long",
                        "string",
                        {
                            "type": "record",
                            "name": "ARecord",
                            "fields": [{"name": "values", "type": {"type": "map", "values": "string"}}],
                        },
                    ],
                },
                {
                    "name": "b_union",
                    "type": [
                        "long",
                        "string",
                        "ARecord",
                    ],
                },
            ],
        }
    )

    assert "a_union: Optional[Union[int,str,ARecord]]" in pydantic_code
    assert "b_union: Union[int,str,ARecord]" in pydantic_code


def test_int():
    pydantic_code = avsc_to_pydantic(
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {
                    "name": "c1",
                    "type": "int",
                },
            ],
        }
    )

    assert "c1: int = Field(..., ge=-2**31, le=(2**31 - 1))" in pydantic_code
