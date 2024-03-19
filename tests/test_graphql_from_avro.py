import pytest

from pydantic_avro.avro_to_graphql import (
    avsc_to_graphql,
    camel_type,
    classes_to_graphql_str,
)

camel_type_tests = [
    ["string", "OptionalString"],
    ["String", "OptionalString"],
    ["string!", "String"],
    ["String!", "String"],
    ["List[String!]!", "ListString"],
    ["List[String!]", "OptionalListString"],
]


@pytest.mark.parametrize("input_type,expected_type", camel_type_tests)
def test_camel_type(input_type: str, expected_type: str):
    assert camel_type(input_type) == expected_type


def test_avsc_to_graphql_empty():
    graphql_classes = avsc_to_graphql({"name": "Test", "type": "record", "fields": []})
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_code = """
type Test {
    _void: String
}"""
    assert expected_code in graphql_code


def test_avsc_to_graphql_primitive():
    graphql_classes = avsc_to_graphql(
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
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_code = """
type Test {
    col1: String!
    col2: Int!
    col3: Float!
    col4: Float!
    col5: Float!
    col6: Boolean!
    col7: String!
}"""
    assert expected_code in graphql_code


def test_avsc_to_graphql_map():
    graphql_classes = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {"type": "map", "values": "string", "default": {}},
                },
            ],
        }
    )
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_tuple = """
type StringMapTuple {
     key: String
     value: [String!]
}"""
    expected_type = """type Test {
    col1: [StringMapTuple]!
}"""
    assert expected_tuple in graphql_code
    assert expected_type in graphql_code


def test_avsc_to_graphql_map_nested_object():
    graphql_classes = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "record",
                            "name": "Nested",
                            "fields": [{"name": "col1", "type": "string"}],
                        },
                        "default": {},
                    },
                },
            ],
        }
    )
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_nested_type = """type Nested {
    col1: String!
}"""
    expected_tuple = """type NestedMapTuple {
     key: String
     value: [Nested!]
}"""
    expected_type = """type Test {
    col1: [NestedMapTuple]!
}"""
    assert expected_nested_type in graphql_code
    assert expected_tuple in graphql_code
    assert expected_type in graphql_code


def test_avsc_to_graphql_namespaced_object_reuse():
    graphql_classes = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {
                        "type": "record",
                        "name": "Nested",
                        "namespace": "com.pydantic",
                        "fields": [{"name": "col1", "type": "string"}],
                    },
                },
                {
                    "name": "col2",
                    "type": "com.pydantic.Nested",
                },
            ],
        }
    )
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_nested_type: str = """type Nested {
    col1: String!
}"""
    expected_type: str = """type Test {
    col1: Nested!
    col2: Nested!
}"""
    assert expected_nested_type in graphql_code
    assert expected_type in graphql_code


def test_avsc_to_graphql_map_nested_array():
    graphql_classes = avsc_to_graphql(
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
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_nested_type: str = """type ListStringMapTuple {
     key: String
     value: [List[String!]!]
}"""
    expected_type: str = """type Test {
    col1: [ListStringMapTuple]!
}"""
    assert expected_nested_type in graphql_code
    assert expected_type in graphql_code


def test_avsc_to_graphql_logical():
    graphql_classes = avsc_to_graphql(
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
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_type: str = """
type Test {
    col1: String!
    col2: Int!
    col3: Int!
    col4: Int!
    col5: Int!
}"""
    assert expected_type in graphql_code


def test_avsc_to_graphql_complex():
    graphql_classes = avsc_to_graphql(
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
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_nested_type: str = """type Nested {
    _void: String
}"""
    expected_type: str = """type Test {
    col1: Nested!
    col2: List[Int!]!
    col3: List[Nested!]!
}"""
    assert expected_type in graphql_code

    assert expected_nested_type in graphql_code


def test_default_optional():
    graphql_classes = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": ["null", "string"], "default": None},
                {"name": "col2", "type": ["string", "null"], "default": "default_str"},
                {"name": "col3", "type": "string"},
            ],
        }
    )
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_type: str = """type Test {
    col1: String
    # use \'"default_str"\' in queries, defaults not supported in graphql schemas
    col2: String
    col3: String!
}"""
    assert expected_type in graphql_code


def test_default():
    graphql_classes = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": "string", "default": "test"},
                {"name": "col2_1", "type": ["null", "string"], "default": None},
                {
                    "name": "col2_2",
                    "type": ["string", "null"],
                    "default": "default_str",
                },
                {
                    "name": "col3",
                    "type": {"type": "map", "values": "string"},
                    "default": {"key": "value"},
                },
                {"name": "col4", "type": "boolean", "default": True},
                {"name": "col5", "type": "boolean", "default": False},
            ],
        }
    )
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_map_type: str = """type StringMapTuple {
     key: String
     value: [String!]
}"""
    expected_type: str = """type Test {
    # use '"test"' in queries, defaults not supported in graphql schemas
    col1: String!
    col2_1: String
    # use '"default_str"' in queries, defaults not supported in graphql schemas
    col2_2: String
    # use '{"key": "value"}' in queries, defaults not supported in graphql schemas
    col3: [StringMapTuple]!
    # use 'True' in queries, defaults not supported in graphql schemas
    col4: Boolean!
    # use 'False' in queries, defaults not supported in graphql schemas
    col5: Boolean!
}"""
    assert expected_map_type in graphql_code
    assert expected_type in graphql_code


def test_enums():
    graphql_classes = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "c1",
                    "type": {
                        "type": "enum",
                        "symbols": ["passed", "failed"],
                        "name": "Status",
                    },
                },
            ],
        }
    )
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_enum = """enum Status {
    passed
    failed
}"""
    expected_type = """type Test {
    c1: Status!
}"""

    assert expected_enum in graphql_code
    assert expected_type in graphql_code


def test_enums_reuse():
    graphql_classes = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "c1",
                    "type": {
                        "type": "enum",
                        "symbols": ["passed", "failed"],
                        "name": "Status",
                    },
                },
                {"name": "c2", "type": "Status"},
            ],
        }
    )
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_enum = """enum Status {
    passed
    failed
}"""
    expected_type = """type Test {
    c1: Status!
    c2: Status!
}"""

    assert expected_enum in graphql_code
    assert expected_type in graphql_code


def test_unions():
    graphql_classes = avsc_to_graphql(
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
                            "fields": [
                                {
                                    "name": "values",
                                    "type": {"type": "map", "values": "string"},
                                }
                            ],
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
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_tuple_type = """type StringMapTuple {
     key: String
     value: [String!]
}"""
    expected_sub_type = """type ARecord {
    values: [StringMapTuple]!
}"""
    expected_type = """type Test {
    a_union: Float | String | ARecord
    b_union: Float! | String! | ARecord!
}"""

    assert expected_tuple_type in graphql_code
    assert expected_sub_type in graphql_code
    assert expected_type in graphql_code


def test_int():
    graphql_classes = avsc_to_graphql(
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
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_type = """type Test {
    c1: Int!
}"""
    assert expected_type in graphql_code


def test_avsc_to_graphql_directives():
    graphql_classes = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": "string"},
                {"name": "col2", "type": "string"},
                {"name": "col3", "type": "string"},
            ],
        },
        {
            "field_directives": {"col1": "@field-dir", "col3": "@field-dir"},
            "type_directives": {"Test": {"col2": "@type-dir", "col3": "@type-dir"}},
        },
    )
    graphql_code = classes_to_graphql_str(graphql_classes)
    expected_code = """
type Test {
    col1: String! @field-dir
    col2: String! @type-dir
    col3: String! @field-dir @type-dir
}"""
    assert expected_code in graphql_code
