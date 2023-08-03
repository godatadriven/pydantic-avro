import enum
import json
import os
import tempfile
import uuid
from datetime import date, datetime, time, timezone
from pprint import pprint
from typing import Dict, List, Optional, Type, Union
from uuid import UUID

from avro import schema as avro_schema
from fastavro import parse_schema, reader, writer
from pydantic import Field

from pydantic_avro.base import AvroBase
from src.pydantic_avro.base import PYDANTIC_V2


def dump(obj: AvroBase):
    return obj.model_dump() if PYDANTIC_V2 else obj.dict()


def parse(model: Type[AvroBase], data: dict):
    return model.model_validate(data) if PYDANTIC_V2 else model.parse_obj(data)


class Nested2Model(AvroBase):
    c111: str


class NestedModel(AvroBase):
    c11: Nested2Model


class Status(str, enum.Enum):
    # Here str is important to serialize pydantic object correctly

    # Note on value:
    #   As avro schema validate enum symbols by default. So Every symbol must be a valid avro symbol
    passed = "passed"
    failed = "failed"


class TestModel(AvroBase):
    __test__ = False
    c1: str
    c2: int
    c3: float
    c4: datetime
    c5: date
    c6: time
    c7: Optional[str] = None
    c8: bool
    c9: UUID = Field(..., description="This is UUID")
    c10: Optional[UUID] = Field(None, description="This is an optional UUID")
    c11: Dict[str, str]
    c12: dict
    c13: Status = Field(..., description="This is Status")
    c14: bytes


class ListofLists(AvroBase):
    c1: int
    c2: List[int]
    c3: List[List[int]]


class ComplexTestModel(AvroBase):
    c1: List[str]
    c2: NestedModel
    c3: List[NestedModel]
    c4: List[datetime]
    c5: Dict[str, NestedModel]
    c6: Union[None, str, int, NestedModel] = None


class ReusedObject(AvroBase):
    c1: Nested2Model
    c2: Nested2Model


class ReusedObjectArray(AvroBase):
    c1: List[Nested2Model]
    c2: Nested2Model


class DefaultValues(AvroBase):
    c1: str = "test"
    c2: Optional[str] = None
    c3: Optional[str] = "test"


class ModelWithAliases(AvroBase):
    field: str = Field(..., alias="Field")


class ModelWithUnion(AvroBase):
    c1: Union[None, str, int, NestedModel] = None
    c2: Optional[Union[str, int, NestedModel]] = None
    c3: Union[str, int, NestedModel]


def test_avro():
    result = TestModel.avro_schema()
    assert result == {
        "type": "record",
        "namespace": "TestModel",
        "name": "TestModel",
        "fields": [
            {"name": "c1", "type": "string"},
            {"name": "c2", "type": "long"},
            {"name": "c3", "type": "double"},
            {"name": "c4", "type": {"type": "long", "logicalType": "timestamp-micros"}},
            {"name": "c5", "type": {"type": "int", "logicalType": "date"}},
            {"name": "c6", "type": {"type": "long", "logicalType": "time-micros"}},
            {"name": "c7", "type": ["null", "string"], "default": None},
            {"name": "c8", "type": "boolean"},
            {"name": "c9", "type": {"type": "string", "logicalType": "uuid"}, "doc": "This is UUID"},
            {
                "name": "c10",
                "type": ["null", {"type": "string", "logicalType": "uuid"}],
                "default": None,
                "doc": "This is an optional UUID",
            },
            {"name": "c11", "type": {"type": "map", "values": "string"}},
            {"name": "c12", "type": {"type": "map", "values": "string"}},
            {
                "name": "c13",
                "type": {"type": "enum", "symbols": ["passed", "failed"], "name": "Status"},
                "doc": "This is Status",
            },
            {"name": "c14", "type": "bytes"},
        ],
    }
    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 14


def test_avro_write():
    record1 = TestModel(
        c1="1",
        c2=2,
        c3=3,
        c4=datetime(2000, 1, 1, 12, 34, 56, tzinfo=timezone.utc),
        c5=date(2000, 1, 1),
        c6=time(12, 34, 56),
        c7="foo",
        c8=True,
        c9=uuid.uuid4(),
        c10=uuid.uuid4(),
        c11={"key": "value"},
        c12={},
        c13=Status.passed,
        c14=bytes(),
    )

    parsed_schema = parse_schema(TestModel.avro_schema())

    # 'records' can be an iterable (including generator)
    records = [dump(record1)]

    with tempfile.TemporaryDirectory() as dir:
        # Writing
        with open(os.path.join(dir, "test.avro"), "wb") as out:
            writer(out, parsed_schema, records)

        result_records = []
        # Reading
        with open(os.path.join(dir, "test.avro"), "rb") as fo:
            for record in reader(fo):
                result_records.append(parse(TestModel, record))

    assert [record1] == result_records


def test_reused_object():
    result = ReusedObject.avro_schema()
    assert result == {
        "type": "record",
        "name": "ReusedObject",
        "namespace": "ReusedObject",
        "fields": [
            {
                "name": "c1",
                "type": {"fields": [{"name": "c111", "type": "string"}], "name": "Nested2Model", "type": "record"},
            },
            {"name": "c2", "type": "Nested2Model"},
        ],
    }
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 2


def test_reused_object_array():
    result = ReusedObjectArray.avro_schema()
    assert result == {
        "type": "record",
        "name": "ReusedObjectArray",
        "namespace": "ReusedObjectArray",
        "fields": [
            {
                "name": "c1",
                "type": {
                    "items": {"fields": [{"name": "c111", "type": "string"}], "name": "Nested2Model", "type": "record"},
                    "type": "array",
                },
            },
            {"name": "c2", "type": "Nested2Model"},
        ],
    }
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 2


def test_complex_avro():
    result = ComplexTestModel.avro_schema()
    pprint(result)
    assert result == {
        "type": "record",
        "name": "ComplexTestModel",
        "namespace": "ComplexTestModel",
        "fields": [
            {"name": "c1", "type": {"items": {"type": "string"}, "type": "array"}},
            {
                "name": "c2",
                "type": {
                    "fields": [
                        {
                            "name": "c11",
                            "type": {
                                "fields": [{"name": "c111", "type": "string"}],
                                "name": "Nested2Model",
                                "type": "record",
                            },
                        }
                    ],
                    "name": "NestedModel",
                    "type": "record",
                },
            },
            {
                "name": "c3",
                "type": {
                    "items": "NestedModel",
                    "type": "array",
                },
            },
            {"name": "c4", "type": {"items": {"logicalType": "timestamp-micros", "type": "long"}, "type": "array"}},
            {"name": "c5", "type": {"type": "map", "values": "NestedModel"}},
            {"name": "c6", "type": ["null", "string", "long", "NestedModel"], "default": None},
        ],
    }

    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 6


def test_avro_parse_list_of_lists():
    record1 = ListofLists(c1=1, c2=[2, 3], c3=[[4, 5], [6, 7]])

    schema = ListofLists.avro_schema()
    parsed_schema = parse_schema(schema)

    records = [dump(record1)]

    with tempfile.TemporaryDirectory() as dir:
        # Writing
        with open(os.path.join(dir, "test.avro"), "wb") as out:
            writer(out, parsed_schema, records)

        result_records = []
        # Reading
        with open(os.path.join(dir, "test.avro"), "rb") as fo:
            for record in reader(fo):
                result_records.append(parse(ListofLists, record))
    assert [record1] == result_records


def test_avro_write_complex():
    record1 = ComplexTestModel(
        c1=["1", "2"],
        c2=NestedModel(c11=Nested2Model(c111="test")),
        c3=[NestedModel(c11=Nested2Model(c111="test"))],
        c4=[1, 2, 3, 4],
        c5={"key": NestedModel(c11=Nested2Model(c111="test"))},
    )

    parsed_schema = parse_schema(ComplexTestModel.avro_schema())

    # 'records' can be an iterable (including generator)
    records = [dump(record1)]

    with tempfile.TemporaryDirectory() as dir:
        # Writing
        with open(os.path.join(dir, "test.avro"), "wb") as out:
            writer(out, parsed_schema, records)

        result_records = []
        # Reading
        with open(os.path.join(dir, "test.avro"), "rb") as fo:
            for record in reader(fo):
                result_records.append(parse(ComplexTestModel, record))
    assert [record1] == result_records


def test_defaults():
    # Pydantic v1 wrongly omit null type from optional field with non-None default
    c3_type = ["null", "string"] if PYDANTIC_V2 else "string"

    result = DefaultValues.avro_schema()
    assert result == {
        "type": "record",
        "namespace": "DefaultValues",
        "name": "DefaultValues",
        "fields": [
            {"name": "c1", "type": "string", "default": "test"},
            {"name": "c2", "type": ["null", "string"], "default": None},
            {"name": "c3", "type": c3_type, "default": "test"},
            # pydantic .schema has no idea c3 can take None, so we do not allow it here either
        ],
    }
    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 3


def test_custom_namespace():
    # if given namespace should change namespace in avro schema
    result = DefaultValues.avro_schema(namespace="test.test")
    assert result["namespace"] == "test.test"

    # if not given namespace should be same as name of the avro schema
    result = DefaultValues.avro_schema()
    assert result["namespace"] == result["name"]


def test_model_with_alias():
    result = ModelWithAliases.avro_schema()
    assert result == {
        "type": "record",
        "namespace": "ModelWithAliases",
        "name": "ModelWithAliases",
        "fields": [{"type": "string", "name": "Field"}],
    }

    result = ModelWithAliases.avro_schema(by_alias=False)
    assert result == {
        "type": "record",
        "namespace": "ModelWithAliases",
        "name": "ModelWithAliases",
        "fields": [
            {"type": "string", "name": "field"},
        ],
    }


def test_union_avro():
    result = ModelWithUnion.avro_schema()
    assert result == {
        "fields": [
            {
                "name": "c1",
                "type": [
                    "null",
                    "string",
                    "long",
                    {
                        "fields": [
                            {
                                "name": "c11",
                                "type": {
                                    "fields": [{"name": "c111", "type": "string"}],
                                    "name": "Nested2Model",
                                    "type": "record",
                                },
                            }
                        ],
                        "name": "NestedModel",
                        "type": "record",
                    },
                ],
                "default": None,
            },
            {
                "name": "c2",
                "type": [
                    "null",
                    "string",
                    "long",
                    "NestedModel",
                ],
                "default": None,
            },
            {
                "name": "c3",
                "type": [
                    "string",
                    "long",
                    "NestedModel",
                ],
            },
        ],
        "name": "ModelWithUnion",
        "namespace": "ModelWithUnion",
        "type": "record",
    }

    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 3


class OptionalArray(AvroBase):
    c1: Optional[List[str]] = None


def test_optional_array():
    result = OptionalArray.avro_schema()

    assert result == {
        "type": "record",
        "namespace": "OptionalArray",
        "name": "OptionalArray",
        "fields": [{"type": ["null", {"type": "array", "items": {"type": "string"}}], "name": "c1", "default": None}],
    }


class IntModel(AvroBase):
    c1: int = Field(..., ge=-(2**31), le=(2**31 - 1))


def test_int():
    result = IntModel.avro_schema()
    assert result == {
        "type": "record",
        "namespace": "IntModel",
        "name": "IntModel",
        "fields": [{"type": "int", "name": "c1"}],
    }


class CustomNameModel(AvroBase):
    c1: int

    if PYDANTIC_V2:
        model_config = {"title": "some_other_name"}
    else:

        class Config:
            title = "some_other_name"


def test_custom_name():
    result = CustomNameModel.avro_schema()
    assert result == {
        "type": "record",
        "namespace": "some_other_name",
        "name": "some_other_name",
        "fields": [{"type": "long", "name": "c1"}],
    }


class ModelWithDocString(AvroBase):
    """
    model with docstring!
    model with multiple line docstring!
    """

    c1: int


def test_record_doc():
    result = ModelWithDocString.avro_schema()
    assert result == {
        "doc": "model with docstring!\nmodel with multiple line docstring!",
        "type": "record",
        "namespace": "ModelWithDocString",
        "name": "ModelWithDocString",
        "fields": [{"type": "long", "name": "c1"}],
    }
