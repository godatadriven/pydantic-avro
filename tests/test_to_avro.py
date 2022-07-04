import enum
import json
import os
import tempfile
import uuid
from datetime import date, datetime, time
from typing import Dict, List, Optional
from uuid import UUID

from avro import schema as avro_schema
from fastavro import parse_schema, reader, writer
from pydantic import Field

from pydantic_avro.base import AvroBase


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
    c1: str
    c2: int
    c3: float
    c4: datetime
    c5: date
    c6: time
    c7: Optional[str]
    c8: bool
    c9: UUID
    c10: Optional[UUID]
    c11: Dict[str, str]
    c12: dict
    c13: Status


class ComplexTestModel(AvroBase):
    c1: List[str]
    c2: NestedModel
    c3: List[NestedModel]
    c4: List[datetime]
    c5: Dict[str, NestedModel]


class ReusedObject(AvroBase):
    c1: Nested2Model
    c2: Nested2Model


class ReusedObjectArray(AvroBase):
    c1: List[Nested2Model]
    c2: Nested2Model


class DefaultValues(AvroBase):
    c1: str = "test"


class ModelWithAliases(AvroBase):
    field: str = Field(..., alias="Field")


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
            {"name": "c9", "type": {"type": "string", "logicalType": "uuid"}},
            {"name": "c10", "type": ["null", {"type": "string", "logicalType": "uuid"}], "default": None},
            {"name": "c11", "type": {"type": "map", "values": "string"}},
            {"name": "c12", "type": {"type": "map", "values": "string"}},
            {"name": "c13", "type": {"type": "enum", "symbols": ["passed", "failed"], "name": "Status"}},
        ],
    }
    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 13


def test_avro_write():
    record1 = TestModel(
        c1="1",
        c2=2,
        c3=3,
        c4=4,
        c5=5,
        c6=6,
        c7=7,
        c8=True,
        c9=uuid.uuid4(),
        c10=uuid.uuid4(),
        c11={"key": "value"},
        c12={},
        c13=Status.passed,
    )

    parsed_schema = parse_schema(TestModel.avro_schema())

    # 'records' can be an iterable (including generator)
    records = [
        record1.dict(),
    ]

    with tempfile.TemporaryDirectory() as dir:
        # Writing
        with open(os.path.join(dir, "test.avro"), "wb") as out:
            writer(out, parsed_schema, records)

        result_records = []
        # Reading
        with open(os.path.join(dir, "test.avro"), "rb") as fo:
            for record in reader(fo):
                result_records.append(TestModel.parse_obj(record))
    assert records == result_records


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
        ],
    }
    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 5


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
    records = [
        record1.dict(),
    ]

    with tempfile.TemporaryDirectory() as dir:
        # Writing
        with open(os.path.join(dir, "test.avro"), "wb") as out:
            writer(out, parsed_schema, records)

        result_records = []
        # Reading
        with open(os.path.join(dir, "test.avro"), "rb") as fo:
            for record in reader(fo):
                result_records.append(ComplexTestModel.parse_obj(record))
    assert records == result_records


def test_defaults():
    result = DefaultValues.avro_schema()
    assert result == {
        "type": "record",
        "namespace": "DefaultValues",
        "name": "DefaultValues",
        "fields": [
            {"name": "c1", "type": "string", "default": "test"},
        ],
    }
    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 1


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
