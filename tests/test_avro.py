import json
import os
import tempfile
import uuid
from datetime import date, datetime, time
from typing import Dict, List, Optional
from uuid import UUID

from avro import schema as avro_schema
from fastavro import parse_schema, reader, writer

from pydantic_avro.avro_to_pydantic import avsc_to_pydatic
from pydantic_avro.base import AvroBase


class Nested2Model(AvroBase):
    c111: str


class NestedModel(AvroBase):
    c11: Nested2Model


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


class ComplexTestModel(AvroBase):
    c1: List[str]
    c2: NestedModel
    c3: List[NestedModel]
    c4: List[datetime]


class ReusedObject(AvroBase):
    c1: Nested2Model
    c2: Nested2Model


class ReusedObjectArray(AvroBase):
    c1: List[Nested2Model]
    c2: Nested2Model


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
            {"name": "c7", "type": ["string", "null"], "default": None},
            {"name": "c8", "type": "boolean"},
            {"name": "c9", "type": {"type": "string", "logicalType": "uuid"}},
            {"name": "c10", "type": [{"type": "string", "logicalType": "uuid"}, "null"], "default": None},
            {"name": "c11", "type": {"default": {}, "type": "map", "values": {"type": "string"}}},
        ],
    }
    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 11


def test_avro_write():
    record1 = TestModel(
        c1="1", c2=2, c3=3, c4=4, c5=5, c6=6, c7=7, c8=True, c9=uuid.uuid4(), c10=uuid.uuid4(), c11={"key": "value"}
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
        ],
    }
    # Reading schema with avro library to be sure format is correct
    schema = avro_schema.parse(json.dumps(result))
    assert len(schema.fields) == 4


def test_avro_write_complex():
    record1 = ComplexTestModel(
        c1=["1", "2"],
        c2=NestedModel(c11=Nested2Model(c111="test")),
        c3=[NestedModel(c11=Nested2Model(c111="test"))],
        c4=[1, 2, 3, 4],
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


def test_avsc_to_pydantic_empty():
    pydantic_code = avsc_to_pydatic({"name": "Test", "type": "record", "fields": []})
    assert "class Test(BaseModel):\n    pass" in pydantic_code


def test_avsc_to_pydantic_primitive():
    pydantic_code = avsc_to_pydatic(
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
            ],
        }
    )
    assert (
        "class Test(BaseModel):\n"
        "    col1: str\n"
        "    col2: int\n"
        "    col3: int\n"
        "    col4: float\n"
        "    col5: float\n"
        "    col6: bool" in pydantic_code
    )


def test_avsc_to_pydantic_map():
    pydantic_code = avsc_to_pydatic(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": {"type": "map", "values": "string", "default": {}}},
            ],
        }
    )
    assert "class Test(BaseModel):\n" "    col1: Dict[str, str]" in pydantic_code


def test_avsc_to_pydantic_logical():
    pydantic_code = avsc_to_pydatic(
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
    pydantic_code = avsc_to_pydatic(
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
        "    col2: List[int] = []\n"
        "    col3: List[Nested] = []\n" in pydantic_code
    )

    assert "class Nested(BaseModel):\n    pass\n" in pydantic_code
