import json
from typing import Optional

from pydantic_avro.from_avro.class_registery import ClassRegistry
from pydantic_avro.from_avro.types import get_pydantic_type


def validate_schema(schema: dict) -> None:
    if "type" not in schema:
        raise AttributeError("Type not supported")
    if "name" not in schema:
        raise AttributeError("Name is required")
    if "fields" not in schema:
        raise AttributeError("Fields are required")


def avsc_to_pydantic(schema: dict) -> str:
    """Generate python code of pydantic of given Avro Schema"""
    # Ensures that state is clean from previous calls
    ClassRegistry().clear()
    validate_schema(schema)
    get_pydantic_type(schema)

    file_content = """
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict, Union
from uuid import UUID

from pydantic import BaseModel, Field


"""
    file_content += "\n\n".join(ClassRegistry().classes.values())

    return file_content


def convert_file(avsc_path: str, output_path: Optional[str] = None):
    with open(avsc_path, "r") as fh:
        avsc_dict = json.load(fh)
    file_content = avsc_to_pydantic(avsc_dict)
    if output_path is None:
        print(file_content)
    else:
        with open(output_path, "w") as fh:
            fh.write(file_content)
