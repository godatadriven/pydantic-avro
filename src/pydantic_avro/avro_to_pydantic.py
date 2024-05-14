import json
import logging
import sys
from pathlib import Path
from typing import Optional, Union

from pydantic_avro.helpers import convert_enum_key

logging.basicConfig(format="{asctime} - {levelname} - {message}", level=logging.INFO, style="{", stream=sys.stdout)
log = logging.getLogger(__name__)


def avsc_to_pydantic(schema: dict, enum_key_style: Optional[str] = None) -> str:
    """Generate python code of pydantic of given Avro Schema"""
    if "type" not in schema or schema["type"] != "record":
        raise AttributeError("Type not supported")
    if "name" not in schema:
        raise AttributeError("Name is required")
    if "fields" not in schema:
        raise AttributeError("fields are required")

    classes = {}

    def get_python_type(t: Union[str, dict]) -> str:
        """Return python type for given avro type"""
        optional = False
        if isinstance(t, str):
            if t == "string":
                py_type = "str"
            elif t == "int":
                py_type = "int"
            elif t == "long":
                py_type = "int"
            elif t == "boolean":
                py_type = "bool"
            elif t == "double" or t == "float":
                py_type = "float"
            elif t == "bytes":
                py_type = "bytes"
            elif t in classes:
                py_type = t
            else:
                raise NotImplementedError(f"Type {t} not supported yet")
        elif isinstance(t, list):
            if "null" in t and len(t) == 2:
                optional = True
                c = t.copy()
                c.remove("null")
                py_type = get_python_type(c[0])
            else:
                if "null" in t:
                    py_type = f"Optional[Union[{','.join([get_python_type(e) for e in t if e != 'null'])}]]"
                else:
                    py_type = f"Union[{','.join([get_python_type(e) for e in t])}]"
        elif t.get("logicalType") == "uuid":
            py_type = "UUID"
        elif t.get("logicalType") == "decimal":
            py_type = "Decimal"
        elif t.get("logicalType") == "timestamp-millis" or t.get("logicalType") == "timestamp-micros":
            py_type = "datetime"
        elif t.get("logicalType") == "time-millis" or t.get("logicalType") == "time-micros":
            py_type = "time"
        elif t.get("logicalType") == "date":
            py_type = "date"
        elif t.get("type") == "enum":
            enum_name = t.get("name")
            if enum_name not in classes:
                enum_class = f"class {enum_name}(str, Enum):\n"
                for s in t.get("symbols"):
                    enum_class += f'    {convert_enum_key(s, enum_key_style)} = "{s}"\n'
                classes[enum_name] = enum_class
            py_type = enum_name
        elif t.get("type") == "string":
            py_type = "str"
        elif t.get("type") == "array":
            sub_type = get_python_type(t.get("items"))
            py_type = f"List[{sub_type}]"
        elif t.get("type") == "record":
            record_type_to_pydantic(t)
            py_type = t.get("name")
        elif t.get("type") == "map":
            value_type = get_python_type(t.get("values"))
            py_type = f"Dict[str, {value_type}]"
        else:
            raise NotImplementedError(
                f"Type {t} not supported yet, "
                f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
            )
        if optional:
            return f"Optional[{py_type}]"
        else:
            return py_type

    def record_type_to_pydantic(schema: dict):
        """Convert a single avro record type to a pydantic class"""
        name = schema["name"]
        current = f"class {name}(BaseModel):\n"

        for field in schema["fields"]:
            n = field["name"]
            t = get_python_type(field["type"])
            default = field.get("default")
            if field["type"] == "int" and "default" in field and isinstance(default, (bool, type(None))):
                current += f"    {n}: {t} = Field({default}, ge=-2**31, le=(2**31 - 1))\n"
            elif field["type"] == "int" and "default" in field:
                current += f"    {n}: {t} = Field({json.dumps(default)}, ge=-2**31, le=(2**31 - 1))\n"
            elif field["type"] == "int":
                current += f"    {n}: {t} = Field(..., ge=-2**31, le=(2**31 - 1))\n"
            elif "default" not in field:
                current += f"    {n}: {t}\n"
            elif isinstance(default, (bool, type(None))):
                current += f"    {n}: {t} = {default}\n"
            else:
                current += f"    {n}: {t} = {json.dumps(default)}\n"
        if len(schema["fields"]) == 0:
            current += "    pass\n"

        classes[name] = current

    record_type_to_pydantic(schema)

    file_content = """
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict, Union
from uuid import UUID

from pydantic import BaseModel, Field


"""
    file_content += "\n\n".join(classes.values())

    return file_content


def convert_file(avsc_path: str, output_path: Optional[str] = None, enum_key_style: Optional[str] = None) -> None:
    with open(avsc_path) as fh:
        avsc_dict = json.load(fh)
    file_content = avsc_to_pydantic(avsc_dict, enum_key_style)
    if not output_path:
        log.info(f"Converted file content:\n{file_content}")
    else:
        fh = Path(output_path)
        fh.parent.mkdir(parents=True, exist_ok=True)
        fh.write_text(file_content)
