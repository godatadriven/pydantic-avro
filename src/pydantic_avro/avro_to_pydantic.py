import json
from typing import Optional, Union

RESERVED_KEYWORDS= {
    "and",
    "as",
    "assert",
    "break",
    "class",
    "continue",
    "def",
    "del",
    "elif",
    "else",
    "except",
    "False",
    "finally",
    "for",
    "from",
    "global",
    "if",
    "import",
    "in",
    "is",
    "lambda",
    "None",
    "nonlocal",
    "not",
    "or",
    "pass",
    "raise",
    "return",
    "True",
    "try",
    "while",
    "with",
    "yield",
}

def avsc_to_pydantic(schema: dict) -> str:
    """Generate python code of pydantic of given Avro Schema"""
    if "type" not in schema or schema["type"] != "record":
        raise AttributeError("Type not supported")
    if "name" not in schema:
        raise AttributeError("Name is required")
    if "fields" not in schema:
        raise AttributeError("fields are required")

    classes = {}

    def get_python_type(t: Union[str, dict]) -> str:
        """Returns python type for given avro type"""
        optional = False
        if isinstance(t, str):
            if t == "string":
                py_type = "str"
            elif t == "long" or t == "int":
                py_type = "int"
            elif t == "boolean":
                py_type = "bool"
            elif t == "double" or t == "float":
                py_type = "float"
            elif t in classes:
                py_type = t
            else:
                raise NotImplementedError(f"Type {t} not supported yet")
        elif isinstance(t, list):
            if "null" in t:
                optional = True
            if len(t) > 2 or (not optional and len(t) > 1):
                raise NotImplementedError("Only a single type ia supported yet")
            c = t.copy()
            if "null" in c:
                c.remove("null")
            py_type = get_python_type(c[0])
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
                    enum_class += f'    {s} = "{s}"\n'
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
            return f"Optional[{py_type}] = None"
        else:
            return py_type

    def record_type_to_pydantic(schema: dict):
        """Convert a single avro record type to a pydantic class"""
        name = schema["name"]
        current = f"class {name}(BaseModel):\n"

        for field in schema["fields"]:
            n = field["name"]
            n = n if n not in RESERVED_KEYWORDS else f"{n}_"
            t = get_python_type(field["type"])
            default = field.get("default")
            if default is None:
                current += f"    {n}: {t}\n"
            elif isinstance(default, bool):
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
from typing import List, Optional, Dict
from uuid import UUID

from pydantic import BaseModel


"""
    file_content += "\n\n".join(classes.values())

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
