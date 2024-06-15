from __future__ import annotations
import json

from pydantic_avro.class_registery import ClassRegistry

LOGICAL_TYPES = {
    "uuid": "UUID",
    "decimal": "Decimal",
    "timestamp-millis": "datetime",
    "timestamp-micros": "datetime",
    "time-millis": "time",
    "time-micros": "time",
    "date": "date",
}


def string_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro string type"""
    if t["type"] == "string":
        return "str"


def int_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro int type"""
    if t["type"] == "int":
        return "int"


def long_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro long type"""
    if t["type"] == "long":
        return "int"


def boolean_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro boolean type"""
    if t["type"] == "boolean":
        return "bool"


def double_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro double type"""
    if t["type"] == "double":
        return "float"


def float_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro float type"""
    if t["type"] == "float":
        return "float"


def bytes_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro bytes type"""
    if t["type"] == "bytes":
        return "bytes"


def list_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro list type"""
    l = t["type"]
    if "null" in l and len(l) == 2:
        c = l.copy()
        c.remove("null")
        return f"Optional[{get_pydantic_type(c[0])}]"
    if "null" in l:
        return f"Optional[Union[{','.join([get_pydantic_type(e) for e in l if e != 'null'])}]]"
    return f"Union[{','.join([get_pydantic_type(e) for e in l])}]"


def map_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro map type"""
    if isinstance(t["type"], dict):
        value_type = get_pydantic_type(t["type"].get("values"))
        return f"Dict[str, {value_type}]"

    value_type = get_pydantic_type(t.get("values"))
    return f"Dict[str, {value_type}]"


def logical_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro logical type"""
    if isinstance(t["type"], dict):
        return LOGICAL_TYPES.get(t["type"].get("logicalType"))
    return LOGICAL_TYPES.get(t.get("logicalType"))


def enum_type_handler(t: dict) -> str:
    """Gets the enum type of a given Avro enum type and adds it to the class registry"""
    name = t["type"].get("name")
    if not ClassRegistry().has_class(name):
        enum_class = f"class {name}(str, Enum):\n"
        for s in t["type"].get("symbols"):
            enum_class += f'    {s} = "{s}"\n'
        ClassRegistry().add_class(name, enum_class)
    return name


def array_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro array type"""
    if isinstance(t["type"], dict):
        sub_type = get_pydantic_type(t["type"].get("items"))
    else:
        sub_type = get_pydantic_type(t.get("items"))
    return f"List[{sub_type}]"


def record_type_handler(t: dict) -> str:
    """Gets the record type of a given Avro record type and adds it to the class registry"""
    t = t["type"] if isinstance(t["type"], dict) else t
    name = t["name"]
    current = f"class {name}(BaseModel):\n"
    fields = t["fields"] if "fields" in t else t["type"]["fields"]
    for field in fields:
        n = field["name"]
        t = get_pydantic_type(field)
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
    if len(fields) == 0:
        current += "    pass\n"

    ClassRegistry().add_class(name, current)
    return name


TYPE_HANDLERS = {
    "string": string_type_handler,
    "int": int_type_handler,
    "long": long_type_handler,
    "boolean": boolean_type_handler,
    "double": double_type_handler,
    "float": float_type_handler,
    "bytes": bytes_type_handler,
    "list": list_type_handler,
    "map": map_type_handler,
    "logical": logical_type_handler,
    "enum": enum_type_handler,
    "array": array_type_handler,
    "record": record_type_handler,
}


def get_pydantic_type(t: str | dict | list) -> str:
    """Get the Pydantic type for a given Avro type"""
    if isinstance(t, str):
        t = {"type": t}

    if isinstance(t["type"], str) and ClassRegistry().has_class(t["type"]):
        return t["type"]

    return get_handler(t)(t)


def get_handler(t: dict) -> callable:
    """Get the handler for a given Avro type"""
    h = None
    t = t["type"]
    if isinstance(t, str):
        h = TYPE_HANDLERS.get(t)
    elif isinstance(t, dict) and "logicalType" in t:
        h = TYPE_HANDLERS.get("logical")
    elif isinstance(t, dict) and "type" in t:
        h = TYPE_HANDLERS.get(t["type"])
    elif isinstance(t, list):
        h = TYPE_HANDLERS.get("list")

    if h:
        return h

    raise NotImplementedError(f"Type {t} not supported yet")
