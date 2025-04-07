import json
from typing import Callable, Union

from pydantic_avro.from_avro.class_registery import ClassRegistry

LOGICAL_TYPES = {
    "uuid": "UUID",
    "decimal": "Decimal",
    "timestamp-millis": "datetime",
    "timestamp-micros": "datetime",
    "time-millis": "time",
    "time-micros": "time",
    "date": "date",
}


AVRO_TO_PY_MAPPING = {
    "string": "str",
    "int": "int",
    "long": "int",
    "boolean": "bool",
    "double": "float",
    "float": "float",
    "bytes": "bytes",
}


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
    type_field = t["type"]
    value_type = None
    if isinstance(type_field, dict):
        avro_value_type = type_field.get("values")
        if avro_value_type is None:
            raise AttributeError(f"Values are required for map type. Received: {t}")
        value_type = get_pydantic_type(avro_value_type)
    if isinstance(type_field, str):
        value_type = t.get("values")

    if value_type is None:
        raise AttributeError(f"Values are required for map type. Received: {t}")

    return f"Dict[str, {value_type}]"


def logical_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro logical type"""
    return LOGICAL_TYPES[t["type"]["logicalType"]]


def enum_type_handler(t: dict) -> str:
    """Gets the enum type of a given Avro enum type and adds it to the class registry"""
    if t["type"] == "enum":
        # comes from a unioned enum (e.g. ["null", "enum"])
        type_info = t
    else:
        # comes from a direct enum
        type_info = t["type"]

    name = type_info["name"]
    symbols = type_info["symbols"]

    if not ClassRegistry().has_class(name):
        enum_class = f"class {name}(str, Enum):\n"
        for s in symbols:
            enum_class += f'    {s} = "{s}"\n'
        ClassRegistry().add_class(name, enum_class)
    return name


def array_type_handler(t: dict) -> str:
    """Get the Python type of a given Avro array type"""
    if isinstance(t["type"], dict):
        sub_type = get_pydantic_type(t["type"]["items"])
    else:
        sub_type = get_pydantic_type(t["items"])
    return f"List[{sub_type}]"


def record_type_handler(t: dict) -> str:
    """Gets the record type of a given Avro record type and adds it to the class registry"""
    t = t["type"] if isinstance(t["type"], dict) else t
    name = t["name"]
    fields = t["fields"] if "fields" in t else t["type"]["fields"]
    field_strings = [generate_field_string(field) for field in fields]
    class_body = "\n".join(field_strings) if field_strings else "    pass"
    current = f"class {name}(BaseModel):\n{class_body}\n"
    ClassRegistry().add_class(name, current)
    return name


TYPE_HANDLERS = {
    "list": list_type_handler,
    "map": map_type_handler,
    "logical": logical_type_handler,
    "enum": enum_type_handler,
    "array": array_type_handler,
    "record": record_type_handler,
}


def generate_field_string(field: dict) -> str:
    """Generate a string representing a field in the Pydantic model."""
    n = field["name"]
    t = get_pydantic_type(field)
    default = field.get("default")
    if field["type"] == "int" and "default" in field and isinstance(default, (bool, type(None))):
        return f"    {n}: {t} = Field({default}, ge=-2**31, le=(2**31 - 1))"
    elif field["type"] == "int" and "default" in field:
        return f"    {n}: {t} = Field({json.dumps(default)}, ge=-2**31, le=(2**31 - 1))"
    elif field["type"] == "int":
        return f"    {n}: {t} = Field(..., ge=-2**31, le=(2**31 - 1))"
    elif "default" not in field:
        return f"    {n}: {t}"
    elif isinstance(default, (bool, type(None))):
        return f"    {n}: {t} = {default}"
    else:
        return f"    {n}: {t} = {json.dumps(default)}"


def get_pydantic_type(t: Union[str, dict]) -> str:
    """Get the Pydantic type for a given Avro type"""
    if isinstance(t, str):
        t = {"type": t}

    if isinstance(t.get("type"), str):
        if ClassRegistry().has_class(t["type"]):
            return t["type"]

        if t["type"] in AVRO_TO_PY_MAPPING:
            return AVRO_TO_PY_MAPPING[t["type"]]

    return get_type_handler(t)(t)


def get_type_handler(t: dict) -> Callable:
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
