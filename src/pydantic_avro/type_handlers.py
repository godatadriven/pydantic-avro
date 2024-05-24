import json
from typing import Protocol

from pydantic import Field, BaseModel

LOGICAL_TYPES = {
    "uuid": "UUID",
    "decimal": "Decimal",
    "timestamp-millis": "datetime",
    "timestamp-micros": "datetime",
    "time-millis": "time",
    "time-micros": "time",
    "date": "date",
}


class ClassRegistry:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ClassRegistry, cls).__new__(cls)
            cls._instance._classes = {}
        return cls._instance

    def add_class(self, name: str, class_def: str):
        self._classes[name] = class_def

    def get_class(self, name: str) -> str:
        if name not in self._classes:
            raise KeyError(f"Class {name} not found in registry")
        return self._classes[name]

    @property
    def classes(self) -> dict:
        return self._classes

    def has_class(self, name: str) -> bool:
        return name in self._classes


def string_type_handler(t: str) -> str:
    if t == "string":
        return "str"


def int_type_handler(t: str) -> str:
    if t == "int":
        return "int"


def long_type_handler(t: str) -> str:
    if t == "long":
        return "int"


def boolean_type_handler(t: str) -> str:
    if t == "boolean":
        return "bool"


def double_type_handler(t: str) -> str:
    if t == "double":
        return "float"


def float_type_handler(t: str) -> str:
    if t == "float":
        return "float"


def bytes_type_handler(t: str) -> str:
    if t == "bytes":
        return "bytes"


def list_type_handler(t: list) -> str:
    if "null" in t and len(t) == 2:
        c = t.copy()
        c.remove("null")
        return f"Optional[{get_pydantic_type(c[0])}]"
    if "null" in t:
        return f"Optional[Union[{','.join([get_pydantic_type(e) for e in t if e != 'null'])}]]"
    return f"Union[{','.join([get_pydantic_type(e) for e in t])}]"


def map_type_handler(t: dict) -> str:
    value_type = get_pydantic_type(t.get("values"))
    return f"Dict[str, {value_type}]"


def logical_type_handler(t: str) -> str:
    return LOGICAL_TYPES.get(t)


def enum_type_handler(t: dict) -> str:
    name = t.get("name")
    if not ClassRegistry.has_class(name):
        enum_class = f"class {name}(str, Enum):\n"
        for s in t.get("symbols"):
            enum_class += f'    {s} = "{s}"\n'
        ClassRegistry().add_class(name, enum_class)
    return name


def array_type_handler(t: dict) -> str:
    sub_type = get_pydantic_type(t.get("items"))
    return f"List[{sub_type}]"


def record_type_handler(schema: dict) -> str:
    name = schema["name"]
    current = f"class {name}(BaseModel):\n"

    for field in schema["fields"]:
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
    if len(schema["fields"]) == 0:
        current += "    pass\n"

    ClassRegistry().add_class(name, current)


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


def get_handler(t: str | dict | list) -> callable:
    if isinstance(t, str):
        return TYPE_HANDLERS.get(t)
    elif isinstance(t, dict) and "logicalType" in t:
        return TYPE_HANDLERS.get("logical_type")
    elif isinstance(t, dict) and "type" in t:
        return TYPE_HANDLERS.get(t["type"])
    elif isinstance(t, list):
        return TYPE_HANDLERS.get("list")


def get_pydantic_type(schema: dict) -> str:
    t = schema["type"]
    handler = get_handler(t)
    if handler is None:
        raise NotImplementedError(f"Type {t} not supported yet")
    if t == "record":
        return handler(schema)
    return handler(t)
