from typing import Any, Dict, List, Optional, Set, Union

from pydantic_avro.to_avro.config import DEFS_NAME

STRING_TYPE_MAPPING = {
    "date-time": {
        "type": "long",
        "logicalType": "timestamp-micros",
    },
    "date": {
        "type": "int",
        "logicalType": "date",
    },
    "time": {
        "type": "long",
        "logicalType": "time-micros",
    },
    "uuid": {
        "type": "string",
        "logicalType": "uuid",
    },
    "binary": "bytes",
}


def get_definition(ref: str, schema: dict):
    """Reading definition of base schema for nested structs"""
    id = ref.replace(f"#/{DEFS_NAME}/", "")
    d = schema.get(DEFS_NAME, {}).get(id)
    if d is None:
        raise RuntimeError(f"Definition {id} does not exist")
    return d


def set_nullability(avro_type_dict: dict) -> dict:
    """Set the nullability of the field"""
    if type(avro_type_dict["type"]) is list:
        if "null" not in avro_type_dict["type"]:
            avro_type_dict["type"].insert(0, "null")
    elif avro_type_dict.get("default") is None:
        avro_type_dict["type"] = ["null", avro_type_dict["type"]]
    avro_type_dict.setdefault("default", None)
    return avro_type_dict


def null_to_first_element(avro_type_dict: dict) -> dict:
    """Set the null as the first element in the list as per avro schema requirements"""
    if type(avro_type_dict["type"]) is list and "null" in avro_type_dict["type"]:
        avro_type_dict["type"].remove("null")
        avro_type_dict["type"].insert(0, "null")
    return avro_type_dict


class AvroTypeHandler:
    def __init__(self, schema: dict):
        self.schema = schema
        self.classes_seen: Set[str] = set()

    def get_avro_type_dict(self, type_schema: dict, value: dict) -> dict:
        """Returns a type of a single field"""
        avro_type_dict: Dict[str, Any] = {}
        if "default" in value:
            avro_type_dict["default"] = value.get("default")
        if "description" in value:
            avro_type_dict["doc"] = value.get("description")
        avro_type_dict = self.get_avro_type(value, type_schema, avro_type_dict)
        return avro_type_dict

    def get_avro_type(self, value: dict, type_schema: dict, avro_type_dict: dict) -> dict:
        t = value.get("type")
        f = value.get("format")
        r = value.get("$ref")
        if "allOf" in value and len(value["allOf"]) == 1:
            r = value["allOf"][0]["$ref"]
        u = value.get("anyOf")

        if u is not None:
            return self.union_to_avro(type_schema, u, avro_type_dict)
        elif r is not None:
            return self.handle_references(r, avro_type_dict, type_schema)
        elif t is None:
            raise ValueError(f"Field '{value}' does not have a defined type.")
        elif t == "array":
            return self.array_to_avro(type_schema, value, avro_type_dict)
        elif t == "string":
            avro_type_dict["type"] = self.string_to_avro(f)
        elif t == "number":
            avro_type_dict["type"] = "double"
        elif t == "integer":
            avro_type_dict["type"] = self.integer_to_avro(value)
        elif t == "boolean":
            avro_type_dict["type"] = "boolean"
        elif t == "null":
            avro_type_dict["type"] = "null"
        elif t == "object":
            avro_type_dict["type"] = self.object_to_avro(type_schema, value)
        else:
            raise NotImplementedError(
                f"Type '{t}' not support yet, "
                f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
            )

        return avro_type_dict

    def handle_references(self, r: str, avro_type_dict: dict, type_schema: dict) -> dict:
        class_name = r.replace(f"#/{DEFS_NAME}/", "")
        if class_name in self.classes_seen:
            avro_type_dict["type"] = class_name
            return avro_type_dict

        d = get_definition(r, self.schema)
        if "enum" in d:
            avro_type_dict["type"] = {
                "type": "enum",
                "symbols": [str(v) for v in d["enum"]],
                "name": d["title"],
            }
        else:
            avro_type_dict["type"] = {
                "type": "record",
                "fields": self.fields_to_avro_dicts(d),
                # Name of the struct should be unique to the complete schema
                # Because of this the path in the schema is tracked and used as name for a nested struct/array
                "name": class_name,
            }

        self.classes_seen.add(class_name)
        return avro_type_dict

    def fields_to_avro_dicts(self, type_schema: dict) -> List[dict]:
        """Gets the fields from the schema and returns them as a list of dictionaries."""
        fields = []

        required = type_schema.get("required", [])
        for key, value in type_schema.get("properties", {}).items():
            avro_type_dict = self.get_avro_type_dict(type_schema=type_schema, value=value)
            avro_type_dict["name"] = key
            if key not in required:
                set_nullability(avro_type_dict)
                avro_type_dict = null_to_first_element(avro_type_dict)

            fields.append(avro_type_dict)
        return fields

    @staticmethod
    def string_to_avro(f: Optional[str]):
        if not f:
            return "string"
        return STRING_TYPE_MAPPING[f]

    @staticmethod
    def integer_to_avro(value: dict) -> str:
        minimum = value.get("minimum")
        maximum = value.get("maximum")
        # integer in python can be a long, only if minimum and maximum value is set a int can be used
        if minimum is not None and minimum >= -(2**31) and maximum is not None and maximum <= (2**31 - 1):
            return "int"
        return "long"

    def object_to_avro(self, schema: dict, value: dict) -> dict:
        a = value.get("additionalProperties")
        value_type = "string" if a is None else self.get_avro_type_dict(schema, a)["type"]
        return {"type": "map", "values": value_type}

    def union_to_avro(self, type_schema: dict, value: list, avro_type_dict: dict) -> dict:
        """Returns a type of a union field"""
        avro_type_dict["type"] = []
        for union_element in value:
            t = self.get_avro_type_dict(type_schema, union_element)
            avro_type_dict["type"].append(t["type"])
        return avro_type_dict

    def array_to_avro(self, type_schema: dict, value: dict, avro_type_dict: dict) -> dict:
        """Returns a type of an array field"""
        items = value.get("items")
        tn = self.get_avro_type_dict(type_schema, items)
        # If items in array are an object:
        if "$ref" in items:
            tn = tn["type"]
        # If items in array are a logicalType
        if (
            isinstance(tn, dict)
            and isinstance(tn.get("type", {}), dict)
            and tn.get("type", {}).get("logicalType") is not None
        ):
            tn = tn["type"]
        # If items in array are an array, the structure must be corrected
        if isinstance(tn, dict) and isinstance(tn.get("type", {}), dict) and tn.get("type", {}).get("type") == "array":
            items = tn["type"]["items"]
            tn = {"type": "array", "items": items}
        avro_type_dict["type"] = {"type": "array", "items": tn}
        return avro_type_dict
