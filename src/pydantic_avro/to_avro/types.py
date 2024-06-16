from typing import Any, Dict, List, Optional, Set

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


class AvroTypeConverter:
    """Converts Pydantic schema to AVRO schema."""

    def __init__(self, schema: dict):
        self.root_schema = schema
        self.classes_seen: Set[str] = set()

    def fields_to_avro_dicts(self, parent_schema: dict) -> List[dict]:
        """Converts fields from the schema to AVRO and returns them as a list of dictionaries.

        :param parent_schema: The parent schema of the field (not the root schema for nested models)
        """
        fields = []

        required = parent_schema.get("required", [])
        for name, field_props in parent_schema.get("properties", {}).items():
            avro_type_dict = self._get_avro_type_dict(field_props=field_props)
            avro_type_dict["name"] = name
            if name not in required:
                set_nullability(avro_type_dict)
                avro_type_dict = null_to_first_element(avro_type_dict)

            fields.append(avro_type_dict)
        return fields

    def _get_avro_type_dict(self, field_props: dict) -> dict:
        """Returns a type of a single field"""
        avro_type_dict: Dict[str, Any] = {}
        if "default" in field_props:
            avro_type_dict["default"] = field_props.get("default")
        if "description" in field_props:
            avro_type_dict["doc"] = field_props.get("description")
        avro_type_dict = self._get_avro_type(field_props, avro_type_dict)
        return avro_type_dict

    def _get_avro_type(self, field_props: dict, avro_type_dict: dict) -> dict:
        """Returns AVRO type of a single field"""
        t = field_props.get("type")
        f = field_props.get("format")
        r = field_props.get("$ref")
        if "allOf" in field_props and len(field_props["allOf"]) == 1:
            r = field_props["allOf"][0]["$ref"]
        u = field_props.get("anyOf")

        if u is not None:
            return self._union_to_avro(u, avro_type_dict)
        elif r is not None:
            return self._handle_references(r, avro_type_dict)
        elif t is None:
            raise ValueError(f"Field '{field_props}' does not have a defined type.")
        elif t == "array":
            return self._array_to_avro(field_props, avro_type_dict)
        elif t == "string":
            avro_type_dict["type"] = self._string_to_avro(f)
        elif t == "integer":
            avro_type_dict["type"] = self._integer_to_avro(field_props)
        elif t == "object":
            avro_type_dict["type"] = self._object_to_avro(field_props)
        elif t == "number":
            avro_type_dict["type"] = "double"
        elif t == "boolean":
            avro_type_dict["type"] = "boolean"
        elif t == "null":
            avro_type_dict["type"] = "null"
        else:
            raise NotImplementedError(
                f"Type '{t}' not support yet, "
                f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
            )

        return avro_type_dict

    def _handle_references(self, r: str, avro_type_dict: dict) -> dict:
        """Finds the type of a reference field"""
        class_name = r.replace(f"#/{DEFS_NAME}/", "")
        if class_name in self.classes_seen:
            avro_type_dict["type"] = class_name
            return avro_type_dict

        d = get_definition(r, self.root_schema)
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

    @staticmethod
    def _string_to_avro(f: Optional[str]):
        """Returns a type of a string field"""
        if not f:
            return "string"
        return STRING_TYPE_MAPPING[f]

    @staticmethod
    def _integer_to_avro(field_props: dict) -> str:
        """Returns a type of an integer field"""
        minimum = field_props.get("minimum")
        maximum = field_props.get("maximum")
        # integer in python can be a long, only if minimum and maximum value is set an int can be used
        if minimum is not None and minimum >= -(2**31) and maximum is not None and maximum <= (2**31 - 1):
            return "int"
        return "long"

    def _object_to_avro(self, field_props: dict) -> dict:
        """Returns a type of an object field"""
        a = field_props.get("additionalProperties")
        value_type = "string" if a is None else self._get_avro_type_dict(a)["type"]
        return {"type": "map", "values": value_type}

    def _union_to_avro(self, field_props: list, avro_type_dict: dict) -> dict:
        """Returns a type of a union field"""
        avro_type_dict["type"] = []
        for union_element in field_props:
            t = self._get_avro_type_dict(union_element)
            avro_type_dict["type"].append(t["type"])
        return avro_type_dict

    def _array_to_avro(self, field_props: dict, avro_type_dict: dict) -> dict:
        """Returns a type of an array field"""
        items = field_props["items"]
        tn = self._get_avro_type_dict(items)
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
