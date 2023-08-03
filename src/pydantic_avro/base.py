from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from pydantic.version import VERSION as PYDANTIC_VERSION

PYDANTIC_V2 = PYDANTIC_VERSION.startswith("2.")

DEFS_NAME = "$defs" if PYDANTIC_V2 else "definitions"


class AvroBase(BaseModel):
    """This is base pydantic class that will add some methods"""

    @classmethod
    def avro_schema(cls, by_alias: bool = True, namespace: Optional[str] = None) -> dict:
        """
        Return the avro schema for the pydantic class

        :param by_alias: generate the schemas using the aliases defined, if any
        :param namespace: Provide an optional namespace string to use in schema generation
        :return: dict with the Avro Schema for the model
        """
        schema = cls.model_json_schema(by_alias=by_alias) if PYDANTIC_V2 else cls.schema(by_alias=by_alias)

        if namespace is None:
            # default namespace will be based on title
            namespace = schema["title"]

        return cls._avro_schema(schema, namespace)

    @staticmethod
    def _avro_schema(schema: dict, namespace: str) -> dict:
        """Return the avro schema for the given pydantic schema"""

        classes_seen = set()

        def get_definition(ref: str, schema: dict):
            """Reading definition of base schema for nested structs"""
            id = ref.replace(f"#/{DEFS_NAME}/", "")
            d = schema.get(DEFS_NAME, {}).get(id)
            if d is None:
                raise RuntimeError(f"Definition {id} does not exist")
            return d

        def get_type(value: dict) -> dict:
            """Returns a type of a single field"""
            t = value.get("type")
            f = value.get("format")
            r = value.get("$ref")
            a = value.get("additionalProperties")
            u = value.get("anyOf")
            minimum = value.get("minimum")
            maximum = value.get("maximum")
            avro_type_dict: Dict[str, Any] = {}
            if "default" in value:
                avro_type_dict["default"] = value.get("default")
            if "description" in value:
                avro_type_dict["doc"] = value.get("description")
            if "allOf" in value and len(value["allOf"]) == 1:
                r = value["allOf"][0]["$ref"]
            if u is not None:
                avro_type_dict["type"] = []
                for union_element in u:
                    avro_type_dict["type"].append(get_type(union_element)["type"])
            elif r is not None:
                class_name = r.replace(f"#/{DEFS_NAME}/", "")
                if class_name in classes_seen:
                    avro_type_dict["type"] = class_name
                else:
                    d = get_definition(r, schema)
                    if "enum" in d:
                        avro_type_dict["type"] = {
                            "type": "enum",
                            "symbols": [str(v) for v in d["enum"]],
                            "name": d["title"],
                        }
                    else:
                        avro_type_dict["type"] = {
                            "type": "record",
                            "fields": get_fields(d),
                            # Name of the struct should be unique true the complete schema
                            # Because of this the path in the schema is tracked and used as name for a nested struct/array
                            "name": class_name,
                        }

                    classes_seen.add(class_name)
            elif t == "array":
                items = value.get("items")
                tn = get_type(items)
                # If items in array are a object:
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
                if (
                    isinstance(tn, dict)
                    and isinstance(tn.get("type", {}), dict)
                    and tn.get("type", {}).get("type") == "array"
                ):
                    items = tn["type"]["items"]
                    tn = {"type": "array", "items": items}
                avro_type_dict["type"] = {"type": "array", "items": tn}
            elif t == "string" and f == "date-time":
                avro_type_dict["type"] = {
                    "type": "long",
                    "logicalType": "timestamp-micros",
                }
            elif t == "string" and f == "date":
                avro_type_dict["type"] = {
                    "type": "int",
                    "logicalType": "date",
                }
            elif t == "string" and f == "time":
                avro_type_dict["type"] = {
                    "type": "long",
                    "logicalType": "time-micros",
                }
            elif t == "string" and f == "uuid":
                avro_type_dict["type"] = {
                    "type": "string",
                    "logicalType": "uuid",
                }
            elif t == "string" and f == "binary":
                avro_type_dict["type"] = "bytes"
            elif t == "string":
                avro_type_dict["type"] = "string"
            elif t == "number":
                avro_type_dict["type"] = "double"
            elif t == "integer":
                # integer in python can be a long, only if minimum and maximum value is set a int can be used
                if minimum is not None and minimum >= -(2**31) and maximum is not None and maximum <= (2**31 - 1):
                    avro_type_dict["type"] = "int"
                else:
                    avro_type_dict["type"] = "long"
            elif t == "boolean":
                avro_type_dict["type"] = "boolean"
            elif t == "null":
                avro_type_dict["type"] = "null"
            elif t == "object":
                if a is None:
                    value_type = "string"
                else:
                    value_type = get_type(a)
                if isinstance(value_type, dict) and len(value_type) == 1:
                    value_type = value_type.get("type")
                avro_type_dict["type"] = {"type": "map", "values": value_type}
            else:
                raise NotImplementedError(
                    f"Type '{t}' not support yet, "
                    f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
                )
            return avro_type_dict

        def get_fields(s: dict) -> List[dict]:
            """Return a list of fields of a struct"""
            fields = []

            required = s.get("required", [])
            for key, value in s.get("properties", {}).items():
                avro_type_dict = get_type(value)
                avro_type_dict["name"] = key

                if key not in required:
                    if type(avro_type_dict["type"]) is list:
                        if "null" not in avro_type_dict["type"]:
                            avro_type_dict["type"].insert(0, "null")
                    elif avro_type_dict.get("default") is None:
                        avro_type_dict["type"] = ["null", avro_type_dict["type"]]
                    avro_type_dict.setdefault("default", None)

                # null must be the first element in the list
                if type(avro_type_dict["type"]) is list and "null" in avro_type_dict["type"]:
                    avro_type_dict["type"].remove("null")
                    avro_type_dict["type"].insert(0, "null")

                fields.append(avro_type_dict)
            return fields

        fields = get_fields(schema)

        return {"type": "record", "namespace": namespace, "name": schema["title"], "fields": fields}
