from typing import List

from pydantic import BaseModel


class AvroBase(BaseModel):
    """This is base pydantic class that will add some methods"""

    @classmethod
    def avro_schema(cls) -> dict:
        """Return the avro schema for the pydantic class"""
        schema = cls.schema()
        return cls._avro_schema(schema)

    @staticmethod
    def _avro_schema(schema: dict) -> dict:
        """Return the avro schema for the given pydantic schema"""

        classes_seen = set()

        def get_definition(ref: str, schema: dict, prefix: str):
            """Reading definition of base schema for nested structs"""
            id = ref.replace("#/definitions/", "")
            d = schema.get("definitions", {}).get(id)
            if d is None:
                raise RuntimeError(f"Definition {id} does not exist")
            return get_fields(d, prefix)

        def get_type(key: str, value: dict, prefix: str) -> dict:
            """Returns a type of a single field"""
            if len(prefix) > 0:
                prefix += "_"
            t = value.get("type")
            f = value.get("format")
            r = value.get("$ref")
            avro_type_dict = {}
            if r is not None:
                prefix += key
                class_name = r.replace("#/definitions/", "")
                if class_name in classes_seen:
                    avro_type_dict["type"] = class_name
                else:
                    avro_type_dict["type"] = {
                        "type": "record",
                        "fields": get_definition(r, schema, prefix),
                        # Name of the struct should be unique true the complete schema
                        # Because of this the path in the schema is tracked and used as name for a nested struct/array
                        "name": class_name,
                    }
                    classes_seen.add(class_name)
            elif t == "array":
                items = value.get("items")
                tn = get_type(key, items, prefix)
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
            elif t == "string":
                avro_type_dict["type"] = "string"
            elif t == "number":
                avro_type_dict["type"] = "double"
            elif t == "integer":
                # integer in python can be a long
                avro_type_dict["type"] = "long"
            elif t == "boolean":
                avro_type_dict["type"] = "boolean"
            else:
                raise RuntimeError(f"Unknown type found: '{t}'")
            return avro_type_dict

        def get_fields(s: dict, prefix: str = "") -> List[dict]:
            """Return a list of fields of a struct"""
            fields = []

            required = s.get("required", [])
            for key, value in s.get("properties", {}).items():
                avro_type_dict = get_type(key, value, prefix)
                avro_type_dict["name"] = key

                if key not in required:
                    avro_type_dict["type"] = [avro_type_dict["type"], "null"]
                    avro_type_dict["default"] = None

                fields.append(avro_type_dict)
            return fields

        fields = get_fields(schema)

        return {"type": "record", "namespace": schema["title"], "name": schema["title"], "fields": fields}
