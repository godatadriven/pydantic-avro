from typing import Optional

from pydantic import BaseModel

from pydantic_avro.to_avro.config import PYDANTIC_V2
from pydantic_avro.to_avro.types import AvroTypeConverter


class AvroBase(BaseModel):
    """This class provides functionality to convert a pydantic model to an Avro schema."""

    @classmethod
    def avro_schema(cls, by_alias: bool = True, namespace: Optional[str] = None) -> dict:
        """Returns the avro schema for the pydantic class

        :param by_alias: generate the schemas using the aliases defined, if any
        :param namespace: Provide an optional namespace string to use in schema generation
        :return: dict with the Avro Schema for the model
        """
        schema = cls.model_json_schema(by_alias=by_alias) if PYDANTIC_V2 else cls.schema(by_alias=by_alias)

        if namespace is None:
            # Default namespace will be based on title
            namespace = schema["title"]

        avro_type_handler = AvroTypeConverter(schema)

        return cls._avro_schema(schema, namespace, avro_type_handler)

    @staticmethod
    def _avro_schema(schema: dict, namespace: str, avro_type_handler: AvroTypeConverter) -> dict:
        """Return the avro schema for the given pydantic schema"""

        fields = avro_type_handler.fields_to_avro_dicts(schema)
        return {"type": "record", "namespace": namespace, "name": schema["title"], "fields": fields}
