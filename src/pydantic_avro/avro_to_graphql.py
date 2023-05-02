import glob
import json
import os
from typing import Optional, Union
from re import sub


def camel_type(s):
    """very simple camel caser"""
    camelled_type = sub(r"(_|-)+", " ", s).title()
    camelled_type = sub(r"[ !\[\]]+", "", camelled_type)
    if s[-1] != "!":
        camelled_type = "Optional" + camelled_type
    return camelled_type


def avsc_to_graphql(schema: dict, config: dict = None) -> dict:
    """Generate python code of pydantic of given Avro Schema"""
    if "type" not in schema or schema["type"] != "record":
        raise AttributeError("Type not supported")
    if "name" not in schema:
        raise AttributeError("Name is required")
    if "fields" not in schema:
        raise AttributeError("fields are required")

    classes: dict = {}

    def add_optional(py_type: str, optional) -> str:
        if optional:
            # if non-optional type but optional by union remove '!'
            if py_type[-1] == "!":
                return py_type[0:-1]
            return py_type
        else:
            return py_type + f"!"

    def get_directive_str(type_name: str, field_name: str, config: dict) -> str:
        if not config:
            return ""
        directive_str: str = ""
        if field_name in config["field_directives"]:
            directive_str += " " + config["field_directives"][field_name]
        if type_name in config["type_directives"]:
            type_directives = config["type_directives"][type_name]
            if field_name in type_directives:
                directive_str += " " + type_directives[field_name]
        return directive_str

    def get_graphql_type(t: Union[str, dict], force_optional: bool = False) -> str:
        """Returns python type for given avro type"""
        optional = force_optional
        optional_handled = False
        if isinstance(t, str):
            if t == "string":
                py_type = "String"
            elif t == "int":
                py_type = "Int"
            elif t == "long":
                py_type = "Float"
            elif t == "boolean":
                py_type = "Boolean"
            elif t == "double" or t == "float":
                py_type = "Float"
            elif t == "bytes":
                py_type = "String"
            elif t in classes:
                py_type = t
            else:
                t_without_namespace = t.split(".")[-1]
                if t_without_namespace in classes:
                    py_type = t_without_namespace
                else:
                    raise NotImplementedError(f"Type {t} not supported yet")
        elif isinstance(t, list):
            optional_handled = True
            if "null" in t and len(t) == 2:
                c = t.copy()
                c.remove("null")
                py_type = get_graphql_type(c[0], True)
            else:
                if "null" in t:
                    optional = True
                py_type = f"{' | '.join([ get_graphql_type(e, optional) for e in t if e != 'null'])}"
        elif t.get("logicalType") == "uuid":
            py_type = "ID"
        elif t.get("logicalType") == "decimal":
            py_type = "Float"
        elif (
            t.get("logicalType") == "timestamp-millis"
            or t.get("logicalType") == "timestamp-micros"
        ):
            py_type = "Int"
        elif (
            t.get("logicalType") == "time-millis"
            or t.get("logicalType") == "time-micros"
        ):
            py_type = "Int"
        elif t.get("logicalType") == "date":
            py_type = "String"
        elif t.get("type") == "enum":
            enum_name = t.get("name")
            if enum_name not in classes:
                enum_class = f"enum {enum_name} " + "{\n"
                for s in t.get("symbols"):
                    enum_class += f"    {s}\n"
                enum_class += "}\n"
                classes[enum_name] = enum_class
            py_type = enum_name
        elif t.get("type") == "string":
            py_type = "str"
        elif t.get("type") == "array":
            sub_type = get_graphql_type(t.get("items"))
            py_type = f"List[{sub_type}]"
        elif t.get("type") == "record":
            record_type_to_graphql(t)
            py_type = t.get("name")
        elif t.get("type") == "map":
            value_type = get_graphql_type(t.get("values"))
            tuple_type = camel_type(value_type) + "MapTuple"
            if tuple_type not in classes:
                tuple_class = f"""type {tuple_type} {{
     key: String
     value: [{value_type}]
}}\n"""
                classes[tuple_type] = tuple_class
            py_type = f"[{tuple_type}]"
        else:
            raise NotImplementedError(
                f"Type {t} not supported yet, "
                f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
            )
        if optional_handled:
            return py_type
        py_type = add_optional(py_type, optional)
        return py_type

    def record_type_to_graphql(schema: dict, config: dict = None):
        """Convert a single avro record type to a pydantic class"""
        type_name = schema["name"]
        current = f"type {type_name} " + "{\n"

        for field in schema["fields"]:
            field_name = field["name"]
            field_type = get_graphql_type(field["type"])
            field_directives = get_directive_str(type_name, field_name, config)
            default = field.get("default")
            if (
                field["type"] == "int"
                and "default" in field
                and isinstance(default, (bool, type(None)))
            ):
                current += f"    # use 'default' in queries, defaults not supported in graphql schemas\n"
                current += f"    {field_name}: {field_type}{field_directives}\n"
            elif field["type"] == "int" and "default" in field:
                current += f"    # use '{json.dumps(default)}' in queries, defaults not supported in graphql schemas\n"
                current += f"    {field_name}: {field_type}{field_directives}\n"
            elif field["type"] == "int":
                current += f"    {field_name}: {field_type}{field_directives}\n"
            elif "default" not in field:
                current += f"    {field_name}: {field_type}{field_directives}\n"
            elif isinstance(default, type(None)):
                current += f"    {field_name}: {field_type}{field_directives}\n"
            elif isinstance(default, bool):
                current += f"    # use '{default}' in queries, defaults not supported in graphql schemas\n"
                current += f"    {field_name}: {field_type}{field_directives}\n"
            else:
                current += f"    # use '{json.dumps(default)}' in queries, defaults not supported in graphql schemas\n"
                current += f"    {field_name}: {field_type}{field_directives}\n"
        if len(schema["fields"]) == 0:
            current += "    _void: String\n"

        current += "}\n"

        classes[type_name] = current

    record_type_to_graphql(schema, config)

    return classes


def classes_to_graphql_str(classes: dict) -> str:
    file_content = "# GENERATED GRAPHQL USING graphql_avro, DO NOT MANUALLY EDIT\n\n"
    file_content += "\n\n".join(sorted(classes.values()))

    return file_content


def get_config(config_json: Optional[str] = None) -> dict:
    if not config_json:
        return None
    with open(config_json, "r") as file_handler:
        return json.load(file_handler)


def convert_file(
    avsc_path: str, output_path: Optional[str] = None, config_json: Optional[str] = None
):
    config = get_config(config_json)
    with open(avsc_path, "r") as file_handler:
        avsc_dict = json.load(file_handler)
    file_content = avsc_to_graphql(avsc_dict, config=config)
    if output_path is None:
        print(file_content)
    else:
        with open(output_path, "w") as file_handler:
            file_handler.write(file_content)


def convert_files(
    avsc_folder: str,
    output_path: Optional[str] = None,
    config_json: Optional[str] = None,
):
    config = get_config(config_json)
    avsc_files: list = glob.glob("*.avsc", root_dir=avsc_folder, recursive=True)
    all_graphql_classes = {}
    for avsc_file in avsc_files:
        avsc_filepath = os.path.join(avsc_folder, avsc_file)
        with open(avsc_filepath, "r") as file_handle:
            avsc_dict = json.load(file_handle)
            if "type" in avsc_dict and avsc_dict["type"] == "enum":
                continue
        graphql_classes = avsc_to_graphql(avsc_dict, config=config)
        all_graphql_classes.update(graphql_classes)
    file_content = classes_to_graphql_str(all_graphql_classes)
    if output_path is None:
        print(file_content)
    else:
        with open(output_path, "w") as file_handle:
            file_handle.write(file_content)
