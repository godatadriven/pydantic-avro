from pydantic import VERSION as PYDANTIC_VERSION

PYDANTIC_V2 = PYDANTIC_VERSION.startswith("2.")
DEFS_NAME = "$defs" if PYDANTIC_V2 else "definitions"
