from functools import reduce
from typing import Optional

ENUM_KEY_STYLES = {"snake_case", "snake_case_upper"}


def convert_enum_key(key: str, style: Optional[str] = None) -> str:
    if not style:
        return key
    if style not in ENUM_KEY_STYLES:
        raise NotImplementedError(f"Invalid enum key style: {key}. Supported styles: {ENUM_KEY_STYLES}")
    snaked = reduce(lambda x, y: x + ("_" if y.isupper() else "") + y, key)
    return snaked.lower() if style == "snake_case" else snaked.upper()
