import pytest

from pydantic_avro.helpers import convert_enum_key


@pytest.mark.parametrize(
    ("key", "style", "expected"),
    [
        ("MyKeyName", "snake_case", "my_key_name"),
        ("MyKeyName", "snake_case_upper", "MY_KEY_NAME"),
        ("MyKeyName", None, "MyKeyName"),
    ]
)
def test_convert_enum_key(key: str, style: str, expected: str) -> None:
    assert convert_enum_key(key, style) == expected


def test_convert_enum_key_wrong_style() -> None:
    with pytest.raises(NotImplementedError) as e:
        convert_enum_key("MyKey", "test")
    assert "Invalid enum key style: MyKey. Supported styles: {'snake_case_upper', 'snake_case'}" in str(e.value)
