"""Test that the public API is properly exported and can be imported."""


def test_import_avrobase_from_main_package():
    """Test AvroBase can be imported from pydantic_avro package."""
    from pydantic_avro import AvroBase

    # Verify it's the correct class
    assert AvroBase.__name__ == "AvroBase"
    assert hasattr(AvroBase, "avro_schema")


def test_all_exports():
    """Test that __all__ is properly defined."""
    import pydantic_avro

    assert hasattr(pydantic_avro, "__all__")
    assert "AvroBase" in pydantic_avro.__all__


def test_avrobase_functionality():
    """Test that imported AvroBase works correctly."""
    from pydantic_avro import AvroBase

    class TestModel(AvroBase):
        name: str
        age: int

    # Test that avro_schema method works
    schema = TestModel.avro_schema()
    assert schema["type"] == "record"
    assert schema["name"] == "TestModel"
    assert len(schema["fields"]) == 2
