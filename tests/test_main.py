import runpy
from unittest.mock import MagicMock, patch

from pydantic_avro import __main__ as main_module


@patch("pydantic_avro.__main__.convert_file")
def test_main_avro_to_pydantic(mock_convert_file):
    # Call the main function with test arguments
    test_args = ["avro_to_pydantic", "--asvc", "test.avsc", "--output", "output.py"]
    main_module.main(test_args)

    # Assert that convert_file was called with the correct arguments
    mock_convert_file.assert_called_once_with("test.avsc", "output.py")


@patch.object(main_module, "main")
@patch.object(
    main_module.sys, "argv", ["__main__.py", "avro_to_pydantic", "--asvc", "test.avsc", "--output", "output.py"]
)
def test_root_main(mock_main):
    # Call the root_main function
    main_module.root_main()

    # Assert that main was called with the correct arguments
    mock_main.assert_called_once_with(["avro_to_pydantic", "--asvc", "test.avsc", "--output", "output.py"])
