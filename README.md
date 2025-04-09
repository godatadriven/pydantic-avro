[![Python package](https://github.com/godatadriven/pydantic-avro/actions/workflows/python-package.yml/badge.svg)](https://github.com/godatadriven/pydantic-avro/actions/workflows/python-package.yml)
[![codecov](https://codecov.io/gh/godatadriven/pydantic-avro/branch/main/graph/badge.svg?token=5L08GOERAW)](https://codecov.io/gh/godatadriven/pydantic-avro)
[![PyPI version](https://badge.fury.io/py/pydantic-avro.svg)](https://badge.fury.io/py/pydantic-avro)
[![CodeQL](https://github.com/godatadriven/pydantic-avro/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/godatadriven/pydantic-avro/actions/workflows/codeql-analysis.yml)

# pydantic-avro

This library can convert a pydantic class to a avro schema or generate python code from a avro schema.

### Install

```bash
pip install pydantic-avro
```

### Pydantic class to avro schema

```python
import json
from typing import Optional

from pydantic_avro.base import AvroBase


class TestModel(AvroBase):
    key1: str
    key2: int
    key2: Optional[str]


schema_dict: dict = TestModel.avro_schema()
print(json.dumps(schema_dict))

```

### Avro schema to pydantic

```shell
# Print to stdout
pydantic-avro avro_to_pydantic --asvc /path/to/schema.asvc

# Save it to a file
pydantic-avro avro_to_pydantic --asvc /path/to/schema.asvc --output /path/to/output.py
```

### Specify expected Avro type

```python
from datetime import datetime
from pydantic import Field
from pydantic_avro.base import AvroBase 

class ExampleModel(AvroBase):
    field1: int = Field(..., avro_type="long")  # Explicitly set Avro type to "long"
    field2: datetime = Field(..., avro_type="timestamp-millis")  # Explicitly set Avro type to "timestamp-millis"
```

### Install for developers

###### Install package

- Requirement: Poetry 1.*

```shell
poetry install
```

###### Run unit tests
```shell
pytest
coverage run -m pytest  # with coverage
# or (depends on your local env) 
poetry run pytest
poetry run coverage run -m pytest  # with coverage
```

##### Run linting

The linting is checked in the github workflow. To fix and review issues run this:
```shell
black .   # Auto fix all issues
isort .   # Auto fix all issues
pflake .  # Only display issues, fixing is manual
```
