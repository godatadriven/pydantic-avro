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
