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

```bash
#!/usr/bin/env bash
# Print to stdout
pydantic-avro avro_to_pydantic --asvc /path/to/schema.asvc

# Save it to a file
pydantic-avro avro_to_pydantic --asvc /path/to/schema.asvc --output /path/to/output.py
```
