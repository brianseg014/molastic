# Molastic

## Install

```console
$ pip install molastic
```

## Quickstart

Molastic is a library to easymock out elasticsearch for your tests

```python
import molastic
import requests

def test_something():
    base_url = 'mock://elastic'
    with molastic.mock_elasticsearch(base_url):
        requests.post(
            url=f'{base_url}/my-index/_doc',
            json={ 
                "user": {
                    "id": "kimchy"
                } 
            }
        )
```

## Features

- Types supported: Long, Float, Boolean, Keyword, Date, Geopoint, Geoshape
- Index operations: Create index, Update mapping
- Document APIs: Index, Update, Delete
- Index APIs: Create index, Update mapping
- Queries DSL supported: Boolean, MatchAll, Term, Range, Geoshape, Geodistance, 
- Scripting: painless (but maps cannot be accessed by dot notation)