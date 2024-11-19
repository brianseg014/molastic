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

- Types supported: Text, SearchAsYouType, Long, Float, Double, Boolean, Keyword, Date, Geopoint, Geoshape
- Analyzers: Standard
- Document APIs: Index, Update, Delete, Get, Exists
- Index APIs: Create index, Delete index, Exists, Update mapping, Get Mapping
    - Multi-Fields
- Queries DSL supported: DisjunctionMaxQuery, Boolean, Match, MatchBoolPrefix, MultiMatch, MatchAll, Term, Prefix, Range, Geoshape, Geodistance
- Scripting: painless (but maps cannot be accessed by dot notation)
