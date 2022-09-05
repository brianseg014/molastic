import json

from molastic import painless
from molastic import java_json


def test_painless_subset():
    painless.execute(
        "ctx['_source']['field'] = params['field'];",
        {
            "ctx": java_json.loads(
                json.dumps({"_source": {"field": "value"}})
            ),
            "params": java_json.loads(json.dumps({"field": "new_value"})),
        },
    )


def test_painless_monolith():
    painless.execute(
        'ctx["_source"]["locations"].add(params["location"]);',
        {
            "ctx": java_json.loads(json.dumps({"_source": {"locations": []}})),
            "params": java_json.loads(
                json.dumps({"location": {"lat": 0.0, "lon": 0.0}})
            ),
        },
    )
