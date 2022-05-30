import ast

from molastic import painless


def test_parse():
    print(painless.parse('ctx["_source"]["locations"].add(params["location"]);'))
