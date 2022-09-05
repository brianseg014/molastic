import json
import pytest

from molastic.core import (
    DynamicMapping,
    IllegalArgumentException,
    KeywordMapper,
    Mappers,
    StrictDynamicMappingException,
)


def test_mappers_parse():
    mappers = Mappers.parse(
        {
            "properties": {
                "field1": {"type": "keyword"},
                "field2": {
                    "type": "object",
                    "properties": {
                        "field3": {
                            "type": "keyword",
                        }
                    },
                },
            }
        }
    )
    assert mappers.get("field1").type == "keyword"
    assert mappers.get("field2").type == "object"
    assert mappers.get("field2.field3").type == "keyword"


def test_mappers_merge_with_new_field():
    mappers = Mappers.parse({"properties": {"field1": {"type": "keyword"}}})
    mappers.merge(
        {
            "properties": {
                "field2": {"properties": {"field3": {"type": "keyword"}}}
            }
        }
    )
    assert mappers.get("field1").type == "keyword"
    assert mappers.get("field2").type == "object"
    assert mappers.get("field2.field3").type == "keyword"


def test_mappers_merge_with_new_sub_field():
    mappers = Mappers.parse(
        {
            "properties": {
                "field1": {"properties": {"field2": {"type": "keyword"}}}
            }
        }
    )
    mappers.merge(
        {
            "properties": {
                "field1": {"properties": {"field3": {"type": "keyword"}}}
            }
        }
    )
    assert mappers.get("field1").type == "object"
    assert mappers.get("field1.field2").type == "keyword"
    assert mappers.get("field1.field3").type == "keyword"


def test_mappers_merge_raise_when_chaging_type():
    with pytest.raises(IllegalArgumentException):
        mappers = Mappers.parse({"properties": {"field": {"type": "keyword"}}})
        mappers.merge({"properties": {"field": {"type": "long"}}})


def test_mappers_dynamic_true_non_existent_field():
    mappers = Mappers()
    mappers.dynamic_map({"field1": {"field2": "value"}})
    assert mappers.get("field1").type == "object"
    assert mappers.get("field1.field2").type == "text"


def test_mappers_dynamic_true_existent_field():
    mappers = Mappers.parse({"properties": {"field": {"type": "keyword"}}})
    mappers.dynamic_map({"field": "value"})

    assert mappers.get("field")
    assert mappers.get("field").type == "keyword"


def test_mappers_dynamic_false():
    mappers = Mappers(
        dynamic_mapping=DynamicMapping(dynamic=DynamicMapping.Dynamic.false)
    )
    mappers.dynamic_map({"field": "value"})
    assert not mappers.has("field")


def test_mappers_dynamic_strict():
    with pytest.raises(StrictDynamicMappingException):
        mappers = Mappers(
            dynamic_mapping=DynamicMapping(
                dynamic=DynamicMapping.Dynamic.strict
            )
        )
        mappers.dynamic_map({"field": "value"})


def test_mappers_dynamic_true_data_types():
    mappers = Mappers(
        dynamic_mapping=DynamicMapping(dynamic=DynamicMapping.Dynamic.true)
    )
    mappers.dynamic_map(
        {
            "null": None,
            "bool": True,
            "number_with_point": 1.0,
            "number_without_point": 1,
            "array": [1],
            "string_with_date": "2022/01/01",
            "string_with_number": "1",
            "string": "molastic",
        }
    )
    assert not mappers.has("null")
    assert mappers.get("bool").type == "boolean"
    assert mappers.get("number_with_point").type == "float"
    assert mappers.get("number_without_point").type == "long"
    assert mappers.get("string_with_date").type == "date"
    assert mappers.get("string_with_number").type == "long"
    assert mappers.get("string").type == "text"


def test_mappers_dynamic_runtime_data_types():
    mappers = Mappers(
        dynamic_mapping=DynamicMapping(dynamic=DynamicMapping.Dynamic.runtime)
    )
    mappers.dynamic_map(
        {
            "null": None,
            "bool": True,
            "number_with_point": 1.0,
            "number_without_point": 1,
            "array": [1],
            "string_with_date": "2022/01/01",
            "string_with_number": "1",
            "string": "molastic",
        }
    )
    assert not mappers.has("null")
    assert mappers.get("bool").type == "boolean"
    assert mappers.get("number_with_point").type == "double"
    assert mappers.get("number_without_point").type == "long"
    assert mappers.get("string_with_date").type == "date"
    assert mappers.get("string_with_number").type == "long"
    assert mappers.get("string").type == "keyword"


def test_mappers_dynamic_date_detection_off():
    mappers = Mappers(dynamic_mapping=DynamicMapping(date_detection=False))
    mappers.dynamic_map({"string_with_date": "2022/01/01"})
    assert mappers.get("string_with_date").type != "date"


def test_mappers_dynamic_numeric_detection_off():
    mappers = Mappers(dynamic_mapping=DynamicMapping(numeric_detection=False))
    mappers.dynamic_map(
        {
            "string_with_number": "1",
            "string_with_number2": "1.0",
        }
    )
    assert mappers.get("string_with_number").type != "long"
    assert mappers.get("string_with_number2").type not in ["float", "double"]


def test_mappers_mappings():
    mappings = {
        "properties": {
            "field1": {"type": "keyword"},
            "field2": {"properties": {"field3": {"type": "keyword"}}},
        }
    }
    mappers = Mappers.parse(mappings)

    assert json.dumps(mappers.mappings) == json.dumps(mappings)
