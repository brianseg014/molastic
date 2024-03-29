import json
import pytest

from molastic import core


def test_mappings_parser():
    mappers = core.MappingsParser.parse(
        {
            "properties": {
                "field1": {
                    "type": "text",
                    "fields": {"field1k": {"type": "keyword"}},
                },
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

    assert any(
        [
            m.targetpath == "field1" and isinstance(m, core.TextMapper)
            for m in mappers
        ]
    )
    assert any(
        [
            m.targetpath == "field1.field1k"
            and isinstance(m, core.KeywordMapper)
            for m in mappers
        ]
    )
    assert any(
        [
            m.targetpath == "field2.field3"
            and isinstance(m, core.KeywordMapper)
            for m in mappers
        ]
    )


def test_mappings_merge_with_new_field():
    mappings = core.MappingsMerger.merge(
        mapping1={"properties": {"field1": {"type": "keyword"}}},
        mapping2={
            "properties": {
                "field2": {"properties": {"field3": {"type": "keyword"}}}
            }
        },
    )
    assert json.dumps(mappings) == json.dumps(
        {
            "properties": {
                "field1": {"type": "keyword"},
                "field2": {"properties": {"field3": {"type": "keyword"}}},
            }
        }
    )


def test_mappings_merge_with_new_sub_field():
    mappings = core.MappingsMerger.merge(
        mapping1={
            "properties": {
                "field1": {"properties": {"field2": {"type": "keyword"}}}
            }
        },
        mapping2={
            "properties": {
                "field1": {"properties": {"field3": {"type": "keyword"}}}
            }
        },
    )
    assert json.dumps(mappings) == json.dumps(
        {
            "properties": {
                "field1": {
                    "properties": {
                        "field2": {"type": "keyword"},
                        "field3": {"type": "keyword"},
                    }
                }
            }
        }
    )


def test_mappings_merge_raise_when_chaging_type():
    with pytest.raises(core.IllegalArgumentException):
        core.MappingsMerger.merge(
            mapping1={"properties": {"field": {"type": "keyword"}}},
            mapping2={"properties": {"field": {"type": "long"}}},
        )


def test_mappings_dynamic_true_non_existent_field():
    dynamic_mapping = core.DynamicMapping()
    mappings = dynamic_mapping.map_source({"field1": {"field2": "value"}})
    assert json.dumps(mappings) == json.dumps(
        {"field1": {"properties": {"field2": {"type": "text"}}}}
    )


def test_mappings_dynamic_false():
    dynamic_mapping = core.DynamicMapping(
        dynamic=core.DynamicMapping.Dynamic.false
    )
    mappings = dynamic_mapping.map_source({"field": "value"})
    assert json.dumps(mappings) == json.dumps({})


def test_mappings_dynamic_strict():
    with pytest.raises(core.StrictDynamicMappingException):
        dynamic_mapping = core.DynamicMapping(
            dynamic=core.DynamicMapping.Dynamic.strict
        )
        dynamic_mapping.map_source({"field": "value"})


def test_mappings_dynamic_true_data_types():
    dynamic_mapping = core.DynamicMapping(
        dynamic=core.DynamicMapping.Dynamic.true
    )
    mappings = dynamic_mapping.map_source(
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
    assert json.dumps(mappings) == json.dumps(
        {
            "bool": {"type": "boolean"},
            "number_with_point": {"type": "float"},
            "number_without_point": {"type": "long"},
            "array": {"type": "long"},
            "string_with_date": {"type": "date"},
            "string_with_number": {"type": "long"},
            "string": {"type": "text"},
        }
    )


def test_mappings_dynamic_runtime_data_types():
    dynamic_mapping = core.DynamicMapping(
        dynamic=core.DynamicMapping.Dynamic.runtime
    )
    mappings = dynamic_mapping.map_source(
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
    assert json.dumps(mappings) == json.dumps(
        {
            "bool": {"type": "boolean"},
            "number_with_point": {"type": "double"},
            "number_without_point": {"type": "long"},
            "array": {"type": "long"},
            "string_with_date": {"type": "date"},
            "string_with_number": {"type": "long"},
            "string": {"type": "keyword"},
        }
    )


def test_mappings_dynamic_date_detection_off():
    dynamic_mapping = core.DynamicMapping(date_detection=False)
    mappings = dynamic_mapping.map_source({"string_with_date": "2022/01/01"})
    assert json.dumps(mappings) == json.dumps(
        {"string_with_date": {"type": "text"}}
    )


def test_mappings_dynamic_numeric_detection_off():
    dynamic_mapping = core.DynamicMapping(numeric_detection=False)
    mappings = dynamic_mapping.map_source(
        {
            "string_with_number": "1",
            "string_with_number2": "1.0",
        }
    )
    assert json.dumps(mappings) == json.dumps(
        {
            "string_with_number": {"type": "text"},
            "string_with_number2": {"type": "text"},
        }
    )
