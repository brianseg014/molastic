import furl
import requests

from molastic import mock_elasticsearch


@mock_elasticsearch("mock://molastic")
def test_search_bool_must():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field": {"type": "keyword"},
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url),
        json={
            "field": "value",
        },
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path="my-index/_search")
    response = requests.get(
        str(search_url),
        json={"query": {"bool": {"must": [{"term": {"field": "value"}}]}}},
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    response = requests.get(
        str(search_url),
        json={
            "query": {"bool": {"must": [{"term": {"field": "not_the_value"}}]}}
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_bool_filter():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field": {"type": "keyword"},
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url),
        json={
            "field": "value",
        },
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path="my-index/_search")
    response = requests.get(
        str(search_url),
        json={"query": {"bool": {"filter": [{"term": {"field": "value"}}]}}},
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    response = requests.get(
        str(search_url),
        json={
            "query": {
                "bool": {"filter": [{"term": {"field": "not_the_value"}}]}
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_bool_should():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field_1": {"type": "keyword"},
                    "field_2": {"type": "keyword"},
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url), json={"field_1": "value_1", "field_2": "value_2"}
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path="my-index/_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "bool": {
                    "should": [
                        {"term": {"field_1": "value_1"}},
                        {"term": {"field_2": "not_the_value_2"}},
                    ]
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path="my-index/_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "bool": {
                    "minimum_should_match": 2,
                    "should": [
                        {"term": {"field_1": "value_1"}},
                        {"term": {"field_2": "not_the_value_2"}},
                    ],
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_bool_mustnot():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field": {"type": "keyword"},
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url),
        json={
            "field": "value",
        },
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path="my-index/_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "bool": {"must_not": [{"term": {"field": "not_the_value"}}]}
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    response = requests.get(
        str(search_url),
        json={"query": {"bool": {"must_not": [{"term": {"field": "value"}}]}}},
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_matchall():
    url = furl.furl("mock://molastic", path="my-index")

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(str(doc_url), json={"field": "do_match"})
    assert response.status_code == 201

    response = requests.post(str(doc_url), json={"field": "do_match"})
    assert response.status_code == 201

    search_url = furl.furl(str(url), path="my-index/_search")
    response = requests.get(str(search_url), json={"query": {"match_all": {}}})
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 2


@mock_elasticsearch("mock://molastic")
def test_search_term():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={"mappings": {"properties": {"field": {"type": "keyword"}}}},
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(str(doc_url), json={"field": "do_match"})
    assert response.status_code == 201

    response = requests.post(str(doc_url), json={"field": "do_not_match"})
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url), json={"query": {"term": {"field": "do_match"}}}
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1


@mock_elasticsearch("mock://molastic")
def test_search_prefix():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={"mappings": {"properties": {"field": {"type": "keyword"}}}},
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(str(doc_url), json={"field": "do_match"})
    assert response.status_code == 201

    response = requests.post(str(doc_url), json={"field": "do_not_match"})
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url), json={"query": {"prefix": {"field": "do_m"}}}
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1


@mock_elasticsearch("mock://molastic")
def test_search_range():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field_long": {"type": "long"},
                    "field_decimal": {"type": "float"},
                    "field_date": {"type": "date"},
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url),
        json={
            "field_long": 10,
            "field_decimal": 10.5,
            "field_date": "2022-01-01",
        },
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url), json={"query": {"range": {"field_long": {"gt": 15}}}}
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "range": {
                    "field_long": {"gt": 5, "gte": 10, "lt": 15, "lte": 10}
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "range": {
                    "field_decimal": {
                        "gt": "5",
                        "gte": 10,
                        "lt": 15.5,
                        "lte": 11,
                    }
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "range": {
                    "field_date": {
                        "gt": "2019-01-01",
                        "gte": "2022-01-01",
                        "lt": "2023-01-01",
                        "lte": "2022-01-01",
                    }
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "range": {
                    "field_date": {
                        "gt": "2019.01.01||-1d",
                        "gte": "2022-01-01",
                        "lt": "now+1y",
                        "lte": "2022-01-01",
                    }
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1


@mock_elasticsearch("mock://molastic")
def test_search_geodistance():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={"mappings": {"properties": {"location": {"type": "geo_point"}}}},
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url),
        json={"location": {"lat": 0.0, "lon": 0.0}},
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "geo_distance": {
                    "distance": "2km",
                    "location": {"lat": 0.0, "lon": 0.0},
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "geo_distance": {
                    "distance": "2km",
                    "location": {"lat": 37.4027209, "lon": -122.1811809},
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_geoshape():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={"mappings": {"properties": {"area": {"type": "geo_shape"}}}},
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url),
        json={
            "area": {
                "type": "polygon",
                "coordinates": [
                    [
                        (-92.090327, 17.883051),
                        (-87.944650, 17.883051),
                        (-88.909535, 13.269298),
                        (-92.443009, 14.619044),
                        (-92.090327, 17.883051),
                    ]
                ],
            }
        },
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "geo_shape": {
                    "area": {
                        "shape": {
                            "type": "point",
                            "coordinates": [-91.3560172, 15.7199869],
                        }
                    }
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    response = requests.get(
        str(search_url),
        json={
            "query": {
                "geo_shape": {
                    "area": {
                        "shape": {
                            "type": "point",
                            "coordinates": [-122.1811809, 37.4027209],
                        }
                    }
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_match():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={"mappings": {"properties": {"field": {"type": "text"}}}},
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(str(doc_url), json={"field": "this is a test"})
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url), json={"query": {"match": {"field": "this is a test"}}}
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url), json={"query": {"match": {"field": "piccolo my dog"}}}
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_match_bool_prefix():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={"mappings": {"properties": {"field": {"type": "text"}}}},
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(str(doc_url), json={"field": "this is a test"})
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url), json={"query": {"match": {"field": "this is a test"}}}
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={"query": {"match_bool_prefix": {"field": "piccolo my dog"}}},
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_multi_match_best_fields():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field1": {"type": "text"},
                    "field2": {"type": "text"},
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url),
        json={"field1": "this is a test", "field2": "this is also a test"},
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "multi_match": {
                    "query": "this is a test",
                    "type": "best_fields",
                    "fields": ["field1", "field2"],
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "multi_match": {
                    "query": "piccolo my dog",
                    "type": "best_fields",
                    "fields": ["field1", "field2"],
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_multi_match_bool_prefix():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field1": {"type": "text"},
                    "field2": {"type": "text"},
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(
        str(doc_url),
        json={"field1": "this is a test", "field2": "this is also a test"},
    )
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "multi_match": {
                    "query": "thi",
                    "type": "bool_prefix",
                    "fields": ["field1", "field2"],
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "multi_match": {
                    "query": "piccolo my dog",
                    "type": "bool_prefix",
                    "fields": ["field1", "field2"],
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0


@mock_elasticsearch("mock://molastic")
def test_search_multifield():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field": {
                        "type": "text",
                        "fields": {"field1k": {"type": "keyword"}},
                    }
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(str(doc_url), json={"field": "This is a test"})
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={"query": {"term": {"field.field1k": "This is a test"}}},
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1


@mock_elasticsearch("mock://molastic")
def test_search_multifield_search_as_you_type():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={
            "mappings": {
                "properties": {
                    "field": {
                        "type": "text",
                        "fields": {"suggest": {"type": "search_as_you_type"}},
                    }
                }
            }
        },
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(str(doc_url), json={"field": "This is a test"})
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url),
        json={
            "query": {
                "multi_match": {
                    "query": "this i",
                    "type": "bool_prefix",
                    "fields": [
                        "field.suggest",
                        "field.suggest._2gram",
                        "field.suggest._3gram",
                    ],
                }
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 1


@mock_elasticsearch("mock://molastic")
def test_search_with_mapping_but_missing_field_in_document():
    url = furl.furl("mock://molastic", path="my-index")

    response = requests.put(
        str(url),
        json={"mappings": {"properties": {"field": {"type": "keyword"}}}},
    )
    assert response.status_code == 200

    doc_url = furl.furl(str(url), path=url.path).add(path="_doc")
    response = requests.post(str(doc_url), json={"unknown_field": "do_match"})
    assert response.status_code == 201

    search_url = furl.furl(str(url), path=url.path).add(path="_search")
    response = requests.get(
        str(search_url), json={"query": {"term": {"field": "do_match"}}}
    )
    assert response.status_code == 200
    assert response.json()["hits"]["total"]["value"] == 0
