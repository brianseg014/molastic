from molastic import core
from molastic import query_dsl


def test_minimum_should_match():
    assert query_dsl.BooleanQuery.MinimumShouldMatch("3").match(3, 5)
    assert not query_dsl.BooleanQuery.MinimumShouldMatch("3").match(1, 3)

    assert query_dsl.BooleanQuery.MinimumShouldMatch("-2").match(3, 5)
    assert not query_dsl.BooleanQuery.MinimumShouldMatch("-1").match(3, 5)


def test_geo_shape_polygon():
    query = query_dsl.GeoshapeQuery(
        core.GeoshapeMapper("coverage", "geo_shape"),
        shape=core.Geoshape.parse_single(
            {"type": "point", "coordinates": [-91.3560172, 15.7199869]}
        ),
        relation=query_dsl.GeoshapeQuery.Relation.CONTAINS,
    )
    matched = query.match(
        core.Document(
            _source={
                "coverage": {
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
            }
        )
    )
    assert matched


def test_geo_shape_multipolygon():
    query = query_dsl.GeoshapeQuery(
        core.GeoshapeMapper("coverage", "geo_shape"),
        shape=core.Geoshape.parse_single(
            {"type": "point", "coordinates": [-91.3560172, 15.7199869]}
        ),
        relation=query_dsl.GeoshapeQuery.Relation.CONTAINS,
    )
    matched = query.match(
        core.Document(
            _source={
                "coverage": {
                    "type": "multipolygon",
                    "coordinates": [
                        [
                            [
                                (-92.090327, 17.883051),
                                (-87.944650, 17.883051),
                                (-88.909535, 13.269298),
                                (-92.443009, 14.619044),
                                (-92.090327, 17.883051),
                            ]
                        ]
                    ],
                }
            }
        )
    )
    assert matched
