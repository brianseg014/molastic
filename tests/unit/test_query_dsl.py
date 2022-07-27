from molastic import query_dsl


def test_minimum_should_match():
    assert query_dsl.BooleanQuery.MinimumShouldMatch("3").match(3, 5)
    assert not query_dsl.BooleanQuery.MinimumShouldMatch("3").match(1, 3)

    assert query_dsl.BooleanQuery.MinimumShouldMatch("-2").match(3, 5)
    assert not query_dsl.BooleanQuery.MinimumShouldMatch("-1").match(3, 5)
