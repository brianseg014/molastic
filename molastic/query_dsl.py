from __future__ import annotations

import re
import abc
import typing

from . import core
from . import utils


class Hit(typing.TypedDict):
    _index: str
    _id: str
    _score: float
    _source: dict
    fields: typing.Optional[dict]


def count(body: dict, indices: typing.Iterable[core.Indice]) -> int:
    return len(search(body, indices))


def search(
    body: dict, indices: typing.Iterable[core.Indice]
) -> typing.Sequence[Hit]:
    hits: typing.List[Hit] = []

    for indice in indices:
        hits.extend(run(body, indice))

    return hits


def run(body: dict, context: core.Indice) -> typing.Sequence[Hit]:
    query = parse(body, context)
    return query.run(context.documents)


def parse(body: dict, context):
    if "query" in body:
        query = SimpleQuery(
            parse_compound_and_leaf_query(body["query"], context)
        )
    else:
        query = SimpleQuery(MatchAllQuery())
    return QueryDSL(query)


def parse_compound_and_leaf_query(
    body: dict, context
) -> typing.Union[CompoundQuery, LeafQuery]:
    query_type = next(iter(body.keys()))
    if len(body) > 1:
        raise core.ParsingException(
            f"[{query_type}] malformed query, expected [END_OBJECT] "
            "but found [FIELD_NAME]"
        )

    if query_type == "match_all":
        return MatchAllQuery()

    if query_type == "bool":
        return BooleanQuery.parse(body[query_type], context)

    if query_type == "term":
        return TermQuery.parse(body[query_type], context)

    if query_type == "prefix":
        return PrefixQuery.parse(body[query_type], context)

    if query_type == "range":
        return RangeQuery.parse(body[query_type], context)

    if query_type == "geo_distance":
        return GeodistanceQuery.parse(body[query_type], context)

    if query_type == "geo_shape":
        return GeoshapeQuery.parse(body[query_type], context)

    if query_type == "match":
        return MatchQuery.parse(body[query_type], context)

    if query_type == "match_bool_prefix":
        return MatchBoolPrefixQuery.parse(body[query_type], context)

    if query_type == "multi_match":
        return MultiMatchQuery.parse(body[query_type], context)

    raise Exception("unknown query type", query_type)


class QueryShardException(core.ElasticError):
    pass


class QueryDSL:
    def __init__(self, query: SimpleQuery) -> None:
        self.query = query

    def run(
        self, documents: typing.Iterable[core.Document]
    ) -> typing.Iterable[Hit]:

        hits = tuple(d for d in documents if self.query.match(d))

        return (
            Hit(
                _index=d["_index"]._id,
                _id=d["_id"],
                _score=0.0,
                _source=d["_source"],
                fields=None,
            )
            for d in hits
        )


class Query(abc.ABC):
    @abc.abstractmethod
    def score(self, document: core.Document) -> float:
        pass

    @abc.abstractmethod
    def match(self, document: core.Document) -> bool:
        pass


class SimpleQuery(Query):
    def __init__(self, query: typing.Union[CompoundQuery, LeafQuery]) -> None:
        super().__init__()
        self.query = query

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        return self.query.match(document)


class LeafQuery(Query):
    pass


class CompoundQuery(Query):
    pass


BoolOccurType = typing.Optional[
    typing.Sequence[typing.Union[CompoundQuery, LeafQuery]]
]


class BooleanQuery(CompoundQuery):
    class MinimumShouldMatch:
        INTEGER_PATTERN = re.compile(r"^(?P<value>\d+)$")
        NEGATIVE_INTEGER_PATTERN = re.compile(r"^-(?P<value>\d+)$")
        PERCENTAGE_PATTERN = re.compile(r"^\d+%$")
        NEGATIVE_PERCENTAGE_PATTERN = re.compile(r"^-\d+%$")

        def __init__(self, param: typing.Union[int, str]) -> None:
            self.param = param

        def match(
            self, optional_clauses_matched: int, total_optional_clauses: int
        ) -> bool:

            interger_match = (
                BooleanQuery.MinimumShouldMatch.INTEGER_PATTERN.match(
                    str(self.param)
                )
            )
            if interger_match is not None:
                # Fixed value
                value = int(interger_match.group("value"))
                return optional_clauses_matched >= value

            negative_integer_match = (
                BooleanQuery.MinimumShouldMatch.NEGATIVE_INTEGER_PATTERN.match(
                    str(self.param)
                )
            )
            if negative_integer_match is not None:
                # Total minus param should be mandatory
                value = int(negative_integer_match.group("value"))
                return (
                    optional_clauses_matched >= total_optional_clauses - value
                )

            raise NotImplementedError(
                "only integer and negative integer implemeted"
            )

    def __init__(
        self,
        must: BoolOccurType = None,
        filter: BoolOccurType = None,
        should: BoolOccurType = None,
        must_not: BoolOccurType = None,
        minimum_should_match: typing.Optional[MinimumShouldMatch] = None,
        boost: float = 1.0,
    ) -> None:
        self.must = must or []
        self.filter = filter or []
        self.should = should or []
        self.must_not = must_not or []

        if minimum_should_match is None:
            if (
                len(self.should) >= 1
                and len(self.must) == 0
                and len(self.filter) == 0
            ):
                self.minimum_should_match = BooleanQuery.MinimumShouldMatch(1)
            else:
                self.minimum_should_match = BooleanQuery.MinimumShouldMatch(0)
        else:
            self.minimum_should_match = minimum_should_match

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        must = sum(1 for q in self.must if q.match(document))
        filter = sum(1 for q in self.filter if q.match(document))
        should = sum(1 for q in self.should if q.match(document))
        must_not = sum(1 for q in self.must_not if not q.match(document))

        matched = True
        matched = matched and must == len(self.must)
        matched = matched and filter == len(self.filter)
        matched = matched and self.minimum_should_match.match(
            should, len(self.should)
        )
        matched = matched and must_not == len(self.must_not)

        return matched

    @classmethod
    def parse(self, body: dict, context: core.Indice) -> BooleanQuery:
        not_recognized = {
            k: v
            for k, v in body.items()
            if k
            not in [
                "must",
                "filter",
                "should",
                "must_not",
                "minimum_should_match",
            ]
        }
        if len(not_recognized) > 0:
            first_param = list(not_recognized)[0]
            raise core.ParsingException(
                f"query does not support [{first_param}]"
            )

        must_body = body.get("must", [])
        filter_body = body.get("filter", [])
        should_body = body.get("should", [])
        must_not_body = body.get("must_not", [])

        if not isinstance(must_body, list):
            raise NotImplementedError("bool must only array supported")
        if not isinstance(filter_body, list):
            raise NotImplementedError("bool filter only array supported")
        if not isinstance(should_body, list):
            raise NotImplementedError("bool should only array supported")
        if not isinstance(must_not_body, list):
            raise NotImplementedError("bool must_not only array supported")

        minimum_should_match_body = body.get("minimum_should_match", None)
        if minimum_should_match_body is not None:
            minimum_should_match = BooleanQuery.MinimumShouldMatch(
                str(minimum_should_match_body)
            )
        else:
            minimum_should_match = None

        return BooleanQuery(
            must=tuple(
                [parse_compound_and_leaf_query(q, context) for q in must_body]
            ),
            filter=tuple(
                [
                    parse_compound_and_leaf_query(q, context)
                    for q in filter_body
                ]
            ),
            should=tuple(
                [
                    parse_compound_and_leaf_query(q, context)
                    for q in should_body
                ]
            ),
            must_not=tuple(
                [
                    parse_compound_and_leaf_query(q, context)
                    for q in must_not_body
                ]
            ),
            minimum_should_match=minimum_should_match,
        )


class DisjuntionMaxQuery(CompoundQuery):
    def __init__(
        self, queries: typing.Sequence[Query], tie_breaker: float = 0.0
    ) -> None:
        self.queries = queries
        self.tie_breaker = tie_breaker

    def score(self, document: core.Document) -> float:
        return 0

    def match(self, document: core.Document) -> bool:
        return any(q.match(document) for q in self.queries)

    @classmethod
    def parse(self, body: dict, context: core.Indice) -> DisjuntionMaxQuery:
        not_recognized = {
            k: v
            for k, v in body.items()
            if k
            not in [
                "queries",
                "tie_breaker",
            ]
        }
        if len(not_recognized) > 0:
            first_param = list(not_recognized)[0]
            raise core.ParsingException(
                f"query does not support [{first_param}]"
            )

        return DisjuntionMaxQuery(
            queries=tuple(
                [
                    parse_compound_and_leaf_query(q, context)
                    for q in body.get("queries", [])
                ]
            ),
            tie_breaker=body.get("tie_breaker", 0.0),
        )


class MatchAllQuery(LeafQuery):
    def score(self, document: core.Document) -> float:
        return 0.0

    def match(self, _: core.Document) -> bool:
        return True


class TermQuery(LeafQuery):
    def __init__(
        self,
        mapper: core.Mapper,
        lookup_value: core.Value,
        boost: float = 1.0,
        case_insensitive: bool = False,
    ) -> None:
        super().__init__()
        self.mapper = mapper
        self.lookup_value = lookup_value
        self.boost = boost
        self.case_insensitive = case_insensitive

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        stored_body = core.read_from_document(
            self.mapper.sourcepath, document, None
        )
        if stored_body is None:
            return False

        for stored_value in self.mapper.map(stored_body):
            if stored_value == self.lookup_value:
                return True

        return False

    @classmethod
    def parse(cls, body: dict, context: core.Indice) -> TermQuery:
        if isinstance(body, dict):
            return cls.parse_object(body, context)

        raise core.ParsingException(
            "[term] query malformed, no start_object after query name"
        )

    @classmethod
    def parse_object(cls, body: dict, context: core.Indice) -> TermQuery:
        body_fields = {k: v for k, v in body.items()}

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise core.ParsingException(
                "[term] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_fields) == 0:
            raise core.IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        mapperpath, body_fieldprops = next(iter(body_fields.items()))

        mapper = context.mappers.get(mapperpath)

        if isinstance(body_fieldprops, str):
            lookup_values = mapper.map(body_fieldprops)
            return TermQuery(mapper, next(iter(lookup_values)))

        elif isinstance(body_fieldprops, dict):
            body_value = body_fieldprops.get("value", None)
            if body_value is None:
                raise core.IllegalArgumentException("value cannot be null")

            lookup_values = mapper.map(body_fieldprops)
            return TermQuery(mapper, next(iter(lookup_values)))

        elif isinstance(body_fieldprops, list):
            raise core.ParsingException(
                "[term] query does not support array of values"
            )

        else:
            raise core.ParsingException(
                "[term] query does not support long, float, boolean"
            )


class PrefixQuery(LeafQuery):
    def __init__(
        self,
        mapper: core.Mapper,
        lookup_value: core.Value,
        boost: float = 1.0,
        case_insensitive: bool = False,
    ) -> None:
        super().__init__()
        self.mapper = mapper
        self.lookup_value = lookup_value
        self.boost = boost
        self.case_insensitive = case_insensitive

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        stored_body = core.read_from_document(
            self.mapper.sourcepath, document, None
        )
        if stored_body is None:
            return False

        for stored_value in self.mapper.map(stored_body):
            stored_value = typing.cast(core.Keyword, stored_value)
            if stored_value.startswith(
                typing.cast(core.Keyword, self.lookup_value)
            ):
                return True

        return False

    @classmethod
    def parse(cls, body: dict, context: core.Indice) -> PrefixQuery:
        if isinstance(body, dict):
            return cls.parse_object(body, context)

        raise core.ParsingException(
            "[prefix] query malformed, no start_object after query name"
        )

    @classmethod
    def parse_object(cls, body: dict, context: core.Indice) -> PrefixQuery:
        body_fields = {k: v for k, v in body.items()}

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise core.ParsingException(
                "[prefix] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_fields) == 0:
            raise core.IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        mapperpath, body_fieldprops = next(iter(body_fields.items()))

        mapper = context.mappers.get(mapperpath)

        if mapper.type not in ["keyword"]:
            raise QueryShardException(
                f"Field [{mapperpath}] is of unsupported type [{mapper.type}] for [range] query"
            )

        if isinstance(body_fieldprops, str):
            lookup_values = mapper.map(body_fieldprops)
            return PrefixQuery(mapper, next(iter(lookup_values)))

        elif isinstance(body_fieldprops, dict):
            body_value = body_fieldprops.get("value", None)
            if body_value is None:
                raise core.IllegalArgumentException("value cannot be null")

            lookup_values = mapper.map(body_fieldprops)
            return PrefixQuery(mapper, next(iter(lookup_values)))

        elif isinstance(body_fieldprops, list):
            raise core.ParsingException(
                "[prefix] query does not support array of values"
            )

        else:
            raise core.ParsingException(
                "[prefix] query does not support long, float, boolean"
            )


RangeQueryValue = typing.Optional[
    typing.Union[core.Long, core.Float, core.Double, core.Date]
]


class RangeQuery(LeafQuery):
    class Relation(utils.CaseInsensitveEnum):
        INTERSECTS = "INTERSECTS"
        CONTAINS = "CONTAINS"
        WITHIN = "WITHIN"

    def __init__(
        self,
        mapper: typing.Union[
            core.LongMapper,
            core.FloatMapper,
            core.DoubleMapper,
            core.DateMapper,
        ],
        gte: RangeQueryValue = None,
        gt: RangeQueryValue = None,
        lt: RangeQueryValue = None,
        lte: RangeQueryValue = None,
        relation: Relation = Relation.INTERSECTS,
        boost: float = 1.0,
    ) -> None:
        self.mapper = mapper
        self.gte = gte
        self.gt = gt
        self.lt = lt
        self.lte = lte
        self.relation = relation
        self.boost = boost

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        stored_body = core.read_from_document(
            self.mapper.sourcepath, document, None
        )
        if stored_body is None:
            return False

        for stored_value in self.mapper.map(stored_body):

            satisfied = True
            if self.gte is not None:
                satisfied = satisfied and stored_value >= self.gte  # type: ignore
            if self.gt is not None:
                satisfied = satisfied and stored_value > self.gt  # type: ignore
            if self.lte is not None:
                satisfied = satisfied and stored_value <= self.lte  # type: ignore
            if self.lt is not None:
                satisfied = satisfied and stored_value < self.lt  # type: ignore

            if satisfied:
                return True

        return False

    @classmethod
    def parse(cls, body: dict, context: core.Indice) -> RangeQuery:
        body_fields = {k: v for k, v in body.items() if isinstance(v, dict)}
        body_params = {
            k: v for k, v in body.items() if not isinstance(v, dict)
        }

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise core.ParsingException(
                "[range] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_params) > 1:
            first_param = list(body_params)[0]
            raise core.ParsingException(
                f"query does not support [{first_param}]"
            )

        if len(body_fields) == 0:
            raise core.IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        mapperpath, body_fieldprops = next(iter(body_fields.items()))

        mapper = context.mappers.get(mapperpath)

        if mapper.type not in [
            "long",
            "float",
            "boolean",
            "date",
        ]:
            raise QueryShardException(
                f"Field [{mapperpath}] is of unsupported type [{mapper.type}] for [range] query"
            )

        body_gte = body_fieldprops.get("gte", None)
        body_gt = body_fieldprops.get("gt", None)
        body_lte = body_fieldprops.get("lte", None)
        body_lt = body_fieldprops.get("lt", None)

        gte: RangeQueryValue = None
        gt: RangeQueryValue = None
        lt: RangeQueryValue = None
        lte: RangeQueryValue = None
        if mapper.type == "date":
            assert isinstance(mapper, core.DateMapper)

            body_format = body_fieldprops.get("format", mapper.format)
            if isinstance(body_gte, str):
                if core.Date.match_date_math_pattern(body_gte):
                    gte = core.Date.parse_date_math(body_gte)
                else:
                    gte = core.Date.parse_single(body_gte, body_format)
            if isinstance(body_gt, str):
                if core.Date.match_date_math_pattern(body_gt):
                    gt = core.Date.parse_date_math(body_gt)
                else:
                    gt = core.Date.parse_single(body_gt, body_format)
            if isinstance(body_lte, str):
                if core.Date.match_date_math_pattern(body_lte):
                    lte = core.Date.parse_date_math(body_lte)
                else:
                    lte = core.Date.parse_single(body_lte, body_format)
            if isinstance(body_lt, str):
                if core.Date.match_date_math_pattern(body_lt):
                    lt = core.Date.parse_date_math(body_lt)
                else:
                    lt = core.Date.parse_single(body_lt, body_format)

        else:  # No date
            assert isinstance(
                mapper,
                (core.LongMapper, core.FloatMapper, core.DoubleMapper),
            )

            parser: typing.Callable[
                [typing.Any], typing.Union[core.Long, core.Float, core.Double]
            ]
            if mapper.type == "long":
                parser = core.Long.parse_single
            elif mapper.type == "float":
                parser = core.Float.parse_single
            elif mapper.type == "double":
                parser = core.Double.parse_single
            else:
                raise NotImplementedError(mapper.type)

            if body_gte is not None and gte is None:
                gte = parser(body_gte)
            if body_gt is not None and gt is None:
                gt = parser(body_gt)
            if body_lte is not None and lte is None:
                lt = parser(body_lt)
            if body_lt is not None and lt is None:
                lte = parser(body_lte)

        return RangeQuery(mapper, gte, gt, lt, lte)


class GeoshapeQuery(LeafQuery):
    class Relation(utils.CaseInsensitveEnum):
        INTERSECTS = "INTERSECTS"
        CONTAINS = "CONTAINS"

    def __init__(
        self,
        mapper: core.GeoshapeMapper,
        shape: core.Geoshape,
        relation: Relation,
    ) -> None:
        super().__init__()
        self.mapper = mapper
        self.shape = shape
        self.relation = relation

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        stored_body = core.read_from_document(
            self.mapper.sourcepath, document, None
        )
        if stored_body is None:
            return False

        for stored_value in self.mapper.map(stored_body):
            stored_value = typing.cast(core.Geoshape, stored_value)

            if self.relation == GeoshapeQuery.Relation.INTERSECTS:
                satisfied = stored_value.intersects(self.shape)
            elif self.relation == GeoshapeQuery.Relation.CONTAINS:
                satisfied = stored_value.contains(self.shape)
            else:
                raise NotImplementedError(
                    f"GeoshapeQuery with relation [{self.relation}]"
                )

            if satisfied:
                return True

        return False

    @classmethod
    def parse(self, body: dict, context: core.Indice) -> GeoshapeQuery:
        body_fields = {k: v for k, v in body.items() if isinstance(v, dict)}
        body_params = {
            k: v for k, v in body.items() if not isinstance(v, dict)
        }

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise core.ParsingException(
                "[range] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_params) > 0:
            first_param = list(body_params)[0]
            raise core.ParsingException(
                f"query does not support [{first_param}]"
            )

        if len(body_fields) == 0:
            raise core.IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        mapperpath, body_fieldprops = next(iter(body_fields.items()))

        mapper = context.mappers.get(mapperpath)

        if mapper.type not in ["geo_shape"]:
            raise QueryShardException(
                f"Field [{mapperpath}] is of unsupported type [{mapper.type}] for [geo_shape] query"
            )

        if (
            "shape" not in body_fieldprops
            and "indexedShapeId" not in body_fieldprops
        ):
            raise core.IllegalArgumentException(
                "either shape or indexShapedId is required"
            )

        if "shape" in body_fieldprops:
            body_relation = body_fieldprops.get(
                "relation", GeoshapeQuery.Relation.INTERSECTS.value
            )

            shape = core.Geoshape.parse_single(body_fieldprops["shape"])
            relation = GeoshapeQuery.Relation(body_relation)

            assert isinstance(mapper, core.GeoshapeMapper)
            return GeoshapeQuery(mapper, shape, relation)
        elif "indexedShapeId" in body_fieldprops:
            raise NotImplementedError("indexedShapeId")
        else:
            raise core.ParsingException(
                "[Geoshape] query does not support inputs"
            )


class GeodistanceQuery(LeafQuery):
    def __init__(
        self,
        mapper: core.GeopointMapper,
        distance: core.Geodistance,
        value: core.Geopoint,
        distance_type: core.Geopoint.DistanceType,
    ) -> None:
        super().__init__()
        self.mapper = mapper
        self.distance = distance
        self.value = value
        self.distance_type = distance_type

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        stored_body = core.read_from_document(
            self.mapper.sourcepath, document, None
        )
        if stored_body is None:
            return False

        for stored_value in self.mapper.map(stored_body):
            distance = stored_value.distance(self.value, self.distance_type)
            if distance <= self.distance:
                return True

        return False

    @classmethod
    def parse(cls, body: dict, context: core.Indice) -> GeodistanceQuery:
        body_fields = {
            k: v
            for k, v in body.items()
            if k
            not in ["distance", "distance_type", "_name", "validation_method"]
        }
        body_params = {
            k: v
            for k, v in body.items()
            if k in ["distance", "distance_type", "_name", "validation_method"]
        }

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise core.ParsingException(
                "[range] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_fields) == 0:
            raise core.IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        mapperpath, body_fieldprops = next(iter(body_fields.items()))

        mapper = context.mappers.get(mapperpath)

        if mapper.type not in ["geo_point"]:
            raise QueryShardException(
                f"Field [{mapperpath}] is of unsupported type "
                f"[{mapper.type}] for [geo_shape] query"
            )

        body_distance = body_params["distance"]
        body_distance_type = body_params.get(
            "distance_type", core.Geopoint.DistanceType.ARC.value
        )

        distance = core.Geodistance.parse_single(body_distance)
        distance_type = core.Geopoint.DistanceType[body_distance_type]
        point = core.Geopoint.parse_single(body_fieldprops)

        assert isinstance(mapper, core.GeopointMapper)
        return GeodistanceQuery(mapper, distance, point, distance_type)


class MatchTermQuery(LeafQuery):
    def __init__(
        self,
        mapper: core.TextMapper,
        lookup_word: str,
    ) -> None:
        super().__init__()
        self.mapper = mapper
        self.lookup_word = lookup_word

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        stored_body = core.read_from_document(
            self.mapper.sourcepath, document, None
        )
        if stored_body is None:
            return False

        for stored_value in self.mapper.map(stored_body):
            assert isinstance(stored_value, core.Text)
            if self.lookup_word in stored_value:
                return True

        return False


class MatchPrefixQuery(LeafQuery):
    def __init__(
        self,
        mapper: core.TextMapper,
        lookup_word: str,
    ) -> None:
        super().__init__()
        self.mapper = mapper
        self.lookup_word = lookup_word

    def score(self, document: core.Document) -> float:
        return 1.0

    def match(self, document: core.Document) -> bool:
        stored_body = core.read_from_document(
            self.mapper.sourcepath, document, None
        )
        if stored_body is None:
            return False

        for stored_value in self.mapper.map(stored_body):
            assert isinstance(stored_value, core.Text)
            if any(
                token.startswith(self.lookup_word) for token in stored_value
            ):
                return True

        return False


class MatchQuery(LeafQuery):
    def __init__(
        self,
        mapper: core.TextMapper,
        lookup_value: core.Text,
    ) -> None:
        super().__init__()
        self.mapper = mapper
        self.lookup_value = lookup_value

        self.bool_query = BooleanQuery(
            should=[MatchTermQuery(self.mapper, w) for w in self.lookup_value]
        )

    def score(self, document: core.Document) -> float:
        return self.bool_query.score(document)

    def match(self, document: core.Document) -> bool:
        return self.bool_query.match(document)

    @classmethod
    def parse(cls, body: dict, context: core.Indice) -> MatchQuery:
        body_fields = {k: v for k, v in body.items()}

        if len(body_fields) == 0:
            raise core.IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise core.ParsingException(
                "[match] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        mapperpath, body_fieldprops = next(iter(body_fields.items()))

        mapper = context.mappers.get(mapperpath)

        if mapper.type not in ["text"]:
            raise QueryShardException(
                f"Field [{mapperpath}] is of unsupported type [{mapper.type}] for [match] query"
            )

        assert isinstance(mapper, core.TextMapper)

        if isinstance(body_fieldprops, str):
            lookup_values = mapper.map(body_fieldprops)
            return MatchQuery(mapper, next(iter(lookup_values)))

        elif isinstance(body_fieldprops, dict):
            body_fieldprops_unrecognized = {
                k: v
                for k, v in body_fieldprops.items()
                if k
                not in [
                    "query",
                    "analyzer",
                    "auto_generate_synonyms_phrase_query",
                    "fuzziness",
                    "max_expansions",
                    "prefix_length",
                    "fuzzy_transpositions",
                    "fuzzy_rewrite",
                    "lenient",
                    "operator",
                    "minimum_should_match",
                    "zero_terms_query",
                ]
            }

            if len(body_fieldprops_unrecognized) > 0:
                first_param = list(body_fieldprops_unrecognized)[0]
                raise core.ParsingException(
                    f"query does not support [{first_param}]"
                )

            lookup_values = mapper.map(body_fieldprops["query"])

            return MatchQuery(mapper, next(iter(lookup_values)))

        elif isinstance(body_fieldprops, list):
            raise core.ParsingException(
                "[match] query does not support array of values"
            )

        else:
            raise core.ParsingException(
                "[match] query does not support long, float, boolean"
            )


class MatchBoolPrefixQuery(LeafQuery):
    def __init__(
        self,
        mapper: core.TextMapper,
        lookup_value: core.Text,
    ) -> None:
        super().__init__()
        self.mapper = mapper
        self.lookup_value = lookup_value

        lookup_values = list(self.lookup_value)
        self.bool_query = BooleanQuery(
            should=[
                *[MatchTermQuery(self.mapper, w) for w in lookup_values[:-1]],
                MatchPrefixQuery(self.mapper, lookup_values[-1]),
            ]
        )

    def score(self, document: core.Document) -> float:
        return self.bool_query.score(document)

    def match(self, document: core.Document) -> bool:
        return self.bool_query.match(document)

    @classmethod
    def parse(cls, body: dict, context: core.Indice) -> MatchBoolPrefixQuery:
        body_fields = {k: v for k, v in body.items()}

        if len(body_fields) == 0:
            raise core.IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise core.ParsingException(
                "[match bool prefix] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        mapperpath, body_fieldprops = next(iter(body_fields.items()))

        mapper = context.mappers.get(mapperpath)

        if mapper.type not in ["text"]:
            raise QueryShardException(
                f"Field [{mapperpath}] is of unsupported type [{mapper.type}] for [match bool prefix] query"
            )

        assert isinstance(mapper, core.TextMapper)

        if isinstance(body_fieldprops, str):
            lookup_values = mapper.map(body_fieldprops)
            return MatchBoolPrefixQuery(mapper, next(iter(lookup_values)))

        elif isinstance(body_fieldprops, dict):
            body_fieldprops_unrecognized = {
                k: v
                for k, v in body_fieldprops.items()
                if k
                not in [
                    "query",
                    "analyzer",
                ]
            }

            if len(body_fieldprops_unrecognized) > 0:
                first_param = list(body_fieldprops_unrecognized)[0]
                raise core.ParsingException(
                    f"query does not support [{first_param}]"
                )

            lookup_values = mapper.map(body_fieldprops["query"])

            return MatchBoolPrefixQuery(mapper, next(iter(lookup_values)))

        elif isinstance(body_fieldprops, list):
            raise core.ParsingException(
                "[match bool prefix] query does not support array of values"
            )

        else:
            raise core.ParsingException(
                "[match bool prefix] query does not support long, float, boolean"
            )


class MultiMatchQuery(LeafQuery):
    class Type(str, utils.CaseInsensitveEnum):
        BestFields = "best_fields"
        MostFields = "most_fields"
        CrossFields = "cross_fields"
        Phrase = "phrase"
        PhrasePrefix = "phrase_prefix"
        BoolPrefix = "bool_prefix"

    def __init__(self, query: Query) -> None:
        super().__init__()
        self.query = query

    def score(self, document: core.Document) -> float:
        return self.query.score(document)

    def match(self, document: core.Document) -> bool:
        return self.query.match(document)

    @classmethod
    def parse(cls, body: dict, context: core.Indice) -> MultiMatchQuery:
        body_params = {
            k: v for k, v in body.items() if k in ["query", "type", "fields"]
        }

        query = body_params.get("query", None)
        fields = body_params.get("fields", [])
        type = MultiMatchQuery.Type(
            body_params.get("type", MultiMatchQuery.Type.BestFields)
        )

        if type == MultiMatchQuery.Type.BestFields:
            return MultiMatchQuery(
                query=DisjuntionMaxQuery(
                    queries=[
                        MatchQuery.parse({field: {"query": query}}, context)
                        for field in fields
                    ]
                )
            )
        elif type == MultiMatchQuery.Type.BoolPrefix:
            return MultiMatchQuery(
                query=DisjuntionMaxQuery(
                    queries=[
                        MatchBoolPrefixQuery.parse(
                            {field: {"query": query}}, context
                        )
                        for field in fields
                    ]
                )
            )
        else:
            raise NotImplementedError("type not yet implemented", type.value)
