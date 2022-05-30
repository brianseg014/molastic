from __future__ import annotations

import re
import abc
import time
import typing

from .utils import *

from .core import *


def count(body: dict, indices: typing.Iterable[Indice]) -> CountResult:

    hits = []
    for i in indices:
        query = parse(body, i)

        hits.extend(tuple(query.process(i.documents)))

    return CountResult(
        count=len(hits),
        _shards=SearchResult.Shards(
            total=1, successful=1, skipped=0, failed=0
        ),
    )


def search(body: dict, indices: typing.Iterable[Indice]) -> SearchResult:

    hits = []
    start_time = time.time()
    for i in indices:
        query = parse(body, i)

        hits.extend(tuple(query.process(i.documents)))
    end_time = time.time()

    max_score = None
    if len(hits) > 0:
        max_score = max(h["_score"] for h in hits)

    return SearchResult(
        took=int(end_time - start_time),
        timed_out=False,
        _shards=SearchResult.Shards(
            total=1, successful=1, skipped=0, failed=0
        ),
        hits=SearchResult.Hits(
            total=SearchResult.Total(value=len(hits), relation="eq"),
            max_score=max_score,
            hits=hits,
        ),
    )


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
        raise ParsingException(
            f"[{query_type}] malformed query, expected [END_OBJECT] but found [FIELD_NAME]"
        )

    if query_type == "match_all":
        return MatchAllQuery()

    if query_type == "bool":
        return BooleanQuery.parse(body[query_type], context)

    if query_type == "term":
        return TermQuery.parse(body[query_type], context)

    if query_type == "range":
        return RangeQuery.parse(body[query_type], context)

    if query_type == "geo_distance":
        return GeodistanceQuery.parse(body[query_type], context)

    if query_type == "geo_shape":
        return GeoshapeQuery.parse(body[query_type], context)

    raise Exception("unknown query type", query_type)


@dataclasses.dataclass(frozen=True)
class SearchResult:
    class Shards(typing.TypedDict):
        total: int
        successful: int
        skipped: int
        failed: int

    class Total(typing.TypedDict):
        value: int
        relation: str

    class Hit(typing.TypedDict):
        _index: str
        _type: str
        _id: str
        _score: float
        _source: dict

    class Hits(typing.TypedDict):
        total: SearchResult.Total
        max_score: int
        hits: typing.Sequence[SearchResult.Hit]

    took: int
    timed_out: bool
    _shards: Shards
    hits: Hits


@dataclasses.dataclass(frozen=True)
class CountResult:
    class Shards(typing.TypedDict):
        total: int
        successful: int
        skipped: int
        failed: int

    count: int
    _shards: CountResult.Shards


class QueryShardException(ElasticError):
    pass


class QueryDSL:
    def __init__(self, query: SimpleQuery) -> None:
        self.query = query

    def process(
        self, documents: typing.Iterable[Document]
    ) -> typing.Iterable[SearchResult.Hit]:

        hits = list(d for d in documents if self.query.match(d))

        return (
            SearchResult.Hit(
                _index=d._index._id,
                _type=d._type,
                _id=d._id,
                _score=0.0,
                _source=d._source,
            )
            for d in hits
        )


class Query(abc.ABC):
    @abc.abstractmethod
    def score(self, document: Document) -> float:
        pass

    @abc.abstractmethod
    def match(self, document: Document) -> bool:
        pass


class SimpleQuery(Query):
    def __init__(self, query: typing.Union[CompoundQuery, LeafQuery]) -> None:
        super().__init__()
        self.query = query

    def score(self, document: Document) -> float:
        return 1.0

    def match(self, document: Document) -> bool:
        return self.query.match(document)


class CompoundQuery(Query):
    pass


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
                    self.param
                )
            )
            if interger_match is not None:
                # Fixed value
                value = int(interger_match.group("value"))
                return optional_clauses_matched >= value

            negative_integer_match = (
                BooleanQuery.MinimumShouldMatch.NEGATIVE_INTEGER_PATTERN.match(
                    self.param
                )
            )
            if negative_integer_match is not None:
                # Total minus param should be mandatory
                value = int(negative_integer_match.group("value"))
                return (
                    optional_clauses_matched
                    >= self.total_optional_cluases - value
                )

            raise NotImplementedError(
                "only integer and negative integer implemeted"
            )

    def __init__(
        self,
        must: typing.Sequence[CompoundQuery, LeafQuery],
        filter: typing.Sequence[CompoundQuery, LeafQuery],
        should: typing.Sequence[CompoundQuery, LeafQuery],
        must_not: typing.Sequence[CompoundQuery, LeafQuery],
        minimum_should_match: MinimumShouldMatch,
        boost: float = 1.0,
    ) -> None:
        self.must = must
        self.filter = filter
        self.should = should
        self.must_not = must_not
        self.minimum_should_match = minimum_should_match

    def score(self, document: Document) -> float:
        return 1.0

    def match(self, document: Document) -> bool:
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
    def parse(self, body: dict, context: Indice) -> BooleanQuery:
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
            if (
                len(must_body) == 0
                and len(filter_body) == 0
                and len(should_body) >= 1
            ):
                minimum_should_match = BooleanQuery.MinimumShouldMatch("1")
            else:
                minimum_should_match = BooleanQuery.MinimumShouldMatch("0")

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


class LeafQuery(Query):
    pass


class MatchAllQuery(LeafQuery):
    def score(self, document: Document) -> float:
        return 0.0

    def match(self, _: Document) -> bool:
        return True


class TermQuery(LeafQuery):
    def __init__(
        self,
        fieldname: str,
        fieldmapping: Type,
        lookup_value: Value,
        boost: float = 1.0,
        case_insensitive: bool = False,
    ) -> None:
        super().__init__()
        self.fieldname = fieldname
        self.fieldmapping = fieldmapping
        self.lookup_value = lookup_value
        self.boost = boost
        self.case_insensitive = case_insensitive

    def score(self, document: Document) -> float:
        return 1.0

    def match(self, document: Document) -> bool:
        stored_body = read_from_document(self.fieldname, document, MISSING)
        if stored_body is MISSING:
            return False

        for stored_value in map_json_to_value(stored_body, self.fieldmapping):
            if stored_value == self.lookup_value:
                return True
        
        return False

    @classmethod
    def parse(cls, body: dict, context: Indice) -> TermQuery:
        if isinstance(body, dict):
            return cls.parse_object(body, context)

        raise ParsingException(
            "[term] query malformed, no start_object after query name"
        )

    @classmethod
    def parse_object(cls, body: dict, context: Indice) -> TermQuery:
        body_fields = {k: v for k, v in body.items()}

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise ParsingException(
                "[term] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_fields) == 0:
            raise IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        fieldname, body_fieldprops = next(iter(body_fields.items()))

        fieldmapping = context.mappings.get(fieldname)

        if isinstance(body_fieldprops, str):
            lookup_value = map_json_to_value(body_fieldprops, fieldmapping)[0]
            return TermQuery(fieldname, fieldmapping, lookup_value)

        elif isinstance(body_fieldprops, dict):
            body_value = body_fieldprops.get("value", None)
            if body_value is None:
                raise IllegalArgumentException("value cannot be null")

            lookup_value = map_json_to_value(body_value, fieldmapping)[0]
            return TermQuery(fieldname, fieldmapping, lookup_value)

        elif isinstance(body_fieldprops, list):
            raise ParsingException(
                "[term] query does not support array of values"
            )


class RangeQuery(LeafQuery):
    class Relation(CaseInsensitveEnum):
        INTERSECTS = "INTERSECTS"
        CONTAINS = "CONTAINS"
        WITHIN = "WITHIN"

    def __init__(
        self,
        fieldname: str,
        fieldmapping: Type,
        gte: typing.Optional[Value] = None,
        gt: typing.Optional[Value] = None,
        lt: typing.Optional[Value] = None,
        lte: typing.Optional[Value] = None,
        relation: Relation = Relation.INTERSECTS,
        boost: float = 1.0,
    ) -> None:
        self.fieldname = fieldname
        self.fieldmapping = fieldmapping
        self.gte = gte
        self.gt = gt
        self.lt = lt
        self.lte = lte
        self.relation = relation
        self.boost = boost

    def score(self, document: Document) -> float:
        return 1.0

    def match(self, document: Document) -> bool:
        stored_body = read_from_document(self.fieldname, document, MISSING)
        if stored_body is MISSING:
            return False

        for stored_value in map_json_to_value(stored_body, self.fieldmapping):

            satisfied = True
            if self.gte is not None:
                satisfied = satisfied and stored_value >= self.gte
            if self.gt is not None:
                satisfied = satisfied and stored_value > self.gt
            if self.lte is not None:
                satisfied = satisfied and stored_value <= self.lte
            if self.lt is not None:
                satisfied = satisfied and stored_value < self.lt

            if satisfied:
                return True

        return False

    @classmethod
    def parse(cls, body: dict, context: Indice) -> RangeQuery:
        body_fields = {k: v for k, v in body.items() if isinstance(v, dict)}
        body_params = {
            k: v for k, v in body.items() if not isinstance(v, dict)
        }

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise ParsingException(
                "[range] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_params) > 1:
            first_param = list(body_params)[0]
            raise ParsingException(f"query does not support [{first_param}]")

        if len(body_fields) == 0:
            raise IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        fieldname, body_fieldprops = next(iter(body_fields.items()))

        fieldmapping = context.mappings.get(fieldname)
        fieldmapping_type = fieldmapping["type"]
        if fieldmapping_type not in [
            "long",
            "float",
            "boolean",
            "date",
        ]:
            raise QueryShardException(
                f"Field [{fieldname}] is of unsupported type [{fieldmapping_type}] for [range] query"
            )

        body_gte = body_fieldprops.get("gte", None)
        body_gt = body_fieldprops.get("gt", None)
        body_lte = body_fieldprops.get("lte", None)
        body_lt = body_fieldprops.get("lt", None)

        gte, gt, lt, lte = None, None, None, None
        if fieldmapping_type == "date":
            body_format = body_fieldprops.get("format", fieldmapping["format"])
            if isinstance(body_gte, str):
                if Date.match_date_math_pattern(body_gte):
                    gte = Date.parse_date_math(body_gte)
                else:
                    gte = Date.parse_single(body_gte, body_format)
            if isinstance(body_gt, str):
                if Date.match_date_math_pattern(body_gt):
                    gt = Date.parse_date_math(body_gt)
                else:
                    gt = Date.parse_single(body_gt, body_format)
            if isinstance(body_lte, str):
                if Date.match_date_math_pattern(body_lte):
                    lte = Date.parse_date_math(body_lte)
                else:
                    lte = Date.parse_single(lte, body_format)
            if isinstance(body_lt, str):
                if Date.match_date_math_pattern(body_lt):
                    lt = Date.parse_date_math(body_lt)
                else:
                    lt = Date.parse_single(body_lt, body_format)

        else: # No date

            parser = None
            if fieldmapping_type == "long":
                parser = Long.parse_single
            elif fieldmapping_type == "float":
                parser = Float.parse_single
            else:
                raise NotImplementedError(fieldmapping_type)

            if body_gte is not None and gte is None:
                gte = parser(body_gte)
            if body_gt is not None and gt is None:
                gt = parser(body_gt)
            if body_lte is not None and lte is None:
                lt = parser(body_lt)
            if body_lt is not None and lt is None:
                lte = parser(body_lte)

        return RangeQuery(fieldname, fieldmapping, gte, gt, lt, lte)


class GeoshapeQuery(LeafQuery):
    class Relation(CaseInsensitveEnum):
        INTERSECTS = "INTERSECTS"
        CONTAINS = "CONTAINS"

    def __init__(
        self,
        fieldname: str,
        fieldmapping: Type,
        shape: Geoshape,
        relation: Relation,
    ) -> None:
        super().__init__()
        self.fieldname = fieldname
        self.fieldmapping = fieldmapping
        self.shape = shape
        self.relation = relation

    def score(self, document: Document) -> float:
        return 1.0

    def match(self, document: Document) -> bool:
        stored_body = read_from_document(self.fieldname, document, MISSING)
        if stored_body is MISSING:
            return False

        for stored_value in map_json_to_value(stored_body, self.fieldmapping):
            stored_value = typing.cast(Geoshape, stored_value)

            if self.relation == GeoshapeQuery.Relation.INTERSECTS:
                satisfied = stored_value.intersects(self.shape)
            elif self.relation == GeoshapeQuery.Relation.CONTAINS:
                satisfied = stored_value.contains(self.shape)
            else:
                raise NotImplementedError(f"GeoshapeQuery with relation [{self.relation}]")

            if satisfied:
                return True

        return False

    @classmethod
    def parse(self, body: dict, context: Indice) -> GeoshapeQuery:
        body_fields = {k: v for k, v in body.items() if isinstance(v, dict)}
        body_params = {
            k: v for k, v in body.items() if not isinstance(v, dict)
        }

        if len(body_fields) > 1:
            field1, field2 = list(body_fields)[0:2]
            raise ParsingException(
                "[range] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_params) > 0:
            first_param = list(body_params)[0]
            raise ParsingException(f"query does not support [{first_param}]")

        if len(body_fields) == 0:
            raise IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        fieldname, body_fieldprops = next(iter(body_fields.items()))

        fieldmapping = context.mappings.get(fieldname)
        fieldmapping_type = fieldmapping["type"]
        if fieldmapping_type not in ["geo_shape"]:
            raise QueryShardException(
                f"Field [{fieldname}] is of unsupported type [{fieldmapping_type}] for [geo_shape] query"
            )

        if (
            "shape" not in body_fieldprops
            and "indexedShapeId" not in body_fieldprops
        ):
            raise IllegalArgumentException(
                "either shape or indexShapedId is required"
            )

        if "shape" in body_fieldprops:
            body_relation = body_fieldprops.get(
                "relation", GeoshapeQuery.Relation.INTERSECTS.value
            )

            shape = Geoshape.parse_single(body_fieldprops["shape"])
            relation = GeoshapeQuery.Relation(body_relation)

            return GeoshapeQuery(fieldname, fieldmapping, shape, relation)
        elif "indexedShapeId" in body_fieldprops:
            raise NotImplementedError("indexedShapeId")


class GeodistanceQuery(LeafQuery):
    def __init__(
        self,
        fieldname: str,
        fieldmapping: Type,
        distance: Geodistance,
        value: typing.Union[Geopoint, Geoshape],
        distance_type: Geopoint.DistanceType,
    ) -> None:
        super().__init__()
        self.fieldname = fieldname
        self.fieldmapping = fieldmapping
        self.distance = distance
        self.value = value
        self.distance_type = distance_type

    def score(self, document: Document) -> float:
        return 1.0

    def match(self, document: Document) -> bool:
        stored_body = read_from_document(self.fieldname, document, MISSING)
        if stored_body is MISSING:
            return False

        for stored_value in Geopoint.parse(stored_body):
            distance = stored_value.distance(self.value, self.distance_type)
            if distance <= self.distance:
                return True

        return False

    @classmethod
    def parse(cls, body: dict, context: Indice) -> GeodistanceQuery:
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
            raise ParsingException(
                "[range] query doesn't support multiple fields, "
                f"found [{field1}] and [{field2}]"
            )

        if len(body_fields) == 0:
            raise IllegalArgumentException(
                "fieldName must not be null or empty"
            )

        fieldname, body_fieldprops = next(iter(body_fields.items()))

        fieldmapping = context.mappings.get(fieldname)
        fieldmapping_type = fieldmapping["type"]
        if fieldmapping_type not in ["geo_point"]:
            raise QueryShardException(
                f"Field [{fieldname}] is of unsupported type [{fieldmapping_type}] for [geo_shape] query"
            )

        body_distance = body_params["distance"]
        body_distance_type = body_params.get(
            "distance_type", Geopoint.DistanceType.ARC.value
        )

        distance = Geodistance.parse_single(body_distance)
        distance_type = Geopoint.DistanceType[body_distance_type]
        point = Geopoint.parse_single(body_fieldprops)

        return GeodistanceQuery(
            fieldname, fieldmapping, distance, point, distance_type
        )
