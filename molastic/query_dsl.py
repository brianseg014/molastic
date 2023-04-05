from __future__ import annotations

import re
import abc
import typing
import itertools

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
        if "query" in body:
            query = parse_compound_and_leaf_query(body["query"])
        else:
            query = MatchAllQuery()

        hits.extend(query.match(Context(indice)))

    return hits


def parse_compound_and_leaf_query(
    body: dict,
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
        return BooleanQuery.parse(body[query_type])

    if query_type == "term":
        return TermQuery.parse(body[query_type])

    if query_type == "prefix":
        return PrefixQuery.parse(body[query_type])

    if query_type == "range":
        return RangeQuery.parse(body[query_type])

    if query_type == "geo_distance":
        return GeodistanceQuery.parse(body[query_type])

    if query_type == "geo_shape":
        return GeoshapeQuery.parse(body[query_type])

    if query_type == "match":
        return MatchQuery.parse(body[query_type])

    if query_type == "match_bool_prefix":
        return MatchBoolPrefixQuery.parse(body[query_type])

    if query_type == "multi_match":
        return MultiMatchQuery.parse(body[query_type])

    raise Exception("unknown query type", query_type)


class QueryShardException(core.ElasticError):
    pass


class Context:
    def __init__(self, indice: core.Indice):
        self.indice = indice
        self.mappers = core.MappingsParser.parse(self.indice.mappings)

    def documents(self) -> typing.Iterable[core.Document]:
        return iter(self.indice.documents)

    def get_mapper_for_field(
        self, fieldpath: str
    ) -> typing.Optional[core.Mapper]:
        return next((m for m in self.mappers if m.can_map(fieldpath)), None)

    def get_document_index_for_field(
        self, fieldpath: str
    ) -> core.DocumentIndex:
        return self.indice.indexes

    def get_document(self, _id: str) -> core.Document:
        document = self.indice.documents.get(_id)
        if document is None:
            raise Exception("document not available")

        return document


class Query(abc.ABC):
    @abc.abstractmethod
    def match(self, context: Context) -> typing.Sequence[Hit]:
        pass

    def create_hit(self, document: core.Document, score: float) -> Hit:
        return Hit(
            _index=document["_index"]._id,
            _id=document["_id"],
            _score=score,
            _source=document["_source"],
            fields=None,
        )


class LeafQuery(Query):
    pass


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
        must: typing.Sequence[typing.Union[CompoundQuery, LeafQuery]] = [],
        filter: typing.Sequence[typing.Union[CompoundQuery, LeafQuery]] = [],
        should: typing.Sequence[typing.Union[CompoundQuery, LeafQuery]] = [],
        must_not: typing.Sequence[typing.Union[CompoundQuery, LeafQuery]] = [],
        minimum_should_match: typing.Optional[int] = None,
        boost: float = 1.0,
    ) -> None:
        self.must = must
        self.filter = filter
        self.should = should
        self.must_not = must_not
        self.minimum_should_match = minimum_should_match

    def match(self, context: Context) -> typing.Sequence[Hit]:
        hits: typing.List[Hit] = []

        must = [hit for q in self.must for hit in q.match(context)]
        filter = [hit for q in self.filter for hit in q.match(context)]
        should = [hit for q in self.should for hit in q.match(context)]
        must_not = [hit for q in self.must_not for hit in q.match(context)]

        if self.minimum_should_match is None:
            if (
                len(self.should) >= 1
                and len(self.must) == 0
                and len(self.filter) == 0
            ):
                minimum_should_match = BooleanQuery.MinimumShouldMatch(1)
            else:
                minimum_should_match = BooleanQuery.MinimumShouldMatch(0)
        else:
            minimum_should_match = BooleanQuery.MinimumShouldMatch(
                self.minimum_should_match
            )

        for hit in [
            self.create_hit(document, 0.0) for document in context.documents()
        ]:
            must_matches = [
                h for h in must if hit["_id"] == h["_id"] == hit["_id"]
            ]
            filter_matches = [
                h for h in filter if hit["_id"] == h["_id"] == hit["_id"]
            ]
            should_matches = [
                h for h in should if hit["_id"] == h["_id"] == hit["_id"]
            ]
            must_not_matches = [
                h for h in must_not if hit["_id"] == h["_id"] == hit["_id"]
            ]

            if len(self.must) != len(must_matches):
                continue

            if len(self.filter) != len(filter_matches):
                continue

            if not minimum_should_match.match(
                len(should_matches), len(self.should)
            ):
                continue

            if len(must_not_matches) > 0:
                continue

            hit["_score"] = sum(
                h["_score"] for h in must_matches + should_matches
            )

            hits.append(hit)

        return hits

    @classmethod
    def parse(self, body: dict) -> BooleanQuery:
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

        return BooleanQuery(
            must=tuple([parse_compound_and_leaf_query(q) for q in must_body]),
            filter=tuple(
                [parse_compound_and_leaf_query(q) for q in filter_body]
            ),
            should=tuple(
                [parse_compound_and_leaf_query(q) for q in should_body]
            ),
            must_not=tuple(
                [parse_compound_and_leaf_query(q) for q in must_not_body]
            ),
            minimum_should_match=body.get("minimum_should_match", None),
        )


class DisjuntionMaxQuery(CompoundQuery):
    def __init__(
        self, queries: typing.Sequence[Query], tie_breaker: float = 0.0
    ) -> None:
        self.queries = queries
        self.tie_breaker = tie_breaker

    def match(self, context: Context) -> typing.Sequence[Hit]:
        hits: typing.List[Hit] = []

        _hits = [hit for q in self.queries for hit in q.match(context)]
        _hits = sorted(_hits, key=lambda h: h["_id"])
        for _, grouped_hits in itertools.groupby(
            _hits, key=lambda h: h["_id"]
        ):
            hit = max(grouped_hits, key=lambda h: h["_score"])

            hit["_score"] = hit["_score"] + self.tie_breaker * (
                len(list(grouped_hits)) - 1
            )

            hits.append(hit)

        return hits

    @classmethod
    def parse(self, body: dict) -> DisjuntionMaxQuery:
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
                    parse_compound_and_leaf_query(q)
                    for q in body.get("queries", [])
                ]
            ),
            tie_breaker=body.get("tie_breaker", 0.0),
        )


class MatchAllQuery(LeafQuery):
    def match(self, context: Context) -> typing.Sequence[Hit]:
        return tuple([self.create_hit(d, 0.0) for d in context.documents()])


class TermQuery(LeafQuery):
    def __init__(
        self,
        fieldpath: str,
        value: str,
        boost: float = 1.0,
        case_insensitive: bool = False,
    ) -> None:
        super().__init__()
        self.fieldpath = fieldpath
        self.value = value
        self.boost = boost
        self.case_insensitive = case_insensitive

    def match(self, context: Context) -> typing.Sequence[Hit]:
        mapper = context.get_mapper_for_field(self.fieldpath)
        if mapper is None:
            return []

        value = mapper.map_value(self.value)

        matches: typing.Set[str] = set()

        index = context.get_document_index_for_field(self.fieldpath)
        for document in index:
            field = document.get(self.fieldpath)
            if field is None:
                continue

            for query_value, document_value in itertools.product(
                value, field.value
            ):
                if query_value == document_value:
                    matches.add(document._id)

        return tuple(
            [
                self.create_hit(context.get_document(_id), 1.0)
                for _id in matches
            ]
        )

    @classmethod
    def parse(cls, body: dict) -> TermQuery:
        if isinstance(body, dict):
            return cls.parse_object(body)

        raise core.ParsingException(
            "[term] query malformed, no start_object after query name"
        )

    @classmethod
    def parse_object(cls, body: dict) -> TermQuery:
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

        fieldpath, fieldprops = next(iter(body_fields.items()))

        if isinstance(fieldprops, str):
            return TermQuery(fieldpath, value=fieldprops)

        elif isinstance(fieldprops, dict):
            value = fieldprops.get("value", None)
            if value is None:
                raise core.IllegalArgumentException("value cannot be null")

            return TermQuery(fieldpath, **fieldprops)

        elif isinstance(fieldprops, list):
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
        fieldpath: str,
        value: str,
        boost: float = 1.0,
        case_insensitive: bool = False,
    ) -> None:
        super().__init__()
        self.fieldpath = fieldpath
        self.value = value
        self.boost = boost
        self.case_insensitive = case_insensitive

    def match(self, context: Context) -> typing.Sequence[Hit]:
        mapper = context.get_mapper_for_field(self.fieldpath)
        if mapper is None:
            return []

        if mapper.type not in ["keyword"]:
            raise QueryShardException(
                f"Field [{self.fieldpath}] is of unsupported type [{mapper.type}] for [prefix] query"
            )

        value = mapper.map_value(self.value)

        matches: typing.Set[str] = set()

        index = context.get_document_index_for_field(self.fieldpath)
        for document in index:
            field = document.get(self.fieldpath)
            if field is None:
                continue

            for query_value, document_value in itertools.product(
                value, field.value
            ):
                assert isinstance(query_value, core.Keyword)
                assert isinstance(document_value, core.Keyword)
                if document_value.startswith(query_value):
                    matches.add(document._id)

        return tuple(
            [
                self.create_hit(context.get_document(_id), 1.0)
                for _id in matches
            ]
        )

    @classmethod
    def parse(cls, body: dict) -> PrefixQuery:
        if isinstance(body, dict):
            return cls.parse_object(body)

        raise core.ParsingException(
            "[prefix] query malformed, no start_object after query name"
        )

    @classmethod
    def parse_object(cls, body: dict) -> PrefixQuery:
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

        fieldpath, fieldprops = next(iter(body_fields.items()))

        if isinstance(fieldprops, str):
            return PrefixQuery(fieldpath, value=fieldprops)

        elif isinstance(fieldprops, dict):
            body_value = fieldprops.get("value", None)
            if body_value is None:
                raise core.IllegalArgumentException("value cannot be null")

            return PrefixQuery(fieldpath, **fieldprops)

        elif isinstance(fieldprops, list):
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
        fieldpath: str,
        gte: typing.Optional[typing.Any] = None,
        gt: typing.Optional[typing.Any] = None,
        lt: typing.Optional[typing.Any] = None,
        lte: typing.Optional[typing.Any] = None,
        relation: Relation = Relation.INTERSECTS,
        boost: float = 1.0,
    ) -> None:
        self.fieldpath = fieldpath
        self.gte = gte
        self.gt = gt
        self.lt = lt
        self.lte = lte
        self.relation = relation
        self.boost = boost

    def match(self, context: Context) -> typing.Sequence[Hit]:
        mapper = context.get_mapper_for_field(self.fieldpath)
        if mapper is None:
            return []

        if mapper.type not in [
            "long",
            "float",
            "boolean",
            "date",
        ]:
            raise QueryShardException(
                f"Field [{self.fieldpath}] is of unsupported type [{mapper.type}] for [range] query"
            )

        if self.gte is not None:
            gte = next(v for v in mapper.map_value(self.gte))
        else:
            gte = None

        if self.gt is not None:
            gt = next(v for v in mapper.map_value(self.gt))
        else:
            gt = None

        if self.lt is not None:
            lt = next(v for v in mapper.map_value(self.lt))
        else:
            lt = None

        if self.lte is not None:
            lte = next(v for v in mapper.map_value(self.lte))
        else:
            lte = None

        matches: typing.Set[str] = set()

        index = context.get_document_index_for_field(self.fieldpath)
        for document in index:
            field = document.get(self.fieldpath)
            if field is None:
                continue

            for document_value in field.value:
                satisfied = True
                if gte is not None:
                    satisfied = satisfied and document_value >= gte  # type: ignore
                if gt is not None:
                    satisfied = satisfied and document_value > gt  # type: ignore
                if lte is not None:
                    satisfied = satisfied and document_value <= lte  # type: ignore
                if lt is not None:
                    satisfied = satisfied and document_value < lt  # type: ignore

                if satisfied:
                    matches.add(document._id)

        return tuple(
            [
                self.create_hit(context.get_document(_id), 1.0)
                for _id in matches
            ]
        )

    @classmethod
    def parse(cls, body: dict) -> RangeQuery:
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

        fieldpath, fieldprops = next(iter(body_fields.items()))

        return RangeQuery(fieldpath, **fieldprops)


class GeoshapeQuery(LeafQuery):
    class Relation(utils.CaseInsensitveEnum):
        INTERSECTS = "INTERSECTS"
        CONTAINS = "CONTAINS"

    def __init__(
        self,
        fieldpath: str,
        shape: typing.Mapping,
        relation: str = "INTERSECTS",
    ) -> None:
        super().__init__()
        self.fieldpath = fieldpath
        self.shape = shape
        self.relation = relation

    def match(self, context: Context) -> typing.Sequence[Hit]:
        mapper = context.get_mapper_for_field(self.fieldpath)
        if mapper is None:
            return []

        if mapper.type not in ["geo_shape"]:
            raise QueryShardException(
                f"Field [{self.fieldpath}] is of unsupported type [{mapper.type}] for [geoshape] query"
            )

        shape = mapper.map_value(self.shape)

        matches: typing.Set[str] = set()

        index = context.get_document_index_for_field(self.fieldpath)
        for document in index:
            field = document.get(self.fieldpath)
            if field is None:
                continue

            relation = GeoshapeQuery.Relation[self.relation]

            for query_shape, document_shape in itertools.product(
                shape, field.value
            ):
                assert isinstance(query_shape, core.Geoshape)
                assert isinstance(document_shape, core.Geoshape)

                if relation == GeoshapeQuery.Relation.INTERSECTS:
                    satisfied = document_shape.intersects(query_shape)
                elif relation == GeoshapeQuery.Relation.CONTAINS:
                    satisfied = document_shape.contains(query_shape)
                else:
                    raise NotImplementedError(
                        f"GeoshapeQuery with relation [{self.relation}]"
                    )

                if satisfied:
                    matches.add(document._id)

        return tuple(
            [
                self.create_hit(context.get_document(_id), 1.0)
                for _id in matches
            ]
        )

    @classmethod
    def parse(self, body: dict) -> GeoshapeQuery:
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

        fieldpath, fieldprops = next(iter(body_fields.items()))

        if "shape" not in fieldprops and "indexedShapeId" not in fieldprops:
            raise core.IllegalArgumentException(
                "either shape or indexShapedId is required"
            )

        if "shape" in fieldprops:
            return GeoshapeQuery(fieldpath, **fieldprops)
        elif "indexedShapeId" in fieldprops:
            raise NotImplementedError("indexedShapeId")
        else:
            raise core.ParsingException(
                "[Geoshape] query does not support inputs"
            )


class GeodistanceQuery(LeafQuery):
    def __init__(
        self,
        fieldpath: str,
        value: typing.Mapping,
        distance: str,
        distance_type: str = "arc",
    ) -> None:
        super().__init__()
        self.fieldpath = fieldpath
        self.value = value
        self.distance = distance
        self.distance_type = distance_type

    def match(self, context: Context) -> typing.Sequence[Hit]:
        mapper = context.get_mapper_for_field(self.fieldpath)
        if mapper is None:
            return []

        if mapper.type not in ["geo_point"]:
            raise QueryShardException(
                f"Field [{self.fieldpath}] is of unsupported type [{mapper.type}] for [geodistance] query"
            )

        value = mapper.map_value(self.value)
        distance = core.Geodistance.parse_single(self.distance)
        distance_type = core.Geopoint.DistanceType.of(self.distance_type)

        matches: typing.Set[str] = set()

        index = context.get_document_index_for_field(self.fieldpath)
        for document in index:
            field = document.get(self.fieldpath)
            if field is None:
                continue

            for query_geopoint, document_geopoint in itertools.product(
                value, field.value
            ):
                assert isinstance(query_geopoint, core.Geopoint)
                assert isinstance(document_geopoint, core.Geopoint)

                if distance >= document_geopoint.distance(
                    query_geopoint, distance_type
                ):
                    matches.add(document._id)

        return tuple(
            [
                self.create_hit(context.get_document(_id), 1.0)
                for _id in matches
            ]
        )

    @classmethod
    def parse(cls, body: dict) -> GeodistanceQuery:
        body_fields = {
            k: v
            for k, v in body.items()
            if k
            not in ["distance", "distance_type", "_name", "validation_method"]
        }
        fieldprops = {
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

        fieldpath, value = next(iter(body_fields.items()))

        return GeodistanceQuery(fieldpath, value, **fieldprops)


class _MatchTermQuery(LeafQuery):
    def __init__(
        self,
        fieldpath: str,
        lookup_word: str,
    ) -> None:
        super().__init__()
        self.fieldpath = fieldpath
        self.lookup_word = lookup_word

    def match(self, context: Context) -> typing.Sequence[Hit]:
        matches: typing.Set[str] = set()

        index = context.get_document_index_for_field(self.fieldpath)
        for document in index:
            field = document.get(self.fieldpath)
            if field is None:
                continue

            for document_value in field.value:
                assert isinstance(document_value, core.Text)

                if self.lookup_word in document_value:
                    matches.add(document._id)

        return tuple(
            [
                self.create_hit(context.get_document(_id), 1.0)
                for _id in matches
            ]
        )


class _MatchPrefixQuery(LeafQuery):
    def __init__(
        self,
        fieldpath: str,
        lookup_word: str,
    ) -> None:
        super().__init__()
        self.fieldpath = fieldpath
        self.lookup_word = lookup_word

    def match(self, context: Context) -> typing.Sequence[Hit]:
        matches: typing.Set[str] = set()

        index = context.get_document_index_for_field(self.fieldpath)
        for document in index:
            field = document.get(self.fieldpath)
            if field is None:
                continue

            for document_value in field.value:
                assert isinstance(document_value, core.Text)

                if any(
                    token.startswith(self.lookup_word)
                    for token in document_value
                ):
                    matches.add(document._id)

        return tuple(
            [
                self.create_hit(context.get_document(_id), 1.0)
                for _id in matches
            ]
        )


class MatchQuery(LeafQuery):
    def __init__(
        self,
        fieldpath: str,
        query: str,
    ) -> None:
        super().__init__()
        self.fieldpath = fieldpath
        self.query = query

    def match(self, context: Context) -> typing.Sequence[Hit]:
        mapper = context.get_mapper_for_field(self.fieldpath)
        if mapper is None:
            return []

        if mapper.type not in ["text"]:
            raise QueryShardException(
                f"Field [{self.fieldpath}] is of unsupported type [{mapper.type}] for [match] query"
            )

        query = next(v for v in mapper.map_value(self.query))
        assert isinstance(query, core.Text)

        self.bool_query = BooleanQuery(
            should=[_MatchTermQuery(self.fieldpath, w) for w in query]
        )

        return self.bool_query.match(context)

    @classmethod
    def parse(cls, body: dict) -> MatchQuery:
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

        fieldpath, fieldprops = next(iter(body_fields.items()))

        if isinstance(fieldprops, str):
            return MatchQuery(fieldpath, query=fieldprops)

        elif isinstance(fieldprops, dict):
            body_fieldprops_unrecognized = {
                k: v
                for k, v in fieldprops.items()
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

            return MatchQuery(fieldpath, **fieldprops)

        elif isinstance(fieldprops, list):
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
        fieldpath: str,
        query: str,
    ) -> None:
        super().__init__()
        self.fieldpath = fieldpath
        self.query = query

    def match(self, context: Context) -> typing.Sequence[Hit]:
        mapper = context.get_mapper_for_field(self.fieldpath)
        if mapper is None:
            return []

        if mapper.type not in ["text", "search_as_you_type"]:
            raise QueryShardException(
                f"Field [{self.fieldpath}] is of unsupported type [{mapper.type}] for [match bool prefix] query"
            )

        query = next(v for v in mapper.map_value(self.query))
        assert isinstance(query, core.Text)

        tokens = list(t for t in query)

        self.bool_query = BooleanQuery(
            should=[
                *[_MatchTermQuery(self.fieldpath, w) for w in tokens[:-1]],
                _MatchPrefixQuery(self.fieldpath, tokens[-1]),
            ]
        )

        return self.bool_query.match(context)

    @classmethod
    def parse(cls, body: dict) -> MatchBoolPrefixQuery:
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

        fieldpath, fieldprops = next(iter(body_fields.items()))

        if isinstance(fieldprops, str):
            return MatchBoolPrefixQuery(fieldpath, query=fieldprops)

        elif isinstance(fieldprops, dict):
            body_fieldprops_unrecognized = {
                k: v
                for k, v in fieldprops.items()
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

            return MatchBoolPrefixQuery(fieldpath, **fieldprops)

        elif isinstance(fieldprops, list):
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

    def match(self, context: Context) -> typing.Sequence[Hit]:
        return self.query.match(context)

    @classmethod
    def parse(cls, body: dict) -> MultiMatchQuery:
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
                        MatchQuery.parse({field: {"query": query}})
                        for field in fields
                    ]
                )
            )
        elif type == MultiMatchQuery.Type.BoolPrefix:
            return MultiMatchQuery(
                query=DisjuntionMaxQuery(
                    queries=[
                        MatchBoolPrefixQuery.parse({field: {"query": query}})
                        for field in fields
                    ]
                )
            )
        else:
            raise NotImplementedError("type not yet implemented", type.value)
