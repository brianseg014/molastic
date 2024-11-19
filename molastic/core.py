from __future__ import annotations

import re
import sys
import json
import uuid
import abc
import enum
import copy
import typing
import decimal
import datetime
import dateutil.relativedelta
import collections.abc
import itertools
import shapely.geometry
import shapely.wkt
import haversine
import pygeohash


from . import analysis
from . import painless
from . import java_json
from . import utils


class MISSING:
    pass


class ElasticError(Exception):
    pass


class ResourceAlreadyExistsException(ElasticError):
    pass


class InvalidIndexNameException(ElasticError):
    pass


class IndexNotFoundException(ElasticError):
    pass


class StrictDynamicMappingException(ElasticError):
    pass


class MapperParsingException(ElasticError):
    pass


class IllegalArgumentException(ElasticError):
    pass


class DateTimeParseException(ElasticError):
    pass


class ParsingException(ElasticError):
    pass


class NumberFormatException(ElasticError):
    pass


class ScriptException(ElasticError):
    pass


class DocumentMissingException(ElasticError):
    def __init__(self, type: str, id: str) -> None:
        super().__init__(f"[{type}][{id}]: document missing")


class Tier(enum.Enum):
    DATA_HOT = "DATA_HOT"
    DATA_WARM = "DATA_WARM"
    DATA_COLD = "DATA_COLD"
    DATA_FROZEN = "DATA_FROZEN"


class OperationType(utils.CaseInsensitveEnum):
    INDEX = "INDEX"
    CREATE = "CREATE"


class Refresh(utils.CaseInsensitveEnum):
    TRUE = True
    FALSE = False
    WAIT_FOR = "WAIT_FOR"


class VersionType(utils.CaseInsensitveEnum):
    EXTERNAL = "EXTERNAL"
    EXTERNAL_GTE = "EXTERNAL_GTE"


class OperationResult(utils.CaseInsensitveEnum):
    NOOP = "noop"
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    NOT_FOUND = "not_found"


class Document(typing.TypedDict):
    _index: Indice
    _id: str
    _type: str
    _source: dict
    _size: int
    _doc_count: typing.Optional[int]
    _field_names: typing.Sequence[str]
    _ignored: typing.Sequence[str]
    _routing: str
    _meta: dict
    _tier: str
    _seq_no: int
    _primary_term: int
    _version: int
    _stored_fields: dict


class DocumentList:
    @classmethod
    def of(cls, documents: typing.Sequence[Document]) -> DocumentList:
        return DocumentList(documents)

    @classmethod
    def empty(cls) -> DocumentList:
        return DocumentList.of([])

    def __init__(self, documents: typing.Sequence[Document]) -> None:
        self.documents = documents

    def __iter__(self):
        return iter(self.documents)

    def has(self, _id: str) -> bool:
        return any(d["_id"] == _id for d in self.documents)

    def get(self, _id: str) -> typing.Optional[Document]:
        return next((d for d in self.documents if d["_id"] == _id), None)

    def upsert(self, document: Document) -> DocumentList:
        documents = [d for d in self.documents if d["_id"] != document["_id"]]

        return DocumentList.of(documents + [document])

    def remove(self, _id: str) -> DocumentList:
        return DocumentList.of([d for d in self.documents if d["_id"] != _id])


class ElasticEngine:
    def __init__(self) -> None:
        self._resources: typing.Dict[str, Indice] = {}

    def create_indice(
        self,
        _id: IndiceName,
        aliases: typing.Optional[typing.List[str]] = None,
        mappings: typing.Optional[typing.Mapping] = None,
        settings: typing.Optional[typing.Mapping] = None,
    ):
        if _id in self._resources:
            raise ResourceAlreadyExistsException(
                f"index [{_id}] already exists"
            )
        self._resources[_id] = Indice(_id, aliases, mappings, settings)

    def delete_indice(self, _id: IndiceName):
        if not self.exists(_id):
            raise IndexNotFoundException(f"No such index [{_id}]")
        del self._resources[_id]

    def indice(self, _id: IndiceName, autocreate: bool = False) -> Indice:
        if not self.exists(_id) and autocreate:
            self.create_indice(_id)

        try:
            return self._resources[_id]
        except KeyError:
            raise IndexNotFoundException(f"No such index [{_id}]")

    def resources(self, target: str) -> typing.Sequence[Indice]:
        return tuple(v for k, v in self._resources.items() if k == target)

    def exists(self, _id: str) -> bool:
        return _id in self._resources


class IndiceName(str):
    @classmethod
    def parse(cls, name) -> IndiceName:
        if name is None:
            raise InvalidIndexNameException("index name cannot be empty")

        if isinstance(name, str):
            return cls.parse_string(name)

        raise InvalidIndexNameException()

    @classmethod
    def parse_string(cls, name: str) -> IndiceName:
        if any(c.isalpha() and c == c.upper() for c in name):
            raise InvalidIndexNameException(
                f"Invalid index name [{name}], must be lowercase"
            )

        if any(c in ' "*\\<|,>/?' for c in name):
            raise InvalidIndexNameException(
                f"Invalid index name [{name}], must not contain "
                'the following characters [ , ", *, \\, <, |, ,, >, /, ?]'
            )

        if ":" in name:
            raise InvalidIndexNameException(
                f"Invalid index name [{name}], must not contain [:]"
            )

        if any(name.startswith(c) for c in "-_+"):
            raise InvalidIndexNameException(
                f"Invalid index name [{name}], must not start "
                "with '_', '-' or '+'"
            )

        return IndiceName(name)


class Indice:
    def __init__(
        self,
        _id: IndiceName,
        aliases: typing.Optional[typing.List[str]] = None,
        mappings: typing.Optional[typing.Mapping] = None,
        settings: typing.Optional[typing.Mapping] = None,
    ) -> None:
        self._id = _id

        self.sequence = itertools.count()
        self.aliases: typing.Sequence[str] = []
        self.mappings: typing.Mapping = {"properties": {}}
        self.settings: typing.Mapping = {
            "index": {
                "creation_date": datetime.datetime.now().timestamp(),
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "uuid": uuid.uuid4().hex,
                "version": {"created": "135217827"},
                "provided_name": self._id,
            }
        }

        self.documents: DocumentList = DocumentList.empty()

        self.indexes: DocumentIndex = DocumentIndex.empty()

        if mappings:
            self.update_mappings(mappings)

    def update_mappings(self, mappings: typing.Mapping):
        mappings = MappingsMerger.merge(
            mapping1=self.mappings, mapping2=mappings
        )
        MappingsParser.parse(mappings)
        self.mappings = mappings

    def index(
        self,
        body: dict,
        id: typing.Optional[str] = None,
        if_seq_no: typing.Optional[int] = None,
        if_primary_term: typing.Optional[int] = None,
        op_type: OperationType = OperationType.INDEX,
        pipeline: typing.Optional[str] = None,
        refresh: Refresh = Refresh.FALSE,
        routing: typing.Optional[str] = None,
        timeout: typing.Optional[str] = None,
        version: typing.Optional[int] = None,
        version_type: typing.Optional[VersionType] = None,
        wait_for_active_shards: str = "1",
        require_alias: bool = False,
    ) -> typing.Tuple[Document, OperationResult]:

        if id is None:
            id = self.create_document_id()

        exists = self.exists(id)
        if exists and op_type == OperationType.CREATE:
            raise ElasticError("document already exists")

        _version: int = 1
        _stored_document = self.get(id)

        if _stored_document is not None:
            _version = _stored_document["_version"] + 1

        _source = body

        _document = Document(
            _index=self,
            _id=id,
            _type="_doc",
            _source=_source,
            _size=sys.getsizeof(_source),
            _doc_count=1,
            _field_names=(),
            _ignored=(),
            _routing=id,
            _meta={},
            _tier=Tier.DATA_HOT.value,
            _seq_no=next(self.sequence),
            _primary_term=1,
            _version=_version,
            _stored_fields={},
        )

        # Update index mappings by dynamic mapping
        dynamic_mapping = DynamicMapping()
        mappings = dynamic_mapping.map_source(_document["_source"])

        self.mappings = MappingsMerger.merge(
            mapping1=self.mappings, mapping2=mappings, dynamic=True
        )

        # Add to general documents
        self.documents = self.documents.upsert(_document)

        # Rebuild indexes
        self.indexes = DocumentIndex.create(self.documents, self.mappings)

        if not exists:
            operation_result = OperationResult.CREATED
        else:
            operation_result = OperationResult.UPDATED

        return _document, operation_result

    def get(self, _id: str) -> typing.Optional[Document]:
        return self.documents.get(_id)

    def delete(
        self,
        id: str,
        if_seq_no: typing.Optional[int] = None,
        if_primary_term: typing.Optional[int] = None,
        refresh: Refresh = Refresh.FALSE,
        routing: typing.Optional[str] = None,
        version: typing.Optional[int] = None,
        version_type: typing.Optional[VersionType] = None,
        wait_for_active_shards: str = "1",
    ) -> typing.Tuple[typing.Optional[Document], OperationResult]:
        if not self.documents.has(id):
            return None, OperationResult.NOT_FOUND

        _stored_document = self.documents.get(id)

        # Remove from general documents
        self.documents = self.documents.remove(id)

        # Rebuild indexes
        self.indexes = DocumentIndex.create(self.documents, self.mappings)

        return _stored_document, OperationResult.DELETED

    def update(
        self,
        body: dict,
        id: str,
        if_seq_no: typing.Optional[int] = None,
        if_primary_term: typing.Optional[int] = None,
        lang: str = "PainlessLang",
        require_alias: bool = False,
        refresh: Refresh = Refresh.FALSE,
        retry_on_conflict: int = 0,
        routing: typing.Optional[str] = None,
        source: typing.Union[bool, list] = True,
        source_excludes: typing.Sequence[str] = (),
        source_includes: typing.Sequence[str] = (),
        timeout: typing.Optional[str] = None,
        wait_for_active_shards: str = "1",
    ) -> typing.Tuple[Document, OperationResult]:

        _version: int = 1

        _stored_document = self.documents.get(id)

        exists = False
        if _stored_document is not None:
            exists = True

        if _stored_document is not None:
            _doc_base = _stored_document["_source"]
            _version = _stored_document["_version"] + 1
        elif body.get("doc_as_upsert", False):
            _doc_base = body["doc"]
        elif body.get("upsert", None) is not None:
            _doc_base = body["upsert"]
        else:
            raise DocumentMissingException("_doc", id)

        _doc_base_copy = copy.deepcopy(_doc_base)
        if "script" in body:
            _source = _doc_base_copy

            scripting = Scripting.parse(body["script"])

            ctx = typing.cast(dict, scripting.dumps({"_source": _source}))

            scripting.execute({"ctx": ctx})

            _source = scripting.loads(ctx)["_source"]

        elif "doc" in body:
            _source = utils.source_merger.merge(_doc_base_copy, body["doc"])
        
        else:
            raise TypeError('Either script or doc is required')

        _document = Document(
            _index=self,
            _id=id,
            _type="_doc",
            _source=_source,
            _size=sys.getsizeof(_source),
            _doc_count=1,
            _field_names=(),
            _ignored=(),
            _routing=id,
            _meta={},
            _tier=Tier.DATA_HOT.value,
            _seq_no=next(self.sequence),
            _primary_term=1,
            _version=_version,
            _stored_fields={},
        )

        # Update index mappings by dynamic mapping
        dynamic_mapping = DynamicMapping()
        mappings = dynamic_mapping.map_source(_document["_source"])

        self.mappings = MappingsMerger.merge(
            mapping1=self.mappings, mapping2=mappings, dynamic=True
        )

        # Add to general documents
        self.documents = self.documents.upsert(_document)

        # Rebuild indexes
        self.indexes = DocumentIndex.create(self.documents, self.mappings)

        if not exists:
            operation_result = OperationResult.CREATED
        else:
            operation_result = OperationResult.UPDATED

        return _document, operation_result

    def multi_get(self):
        raise NotImplementedError()

    def bulk(self):
        raise NotImplementedError()

    def delete_by_query(self):
        raise NotImplementedError()

    def update_by_query(self):
        raise NotImplementedError()

    def exists(self, _id: str) -> bool:
        return self.documents.has(_id)

    def create_document_id(self) -> str:
        return str(uuid.uuid4())


class DocumentIndex:
    """
    General index purpose. Consists of a flatten document version of
    fieldpath,[value] paisrs.

    For example:
        Document:
            source:
                {
                    "field1": "value",
                    "field2: {
                        "field3": "value"
                    }
                }

        Index:
            {
                "field1": [Keyword("value")],
                "field2.field3": [Keyword("value")]
            }

    """

    class FieldIndexed:
        def __init__(
            self, fieldpath: str, value: typing.Sequence[Value]
        ) -> None:
            self.fieldpath = fieldpath
            self.value = value

    class DocumentIndexed:
        def __init__(
            self, _id: str, fields: typing.Sequence[DocumentIndex.FieldIndexed]
        ) -> None:
            self._id = _id
            self.fields = fields

        def get(
            self, fieldpath: str
        ) -> typing.Optional[DocumentIndex.FieldIndexed]:
            return next(
                (f for f in self.fields if f.fieldpath == fieldpath), None
            )

    def __init__(
        self, indexes: typing.Sequence[DocumentIndex.DocumentIndexed]
    ) -> None:
        # [doc_id][targetpath] = [value]
        self.indexes = indexes

    def __iter__(self):
        return iter(self.indexes)

    @classmethod
    def empty(cls) -> DocumentIndex:
        return DocumentIndex([])

    @classmethod
    def create(
        cls, documents: typing.Iterable[Document], mappings: typing.Mapping
    ) -> DocumentIndex:
        mappers = MappingsParser.parse(mappings)

        indexes: typing.List[DocumentIndex.DocumentIndexed] = []
        for document in documents:
            indexes.append(
                DocumentIndex.DocumentIndexed(
                    _id=document["_id"],
                    fields=[
                        DocumentIndex.FieldIndexed(fieldpath, list(value))
                        for mapper in mappers
                        for fieldpath, value in mapper.map_document(
                            document
                        ).items()
                    ],
                )
            )

        return DocumentIndex(indexes)


class Mapper(abc.ABC):
    """
    A mapper transforms a document segment into a flatten dict
    with targetpath,[value] pairs.
    """

    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        fields: typing.Optional[typing.Mapping],
    ) -> None:
        self.sourcepath = sourcepath
        self.targetpath = targetpath
        self.type = type
        self.fields = fields

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(sourcepath='{self.sourcepath}', targetpath='{self.targetpath}')"

    def can_map(self, fieldpath: str) -> bool:
        return self.targetpath == fieldpath

    @abc.abstractmethod
    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        pass

    @abc.abstractmethod
    def map_value(self, body) -> typing.Iterable[Value]:
        pass


class KeywordMapper(Mapper):
    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        ignore_above: int = 2147483647,
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)
        self.ignore_above = ignore_above

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        return Keyword.parse(body)


class BooleanMapper(Mapper):
    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        return Boolean.parse(body)


class FloatMapper(Mapper):
    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        return Float.parse(body)


class DoubleMapper(Mapper):
    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        return Double.parse(body)


class LongMapper(Mapper):
    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        return Long.parse(body)


class DateMapper(Mapper):
    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        format: str = "strict_date_optional_time||epoch_millis",
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)
        self.format = format

    def __repr__(self):
        return f"DateMapper(format={repr(self.format)})"

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        if isinstance(body, str) and Date.match_date_math_pattern(body):
            return [Date.parse_date_math(body)]
        else:
            return Date.parse(body, self.format)


class TextMapper(Mapper):
    default_analyzer = analysis.StandardAnalyzer()

    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        analyzer: str = "standard",
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)
        self.analyzer = analyzer

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        return Text.parse(body, self.default_analyzer)


class SearchAsYouTypeMapper(Mapper):
    default_analyzer = analysis.StandardAnalyzer()

    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        analyzer: str = "standard",
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)
        self.analyzer = analyzer

    def can_map(self, fieldpath: str) -> bool:
        return fieldpath in [
            self.targetpath,
            self.targetpath + "._2gram",
            self.targetpath + "._3gram",
            self.targetpath + "._index_prefix",
        ]

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {
            self.targetpath: self.map_value(body),
            self.targetpath + "._2gram": self.map_value(body),
            self.targetpath + "._3gram": self.map_value(body),
            self.targetpath + "._index_prefix": self.map_value(body),
        }

    def map_value(self, body) -> typing.Iterable[Value]:
        return Text.parse(body, self.default_analyzer)


class GeopointMapper(Mapper):
    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        return Geopoint.parse(body)


class GeoshapeMapper(Mapper):
    def __init__(
        self,
        sourcepath: str,
        targetpath: str,
        type: str,
        fields: typing.Optional[typing.Mapping] = None,
    ) -> None:
        super().__init__(sourcepath, targetpath, type, fields)

    def map_document(
        self, document: Document
    ) -> typing.Dict[str, typing.Iterable[Value]]:
        body = read_from_document(self.sourcepath, document, None)
        if body is None:
            return {}

        return {self.targetpath: self.map_value(body)}

    def map_value(self, body) -> typing.Iterable[Value]:
        return Geoshape.parse(body)


class DynamicMapping:
    class Dynamic(utils.CaseInsensitveEnum):
        true = "true"
        runtime = "runtime"
        false = "false"
        strict = "strict"

    def __init__(
        self,
        dynamic: Dynamic = Dynamic.true,
        date_detection: bool = True,
        dynamic_date_formats: typing.Sequence[str] = [
            "strict_date_optional_time",
            "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd",
        ],
        numeric_detection: bool = True,
    ) -> None:
        self.dynamic = dynamic
        self.date_detection = date_detection
        self.dynamic_date_formats = dynamic_date_formats
        self.numeric_detection = numeric_detection

    def map_source(self, source: dict) -> typing.Mapping:
        dicttree: typing.Callable[
            [], typing.DefaultDict
        ] = lambda: collections.defaultdict(dicttree)

        mappings = dicttree()

        allowed_prefixes: typing.List[str] = []

        for path, body in utils.flatten(source):
            if path.count(".") > 0:
                # Child node
                if not any(path.startswith(f) for f in allowed_prefixes):
                    # If not child node of ObjectMapper, ignore
                    continue

            segments = utils.intersperse(path.split("."), "properties")

            if self.dynamic == DynamicMapping.Dynamic.strict:
                parent_segment = segments[-2] if len(segments) > 1 else "_doc"
                raise StrictDynamicMappingException(
                    f"mapping set to strict, dynamic introduction of [{path}] within [{parent_segment}] is not allowed"
                )

            mapping_v = self.map_value(body)
            if mapping_v is None:
                continue

            type = mapping_v.get("type", "object")
            if type == "object":
                # Let visit object child nodes
                allowed_prefixes.append(path)
                continue

            fragment = utils.get_from_mapping(segments[:-1], mappings)
            assert fragment is not None
            fragment[segments[-1]] = mapping_v

        return mappings

    def map_value(self, value) -> typing.Optional[typing.Mapping]:
        """Value in the document to infer data type.
        Returns None if the field should not be mapped.
        """
        if self.dynamic == DynamicMapping.Dynamic.false:
            return None

        if self.dynamic == DynamicMapping.Dynamic.strict:
            return None

        while utils.is_array(value):
            value = typing.cast(typing.Sequence, value)
            if len(value) == 0:
                value = None
            else:
                value = value[0]

        if value is None:
            return None

        if isinstance(value, bool):
            return {"type": "boolean"}
        elif isinstance(value, float):
            if self.dynamic == DynamicMapping.Dynamic.true:
                return {"type": "float"}
            elif self.dynamic == DynamicMapping.Dynamic.runtime:
                return {"type": "double"}
            else:
                raise Exception("should not be here, report error")
        elif isinstance(value, int):
            return {"type": "long"}
        elif isinstance(value, str):
            if self.numeric_detection and Long.match_pattern(value):
                return {"type": "long"}
            elif (
                self.numeric_detection
                and self.dynamic == DynamicMapping.Dynamic.true
                and Float.match_pattern(value)
            ):
                return {"type": "float"}
            elif (
                self.numeric_detection
                and self.dynamic == DynamicMapping.Dynamic.runtime
                and Double.match_pattern(value)
            ):
                return {"type": "boolean"}
            elif self.date_detection and Date.match_date_format(
                value, "||".join(self.dynamic_date_formats)
            ):
                return {"type": "date"}
            else:
                if self.dynamic == DynamicMapping.Dynamic.true:
                    return {"type": "text"}
                elif self.dynamic == DynamicMapping.Dynamic.runtime:
                    return {"type": "keyword"}
                else:
                    raise Exception("should not be here, report error")
        elif isinstance(value, dict):
            if self.dynamic == DynamicMapping.Dynamic.true:
                return {"type": "object", "properties": {}}
            else:
                return None
        else:
            raise NotImplementedError(type(value), value)


class MappingsMerger:
    @classmethod
    def merge(
        cls,
        mapping1: typing.Mapping,
        mapping2: typing.Mapping,
        dynamic: bool = False,
    ) -> typing.Mapping:
        dicttree: typing.Callable[
            [], typing.DefaultDict
        ] = lambda: collections.defaultdict(dicttree)

        mappings = dicttree()

        mapping1_flatten = dict(utils.flatten(mapping1))
        mapping2_flatten = dict(utils.flatten(mapping2))

        patterns = [re.compile("^properties.\\w+$")]

        for mapping_k in list(mapping1_flatten.keys()) + list(
            mapping2_flatten.keys()
        ):
            if not any(p.match(mapping_k) for p in patterns):
                continue

            segments = mapping_k.split(".")

            if mapping_k in mapping1_flatten and mapping_k in mapping2_flatten:
                mapping1_type = mapping1_flatten[mapping_k].get(
                    "type", "object"
                )
                mapping2_type = mapping2_flatten[mapping_k].get(
                    "type", "object"
                )
                if mapping1_type != mapping2_type:
                    raise IllegalArgumentException(
                        f"mapper [{segments[-1]}] cannot be changed from "
                        f"type [{mapping1_type}] to [{mapping2_type}]"
                    )

            if mapping_k in mapping1_flatten:
                type = mapping1_flatten[mapping_k].get("type", "object")

            if mapping_k in mapping2_flatten:
                type = mapping2_flatten[mapping_k].get("type", "object")

            if type == "object":
                # Let visit object child nodes
                patterns.append(re.compile(f"^{mapping_k}.properties.\\w+$"))
                continue

            if dynamic:
                mapping_v = utils.mapping_dynamic_merger.merge(
                    copy.deepcopy(mapping1_flatten.get(mapping_k, {})),
                    copy.deepcopy(mapping2_flatten.get(mapping_k, {})),
                )
            else:
                mapping_v = utils.mapping_merger.merge(
                    copy.deepcopy(mapping1_flatten.get(mapping_k, {})),
                    copy.deepcopy(mapping2_flatten.get(mapping_k, {})),
                )

            fragment = utils.get_from_mapping(segments[:-1], mappings)
            assert fragment is not None
            fragment[segments[-1]] = mapping_v

        return mappings


class MappingsParser:

    MAPPERS: typing.Mapping[str, typing.Type[Mapper]] = {
        "keyword": KeywordMapper,
        "boolean": BooleanMapper,
        "long": LongMapper,
        "float": FloatMapper,
        "double": DoubleMapper,
        "date": DateMapper,
        "text": TextMapper,
        "search_as_you_type": SearchAsYouTypeMapper,
        "geo_point": GeopointMapper,
        "geo_shape": GeoshapeMapper,
    }

    @classmethod
    def parse(cls, mappings: typing.Mapping) -> typing.Sequence[Mapper]:
        if any(True for k in mappings.keys() if k != "properties"):
            raise MapperParsingException(
                f"Root mapping definition has unsupported parameters: {mappings}"
            )

        patterns = [re.compile("^properties.\\w+$")]

        mappers: typing.List[Mapper] = []
        for mappingpath, mapping in utils.flatten(mappings):
            if not any(p.match(mappingpath) for p in patterns):
                continue

            segments = mappingpath.split(".")[1::2]
            if len(segments) == 0:
                continue

            # FIELD

            if not isinstance(mapping, collections.abc.Mapping):
                raise MapperParsingException(
                    f"Expected map for property [properties] "
                    f"on field [{segments[-1]}] but got {mapping}"
                )

            sourcepath = ".".join(segments)
            type = mapping.get("type", "object")

            if type == "object":
                # Let visit object child nodes
                patterns.append(re.compile(f"^{mappingpath}.properties.\\w+$"))
                continue

            try:
                clstype = MappingsParser.MAPPERS[type]
            except KeyError:
                raise MapperParsingException(
                    f"No handler for type [{type}] declared on field [{segments[-1]}]"
                )

            if "type" not in mapping:
                mappers.append(clstype(sourcepath, type="object", **mapping))
            else:
                mappers.append(clstype(sourcepath, sourcepath, **mapping))

            # SUBFIELDS
            for subfield, mapping in mapping.get("fields", {}).items():
                if not isinstance(mapping, collections.abc.Mapping):
                    raise MapperParsingException(
                        f"Expected map for property [properties] "
                        f"on field [{subfield}] but got {mapping}"
                    )

                type = mapping.get("type", "object")
                if type == "object":
                    raise MapperParsingException(
                        f"Unsupported type [{type}] declared on field [{subfield}]"
                    )

                try:
                    clstype = MappingsParser.MAPPERS[type]
                except KeyError:
                    raise MapperParsingException(
                        f"No handler for type [{type}] declared on field [{subfield}]"
                    )

                targetpath = f"{sourcepath}.{subfield}"

                mappers.append(clstype(sourcepath, targetpath, **mapping))

        return mappers


class Object(dict):
    def __repr__(self):
        return f'Object({{ {", ".join("%r: %r" % i for i in self.items())} }})'


class Value(abc.ABC):
    def __init__(
        self,
        value: typing.Union[
            str, int, float, bool, dict, typing.Sequence, decimal.Decimal, None
        ],
    ) -> None:
        self.value = value


class Null(Value):
    _instance = None

    def __init__(self) -> None:
        super().__init__(None)

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    def __repr__(self):
        return "Null"


class Keyword(Value):
    def __init__(self, value) -> None:
        super().__init__(value)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Keyword):
            return False

        return self.value == __o.value

    def __repr__(self):
        return f"Keyword('{self.value}')"

    def startswith(self, other: Keyword) -> bool:
        is_self_primitive = isinstance(self.value, (str, int, float, bool))
        is_other_primitive = isinstance(other.value, (str, int, float, bool))
        if is_self_primitive and is_other_primitive:
            return str(self.value).startswith(str(other.value))
        else:
            return False

    @classmethod
    def parse(cls, body) -> typing.Iterable[Keyword]:
        return tuple(cls.parse_single(i) for i in utils.walk_json_field(body))

    @classmethod
    def parse_single(cls, body) -> Keyword:
        return Keyword(body)


class Boolean(Value):
    def __init__(self, value) -> None:
        super().__init__(value)

    def __repr__(self):
        return f"Boolean({self.value})"

    @classmethod
    def parse(cls, body) -> typing.Iterable[Boolean]:
        return tuple(cls.parse_single(i) for i in utils.walk_json_field(body))

    @classmethod
    def parse_single(cls, body: typing.Union[str, bool]) -> Boolean:
        if isinstance(body, str):
            return cls.parse_string(body)
        if isinstance(body, bool):
            return cls.parse_boolean(body)

        raise ParsingException("boolean expected")

    @classmethod
    def parse_string(cls, body: str) -> Boolean:
        if body == "true":
            return cls.parse_boolean(True)
        if body == "false":
            return cls.parse_boolean(False)
        if body == "":
            return cls.parse_boolean(False)

        raise ParsingException("boolean expected")

    @classmethod
    def parse_boolean(cls, body: bool) -> Boolean:
        return Boolean(body)


class Float(Value):
    PATTERN = re.compile(r"^\d+(\.\d+)?$")

    def __init__(self, value: float) -> None:
        super().__init__(value)

    def __repr__(self):
        return f"Float({self.value})"

    def __ge__(self, __o: Float) -> bool:
        assert isinstance(self.value, float)
        assert isinstance(__o.value, float)
        return self.value >= __o.value

    def __gt__(self, __o: Float) -> bool:
        assert isinstance(self.value, float)
        assert isinstance(__o.value, float)
        return self.value > __o.value

    def __le__(self, __o: Float) -> bool:
        assert isinstance(self.value, float)
        assert isinstance(__o.value, float)
        return self.value <= __o.value

    def __lt__(self, __o: Float) -> bool:
        assert isinstance(self.value, float)
        assert isinstance(__o.value, float)
        return self.value < __o.value

    @classmethod
    def match_pattern(cls, body: str) -> bool:
        return Float.PATTERN.match(body) is not None

    @classmethod
    def parse(cls, body) -> typing.Iterable[Float]:
        return tuple(
            [cls.parse_single(i) for i in utils.walk_json_field(body)]
        )

    @classmethod
    def parse_single(cls, body: typing.Union[str, int, float]) -> Float:
        if isinstance(body, str):
            return cls.parse_string(body)
        if isinstance(body, (int, float)):
            return cls.parse_numeric(body)

        raise ParsingException("numeric expected")

    @classmethod
    def parse_string(cls, body: str) -> Float:
        if utils.match_numeric_pattern(body):
            return cls.parse_numeric(body)

        raise NumberFormatException(f'For input string: "{body}"')

    @classmethod
    def parse_numeric(cls, body: typing.Union[str, int, float]) -> Float:
        return Float(float(body))


class Double(Value):
    PATTERN = re.compile(r"^\d+(\.\d+)?$")

    def __init__(self, value: decimal.Decimal) -> None:
        super().__init__(value)

    def __repr__(self):
        return f"Double({self.value})"

    def __ge__(self, __o: Double) -> bool:
        assert isinstance(self.value, decimal.Decimal)
        assert isinstance(__o.value, decimal.Decimal)
        return self.value >= __o.value

    def __gt__(self, __o: Double) -> bool:
        assert isinstance(self.value, decimal.Decimal)
        assert isinstance(__o.value, decimal.Decimal)
        return self.value > __o.value

    def __le__(self, __o: Double) -> bool:
        assert isinstance(self.value, decimal.Decimal)
        assert isinstance(__o.value, decimal.Decimal)
        return self.value <= __o.value

    def __lt__(self, __o: Double) -> bool:
        assert isinstance(self.value, decimal.Decimal)
        assert isinstance(__o.value, decimal.Decimal)
        return self.value < __o.value

    @classmethod
    def match_pattern(cls, body: str) -> bool:
        return Double.PATTERN.match(body) is not None

    @classmethod
    def parse(cls, body) -> typing.Iterable[Double]:
        return tuple(
            [cls.parse_single(i) for i in utils.walk_json_field(body)]
        )

    @classmethod
    def parse_single(cls, body: typing.Union[str, int, float]) -> Double:
        if isinstance(body, str):
            return cls.parse_string(body)
        if isinstance(body, (int, float)):
            return cls.parse_numeric(body)

        raise ParsingException("numeric expected")

    @classmethod
    def parse_string(cls, body: str) -> Double:
        if utils.match_numeric_pattern(body):
            return cls.parse_numeric(body)

        raise NumberFormatException(f'For input string: "{body}"')

    @classmethod
    def parse_numeric(cls, body: typing.Union[str, int, float]) -> Double:
        return Double(decimal.Decimal(body))


class Long(Value):
    PATTERN = re.compile(r"^\d+$")

    def __init__(self, value: int) -> None:
        super().__init__(value)

    def __repr__(self):
        return f"Long({self.value})"

    def __ge__(self, __o: Long) -> bool:
        assert isinstance(self.value, int)
        assert isinstance(__o.value, int)
        return self.value >= __o.value

    def __gt__(self, __o: Long) -> bool:
        assert isinstance(self.value, int)
        assert isinstance(__o.value, int)
        return self.value > __o.value

    def __le__(self, __o: Long) -> bool:
        assert isinstance(self.value, int)
        assert isinstance(__o.value, int)
        return self.value <= __o.value

    def __lt__(self, __o: Long) -> bool:
        assert isinstance(self.value, int)
        assert isinstance(__o.value, int)
        return self.value < __o.value

    @classmethod
    def match_pattern(cls, value: str) -> bool:
        return Long.PATTERN.match(value) is not None

    @classmethod
    def parse(cls, body) -> typing.Iterable[Long]:
        return tuple(cls.parse_single(i) for i in utils.walk_json_field(body))

    @classmethod
    def parse_single(cls, body: typing.Union[str, int, float]) -> Long:
        if isinstance(body, str):
            return cls.parse_string(body)
        if isinstance(body, (int, float)):
            return cls.parse_numeric(body)

        raise ParsingException("numeric expected")

    @classmethod
    def parse_string(cls, body: str) -> Long:
        if utils.match_numeric_pattern(body):
            return cls.parse_numeric(int(body))

        raise NumberFormatException(f'For input string: "{body}"')

    @classmethod
    def parse_numeric(cls, body: typing.Union[int, float]) -> Long:
        return Long(int(body))


class Date(Value):
    NOW_PATTERN = re.compile(
        r"^now((?P<delta_measure>[-+]\d+)(?P<delta_unit>[yMwdhHms]))?(/(?P<round_unit>[yMwdhHms]))?$"
    )
    ANCHOR_PATTERN = re.compile(
        r"^(?P<anchor>.+)\|\|((?P<delta_measure>[-+]\d+)(?P<delta_unit>[yMwdhHms]))?(/(?P<round_unit>[yMwdhHms]))?$"
    )

    def __init__(self, epoch: int) -> None:
        # milliseconds-since-the-epoch in UTC
        self.epoch = epoch

    def __repr__(self):
        return f"Date({self.epoch})"

    def __eq__(self, __o) -> bool:
        if not isinstance(__o, Date):
            return False

        return self.epoch == __o.epoch

    def __ge__(self, __o: Date) -> bool:
        return self.epoch >= __o.epoch

    def __gt__(self, __o: Date) -> bool:
        return self.epoch > __o.epoch

    def __le__(self, __o: Date) -> bool:
        return self.epoch <= __o.epoch

    def __lt__(self, __o: Date) -> bool:
        return self.epoch < __o.epoch

    @classmethod
    def parse_date_format(cls, format: str) -> typing.Sequence[str]:
        formats = []

        for f in format.split("||"):
            f_lower = f.lower()

            if f_lower == "date_optional_time":
                formats.extend(
                    [
                        "yyyy-MM-dd",
                        "yy-MM-dd",
                        "yyyy-MM-dd'T'HH:mm::ss.SSSZ",
                        "yy-MM-dd'T'HH:mm::ss.SSSZ",
                    ]
                )
            elif f_lower == "strict_date_optional_time":
                formats.extend(["yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm::ss.SSSZ"])
            elif f_lower == "strict_date_optional_time_nanos":
                formats.extend(
                    ["yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm::ss.SSSSSSZ"]
                )
            elif f_lower == "basic_date":
                formats.extend(["yyyyMMdd"])
            elif f_lower == "basic_date_time":
                formats.extend(["yyyyMMdd'T'HHmmss.SSSZ"])
            elif f_lower == "basic_date_time_no_millis":
                formats.extend(["yyyyMMdd'T'HHmmssZ"])
            elif f_lower == "basic_ordinal_date":
                formats.extend(["yyyyDDD"])
            elif f_lower == "basic_ordinal_date_time":
                formats.extend(["yyyyDDD'T'HHmmss.SSSZ"])
            elif f_lower == "basic_ordinal_date_time_no_millis":
                formats.extend(["yyyyDDD'T'HHmmssZ"])
            elif f_lower == "basic_time":
                formats.extend(["HHmmss.SSSZ"])
            elif f_lower == "basic_time_no_millis":
                formats.extend(["HHmmssZ"])
            elif f_lower == "basic_t_time":
                formats.extend(["'T'HHmmss.SSSZ"])
            elif f_lower == "basic_t_time_no_millis":
                formats.extend(["'T'HHmmssZ"])
            elif f_lower == "date":
                formats.extend(["yy-MM-dd", "yyyy-MM-dd"])
            elif f_lower == "strict_date":
                formats.extend(["yyyy-MM-dd"])
            elif f_lower == "strict_date_hour":
                formats.extend(["yyyy-MM-dd'T'HH"])
            elif f_lower == "strict_date_hour_minute":
                formats.extend(["yyyy-MM-dd'T'HH:mm"])
            elif f_lower == "strict_date_hour_minute_second":
                formats.extend(["yyyy-MM-dd'T'HH:mm:ss"])
            elif f_lower == "strict_date_hour_minute_second_fraction":
                formats.extend(["yyyy-MM-dd'T'HH:mm:ss.SSS"])
            elif f_lower == "strict_date_hour_minute_second_millis":
                formats.extend(["yyyy-MM-dd'T'HH:mm:ss.SSS"])
            elif f_lower == "strict_date_time":
                formats.extend(["yyyy-MM-dd'T'HH:mm:ss.SSSZ"])
            elif f_lower == "strict_date_time_no_millis":
                formats.extend(["yyyy-MM-dd'T'HH:mm:ssZ"])
            elif f_lower == "strict_hour":
                formats.extend(["HH"])
            elif f_lower == "strict_hour_minute":
                formats.extend(["HH:mm"])
            elif f_lower == "strict_hour_minute_second":
                formats.extend(["HH:mm:ss"])
            elif f_lower == "strict_hour_minute_second_fraction":
                formats.extend(["HH:mm:ss.SSS"])
            elif f_lower == "strict_hour_minute_second_millis":
                formats.extend(["HH:mm:ss.SSS"])
            elif f_lower == "strict_ordinal_date":
                formats.extend(["yyyy-DDD"])
            elif f_lower == "strict_ordinal_date_time":
                formats.extend(["yyyy-DDD'T'HH:mm:ss.SSSZ"])
            elif f_lower == "strict_ordinal_date_time_no_millis":
                formats.extend(["yyyy-DDD'T'HH:mm:ssZ"])
            elif f_lower == "strict_time":
                formats.extend(["HH:mm:ss.SSSZ"])
            elif f_lower == "strict_time_no_millis":
                formats.extend(["HH:mm:ssZ"])
            elif f_lower == "strict_t_time":
                formats.extend(["'T'HH:mm:ss.SSSZ"])
            elif f_lower == "strict_t_time_no_millis":
                formats.extend(["'T'HH:mm:ssZ"])
            elif f_lower == "strict_year":
                formats.extend(["yyyy"])
            elif f_lower == "strict_year_month":
                formats.extend(["yyyy-MM"])
            elif f_lower == "strict_year_month_day":
                formats.extend(["yyyy-MM-dd"])
            else:
                formats.extend([f])

        return formats

    @classmethod
    def match_date_format(
        cls, value: typing.Union[str, int, float], format: str
    ) -> bool:
        "Test if value match with java date format"
        for f in cls.parse_date_format(format):
            f_lower = f.lower()

            if f_lower == "epoch_millis":
                if isinstance(value, str) and not str.isdigit(value):
                    continue

                try:
                    datetime.datetime.utcfromtimestamp(float(value) / 1000)
                    return True
                except ValueError:
                    pass

            elif f_lower == "epoch_second":
                if isinstance(value, str) and not str.isdigit(value):
                    continue

                try:
                    datetime.datetime.utcfromtimestamp(float(value))
                    return True
                except ValueError:
                    pass

            else:
                try:
                    datetime.datetime.strptime(
                        str(value), utils.transpose_date_format(f)
                    )
                    return True
                except ValueError:
                    pass
                except re.error:
                    raise Exception(utils.transpose_date_format(f))

        return False

    @classmethod
    def parse(cls, body, format: str) -> typing.Iterable[Date]:
        return tuple(
            cls.parse_single(i, format) for i in utils.walk_json_field(body)
        )

    @classmethod
    def parse_single(
        cls, body: typing.Union[str, int, float], format: str
    ) -> Date:
        for f in cls.parse_date_format(format):
            if cls.match_date_format(body, f):
                if f == "epoch_millis":
                    return Date(int(body))
                elif f == "epoch_second":
                    return Date(int(body * 1000))
                else:
                    return Date(
                        int(
                            datetime.datetime.utcfromtimestamp(
                                datetime.datetime.strptime(
                                    str(body), utils.transpose_date_format(f)
                                ).timestamp()
                            ).timestamp()
                            * 1000
                        )
                    )

        raise DateTimeParseException(
            f"Text '{body}' could not be parsed with formats [{format}]"
        )

    @classmethod
    def match_date_math_pattern(cls, body: str) -> bool:
        return (
            Date.ANCHOR_PATTERN.match(body) is not None
            or Date.NOW_PATTERN.match(body) is not None
        )

    @classmethod
    def parse_date_math(cls, body: str) -> Date:
        match_anchor = Date.ANCHOR_PATTERN.match(body)
        if match_anchor is not None:
            dt = datetime.datetime.fromtimestamp(
                cls.parse_single(
                    match_anchor.group("anchor"), format="yyyy.MM.dd"
                ).epoch
                / 1000
            )

            delta_measure = match_anchor.group("delta_measure")
            delta_unit = match_anchor.group("delta_unit")
            if delta_measure is not None and delta_unit is not None:
                dt = dt + cls.relativedelta(int(delta_measure), delta_unit)

            round_unit = match_anchor.group("round_unit")
            if round_unit is not None:
                dt = cls.round(dt, round_unit)
            return Date(int(dt.timestamp() * 1000))

        match_now = Date.NOW_PATTERN.match(body)
        if match_now is not None:
            dt = datetime.datetime.utcnow()

            delta_measure = match_now.group("delta_measure")
            delta_unit = match_now.group("delta_unit")
            if delta_measure is not None and delta_unit is not None:
                dt = dt + cls.relativedelta(int(delta_measure), delta_unit)

            round_unit = match_now.group("round_unit")
            if round_unit is not None:
                dt = cls.round(dt, round_unit)
            return Date(int(dt.timestamp() * 1000))

        raise ElasticError("bad match now and anchor")

    @classmethod
    def relativedelta(
        cls, measure: int, unit: str
    ) -> dateutil.relativedelta.relativedelta:
        if unit == "y":
            return dateutil.relativedelta.relativedelta(years=measure)
        elif unit == "M":
            return dateutil.relativedelta.relativedelta(months=measure)
        elif unit == "w":
            return dateutil.relativedelta.relativedelta(weeks=measure)
        elif unit == "d":
            return dateutil.relativedelta.relativedelta(days=measure)
        elif unit in ("h", "H"):
            return dateutil.relativedelta.relativedelta(hours=measure)
        elif unit == "m":
            return dateutil.relativedelta.relativedelta(minutes=measure)
        elif unit == "s":
            return dateutil.relativedelta.relativedelta(seconds=measure)
        else:
            raise ElasticError(f"bad time unit [{unit}]")

    @classmethod
    def round(cls, dt: datetime.datetime, unit: str) -> datetime.datetime:
        if unit == "y":
            return dt.replace(
                month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
        elif unit == "M":
            return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif unit == "d":
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif unit in ("h", "H"):
            return dt.replace(minute=0, second=0, microsecond=0)
        elif unit == "m":
            return dt.replace(second=0, microsecond=0)
        elif unit == "s":
            return dt.replace(microsecond=0)

        raise ElasticError("bad round unit")


class Text(Value):
    def __init__(self, value: str, analyzer: analysis.Analyzer) -> None:
        super().__init__(value)
        self.index = {
            word: len(tuple(words))
            for word, words in itertools.groupby(
                tuple(analyzer.create_stream(value))
            )
        }

    def __iter__(self):
        yield from self.index.keys()

    def __repr__(self):
        words = ", ".join(f"'{w}'" for w in iter(self))

        return f"Text(words=[{words}])"

    @classmethod
    def parse(cls, body, analyzer: analysis.Analyzer) -> typing.Iterable[Text]:
        return tuple(
            cls.parse_single(i, analyzer) for i in utils.walk_json_field(body)
        )

    @classmethod
    def parse_single(cls, body, analyzer: analysis.Analyzer) -> Text:
        return Text(body, analyzer)


class Geodistance(Value):
    DISTANCE_PATTERN = re.compile(
        r"^(?P<measure>\d+)(?P<unit>mi|miles|yd|yards|ft|feet|in|inch|km|kilometers|m|meters|cm|centimeters|mm|millimeters|NM|nmi|nauticalmiles)$"
    )

    class Unit(utils.CaseInsensitveEnum):
        MILE = "MILE"
        YARD = "YARD"
        FEET = "FEET"
        INCH = "INCH"
        KILOMETER = "KILOMETER"
        METER = "METER"
        CENTIMETER = "CENTIMETER"
        MILLIMETER = "MILLIMETER"
        NAUTICALMILE = "NAUTICALMILE"

    _MILLIS_MULTIPLIERS = {
        Unit.MILE: 1609344,
        Unit.YARD: 914.4,
        Unit.FEET: 304.8,
        Unit.INCH: 25.4,
        Unit.KILOMETER: 1000000,
        Unit.METER: 1000,
        Unit.CENTIMETER: 10,
        Unit.MILLIMETER: 1,
        Unit.NAUTICALMILE: 1852000,
    }

    _UNIT_MAPPING = {
        "mi": Unit.MILE,
        "miles": Unit.MILE,
        "yd": Unit.YARD,
        "yards": Unit.YARD,
        "ft": Unit.FEET,
        "feet": Unit.FEET,
        "in": Unit.INCH,
        "inch": Unit.INCH,
        "km": Unit.KILOMETER,
        "kilometers": Unit.KILOMETER,
        "m": Unit.METER,
        "meters": Unit.METER,
        "cm": Unit.CENTIMETER,
        "centimeters": Unit.CENTIMETER,
        "mm": Unit.MILLIMETER,
        "millimeters": Unit.MILLIMETER,
        "NM": Unit.NAUTICALMILE,
        "nmi": Unit.NAUTICALMILE,
        "nauticalmiles": Unit.NAUTICALMILE,
    }

    def __init__(
        self, value: typing.Union[str, dict], measure: float, unit: Unit
    ) -> None:
        super().__init__(value)
        self.measure = measure
        self.unit = unit

    def millimeters(self) -> float:
        return self.measure * Geodistance._MILLIS_MULTIPLIERS[self.unit]

    def __gt__(self, __o: Geodistance) -> bool:
        return self.millimeters() > __o.millimeters()

    def __ge__(self, __o: Geodistance) -> bool:
        return self.millimeters() >= __o.millimeters()

    def __lt__(self, __o: Geodistance) -> bool:
        return self.millimeters() < __o.millimeters()

    def __le__(self, __o: Geodistance) -> bool:
        return self.millimeters() <= __o.millimeters()

    def __repr__(self):
        return f"Geodistance(measure={self.measure}, unit='{self.unit.value}')"

    @classmethod
    def parse_single(cls, body: str) -> Geodistance:
        if isinstance(body, str):
            return cls.parse_string(body)
        raise ParsingException("geo_distance expected")

    @classmethod
    def parse_string(cls, body: str) -> Geodistance:
        match = Geodistance.DISTANCE_PATTERN.match(body)

        if match is None:
            raise ElasticError("bad distance format")

        body_measure = match.group("measure")
        body_unit = match.group("unit")

        measure = float(body_measure)
        unit = Geodistance._UNIT_MAPPING[body_unit]

        return Geodistance(body, measure, unit)


class Geopoint(Value):
    class DistanceType(utils.CaseInsensitveEnum):
        ARC = "ARC"
        PLANE = "PLANE"

        @classmethod
        def of(cls, distance_type: str) -> Geopoint.DistanceType:
            return Geopoint.DistanceType[distance_type.upper()]

    def __init__(
        self,
        value: typing.Union[str, dict, typing.Sequence[float]],
        point: shapely.geometry.Point,
    ) -> None:
        super().__init__(value)
        self.point = point

    def distance(
        self, __o: Geopoint, distance_type: DistanceType
    ) -> Geodistance:
        if distance_type == Geopoint.DistanceType.ARC:
            measure = haversine.haversine(
                point1=(self.point.y, self.point.x),
                point2=(__o.point.y, __o.point.x),
                unit=haversine.Unit.METERS,
            )
            return Geodistance({}, measure, Geodistance.Unit.METER)
        else:
            raise ElasticError("bad distance type")

    @classmethod
    def parse(cls, body) -> typing.Iterable[Geopoint]:
        if utils.is_array(body):
            try:
                return tuple([cls.parse_array(body)])
            except ParsingException:
                return tuple(cls.parse_single(i) for i in body)

        return tuple([cls.parse_single(body)])

    @classmethod
    def parse_single(
        cls, body: typing.Union[dict, str, typing.Sequence[float]]
    ) -> Geopoint:
        if isinstance(body, dict):
            return cls.parse_object(body)
        elif isinstance(body, str):
            return cls.parse_string(body)
        elif utils.is_array(body):
            return cls.parse_array(body)

        raise ParsingException("geo_point expected")

    @classmethod
    def parse_object(cls, body: dict) -> Geopoint:
        if not ("lat" in body and "lon" in body):
            raise IllegalArgumentException("[lat] and [lon] expected")

        point = shapely.geometry.Point(body["lon"], body["lat"])

        return Geopoint(body, point)

    @classmethod
    def parse_string(cls, body: str) -> Geopoint:
        lat_lon_pattern = re.compile(
            r"^(?P<lon>[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)),\s*(?P<lat>[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?))$"
        )

        # Try wkt expressed as lon lat
        try:
            point = shapely.wkt.loads(body)
            if point is not None:
                if not isinstance(point, shapely.geometry.Point):
                    raise ElasticError("wkt point expected")

                return Geopoint(body, point)
        except Exception:
            pass

        # Try lat,lon
        match = lat_lon_pattern.match(body)
        if match is not None:
            lat = float(match.group("lat"))
            lon = float(match.group("lon"))
            point = shapely.geometry.Point(lon, lat)
            return Geopoint(body, point)

        # Try geohash
        try:
            coords = pygeohash.decode(body)
            point = shapely.geometry.Point(*coords)
            return Geopoint(body, point)
        except Exception:
            pass

        raise ParsingException(
            f"couldn't build wkt or lon,lat or geohash using [{body}]"
        )

    @classmethod
    def parse_array(cls, body: typing.Sequence[float]) -> Geopoint:
        if not 2 <= len(body) <= 3:
            raise ParsingException("geo_point expected")

        if not isinstance(body[0], float):
            raise ParsingException("geo_point expected")

        return Geopoint(body, shapely.geometry.Point(*body))


class Geoshape(Value):
    class Orientation(utils.CaseInsensitveEnum):
        RIGHT = "RIGHT"
        LEFT = "LEFT"

    def __init__(
        self,
        value: typing.Union[str, dict],
        shape: typing.Union[shapely.geometry.Point, shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    ) -> None:
        super().__init__(value)
        self.shape = shape

    def intersects(self, __o: Geoshape) -> bool:
        return self.shape.intersects(__o.shape)

    def contains(self, __o: Geoshape) -> bool:
        return self.shape.contains(__o.shape)

    def __repr__(self):
        if isinstance(self.shape, shapely.geometry.Point):
            return f"Geoshape('Point', {self.shape.x}, {self.shape.y})"
        elif isinstance(self.shape, shapely.geometry.Polygon):
            return f"Geoshape('Polygon', {list(self.shape.exterior.coords)})"
        elif isinstance(self.shape, shapely.geometry.MultiPolygon):
            return f"Geoshape('MultiPolygon', ...)"

    @classmethod
    def parse(cls, body) -> typing.Iterable[Geoshape]:
        return tuple(cls.parse_single(i) for i in utils.walk_json_field(body))

    @classmethod
    def parse_single(cls, body: typing.Union[dict, str]) -> Geoshape:
        if isinstance(body, dict):
            return cls.parse_object(body)
        elif isinstance(body, str):
            return cls.parse_string(body)

        raise ParsingException("geo_shape expected")

    @classmethod
    def parse_string(cls, body: str) -> Geoshape:
        return Geoshape(body, typing.cast(shapely.MultiPolygon, shapely.wkt.loads(body)))

    @classmethod
    def parse_object(cls, body: dict) -> Geoshape:
        t = typing.cast(str, body["type"])
        t = t.upper()

        if t == "POINT":
            coords = typing.cast(list, body["coordinates"])
            return Geoshape(body, shapely.geometry.Point(*coords))
        elif t == "POLYGON":
            coords = typing.cast(typing.List[list], body["coordinates"])
            return Geoshape(
                body,
                shapely.geometry.Polygon(shell=coords[0], holes=coords[1:]),
            )
        elif t == "MULTIPOLYGON":
            coords = typing.cast(typing.List, body["coordinates"])
            return Geoshape(
                body,
                shapely.geometry.MultiPolygon(
                    polygons=[
                        (polygon[0], polygon[1:])  # polygon  # holes
                        for polygon in coords
                    ]
                ),
            )

        raise ParsingException("geo_shape expected")


class Scripting:
    def __init__(self, source: str, lang: str, params: dict) -> None:
        self.source = source
        self.lang = lang
        self.params = params

    def execute(self, variables: dict):
        try:
            if self.lang == "painless":
                painless.execute(
                    self.source,
                    {
                        **variables,
                        "params": java_json.loads(json.dumps(self.params)),
                    },
                )
            else:
                raise NotImplementedError(
                    f"scripting lang {self.lang} not yet supported"
                )
        except Exception as e:
            raise ScriptException("runtime error") from e

    def dumps(self, variables: dict):
        "Converts python mapping into scripting language mapping"
        try:
            if self.lang == "painless":
                return java_json.loads(json.dumps(variables))
            else:
                raise NotImplementedError(
                    f"scripting lang {self.lang} not yet supported"
                )
        except Exception as e:
            raise ScriptException("casting error") from e

    def loads(self, variables: dict):
        "Convers from scripting language mapping into python mapping"
        try:
            if self.lang == "painless":
                return json.loads(java_json.dumps(variables))
            else:
                raise NotImplementedError(
                    f"scripting lang {self.lang} not yet supported"
                )
        except Exception as e:
            raise ScriptException("casting error") from e

    @classmethod
    def parse(cls, body: typing.Union[str, dict]) -> Scripting:
        if isinstance(body, str):
            return cls.parse_string(body)
        if isinstance(body, dict):
            return cls.parse_object(body)

        raise ElasticError("params not supported")

    @classmethod
    def parse_string(cls, body: str) -> Scripting:
        return Scripting(body, "painless", {})

    @classmethod
    def parse_object(cls, body: dict) -> Scripting:
        body_source = body.get("source", None)
        body_lang = body.get("lang", "painless")
        body_params = body.get("params", {})

        return Scripting(body_source, body_lang, body_params)


def read_from_document(
    sourcepath: str, document: Document, _default: typing.Any = MISSING
) -> typing.Any:
    try:
        if sourcepath in document:
            return _raise_if_missing(
                _missing_if_empty_array(
                    utils.get_from_mapping([sourcepath], document)
                )
            )

        return _raise_if_missing(
            _missing_if_empty_array(
                utils.get_from_mapping(
                    sourcepath.split("."), document["_source"]
                )
            )
        )
    except Exception:
        return _raise_if_missing(_default)


def _missing_if_empty_array(v):
    if isinstance(v, (list, tuple)) and len(v) == 0:
        return MISSING

    return v


def _raise_if_missing(v):
    if v is MISSING:
        raise KeyError()
    return v
