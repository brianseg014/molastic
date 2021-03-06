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
        self.mappers = Mappers()
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
        self.documents_by_id: typing.Dict[str, Document] = {}

        if mappings:
            self.update_mappings(mappings)

    @property
    def mappings(self) -> typing.Mapping:
        return self.mappers.mappings

    @property
    def documents(self) -> typing.Sequence[Document]:
        return tuple(self.documents_by_id.values())

    def update_mappings(self, mappings: typing.Mapping):
        self.mappers.merge(mappings)

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
        _stored_document = self.documents_by_id.get(id, None)

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

        self.make_searchable(_document)

        if not exists:
            operation_result = OperationResult.CREATED
        else:
            operation_result = OperationResult.UPDATED

        return _document, operation_result

    def get(self):
        raise NotImplementedError()

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
        _stored_document = self.documents_by_id.get(id, None)
        if _stored_document is None:
            return None, OperationResult.NOT_FOUND

        self.documents_by_id.pop(id)

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

        _stored_document = self.documents_by_id.get(id, None)

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

            ctx = scripting.dumps({"_source": _source})

            scripting.execute({"ctx": ctx})

            _source = scripting.loads(ctx)["_source"]

        elif "doc" in body:
            _source = utils.source_merger.merge(_doc_base_copy, body["doc"])

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

        self.make_searchable(_document)

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
        return _id in self.documents_by_id

    def create_document_id(self) -> str:
        return str(uuid.uuid4())

    def make_searchable(self, document: Document):
        self.mappers.dynamic_map(document["_source"])

        self.documents_by_id[document["_id"]] = document


class Mapper(abc.ABC):
    def __init__(self, fieldpath: str, type: str) -> None:
        self.fieldpath = fieldpath
        self.fieldmapping: typing.MutableMapping[str, typing.Any] = {
            "type": type
        }

    @property
    def fieldname(self) -> str:
        return self.fieldpath.split(".")[-1]

    @property
    def type(self) -> str:
        return self.fieldmapping["type"]

    @property
    def mappings(self) -> typing.Mapping:
        return self.fieldmapping

    @abc.abstractmethod
    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        "Create merger functions. A merger function merges actual with new mappings"
        pass

    @abc.abstractmethod
    def map(self, body) -> typing.Iterable[Value]:
        "Create runtime value given a source body"
        pass


class ObjectMapper(Mapper, collections.abc.MutableMapping):
    @property
    def mappings(self) -> dict:
        if "properties" in self.fieldmapping:
            return {
                "properties": {
                    k: typing.cast(Mapper, v).mappings
                    for k, v in self.fieldmapping["properties"].items()
                }
            }
        else:
            return {}

    def __getitem__(self, __k):
        return self.fieldmapping["properties"][__k]

    def __setitem__(self, __k, __v):
        if "properties" not in self.fieldmapping:
            self.fieldmapping["properties"] = {}
        self.fieldmapping["properties"][__k] = __v

    def __delitem__(self, __k):
        del self.fieldmapping["properties"][__k]

    def __iter__(self):
        return iter(self.fieldmapping["properties"])

    def __len__(self):
        return len(self.fieldmapping["properties"])

    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        for paramname in fieldmapping:
            if paramname == "properties":
                pass
            else:
                raise MapperParsingException(
                    f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
                )
        return []

    def map(self, body) -> typing.Iterable[Value]:
        raise TypeError("should not map")


class KeywordMapper(Mapper):
    @property
    def ignore_above(self) -> int:
        return self.fieldmapping.get("ignore_above", 2147483647)

    @ignore_above.setter
    def ignore_above(self, ignore_above):
        self.fieldmapping["ignore_above"] = ignore_above

    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        mergers: typing.List[typing.Callable[[], None]] = []
        for paramname in fieldmapping:
            if paramname == "ignore_above":
                mergers.append(
                    lambda: setattr(
                        self, "ignore_above", fieldmapping[")ignore_above"]
                    )
                )
            else:
                raise MapperParsingException(
                    f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
                )
        return mergers

    def map(self, body) -> typing.Iterable[Keyword]:
        return Keyword.parse(body)


class BooleanMapper(Mapper):
    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        for paramname in fieldmapping:
            raise MapperParsingException(
                f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
            )
        return []

    def map(self, body) -> typing.Iterable[Value]:
        return Boolean.parse(body)


class FloatMapper(Mapper):
    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        for paramname in fieldmapping:
            raise MapperParsingException(
                f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
            )
        return []

    def map(self, body) -> typing.Iterable[Float]:
        return Float.parse(body)


class DoubleMapper(Mapper):
    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        for paramname in fieldmapping:
            raise MapperParsingException(
                f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
            )
        return []

    def map(self, body) -> typing.Iterable[Value]:
        return Double.parse(body)


class LongMapper(Mapper):
    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        for paramname in fieldmapping:
            raise MapperParsingException(
                f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
            )
        return []

    def map(self, body) -> typing.Iterable[Long]:
        return Long.parse(body)


class DateMapper(Mapper):
    @property
    def format(self) -> str:
        return self.fieldmapping.get(
            "format", "strict_date_optional_time||epoch_millis"
        )

    @format.setter
    def format(self, format):
        Date.parse_date_format(format)
        self.fieldmapping["format"] = format

    def __repr__(self):
        return f"DateMapper(format={repr(self.format)})"

    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        mergers: typing.List[typing.Callable[[], None]] = []
        for paramname in fieldmapping:
            if paramname == "format":
                mergers.append(
                    lambda: setattr(self, "format", fieldmapping["format"])
                )
            else:
                raise MapperParsingException(
                    f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
                )
        return mergers

    @typing.overload
    def map(self, body) -> typing.Iterable[Date]:
        ...

    @typing.overload
    def map(self, body, format: str) -> typing.Iterable[Date]:
        ...

    def map(
        self, body, format: typing.Optional[str] = None
    ) -> typing.Iterable[Date]:
        return Date.parse(body, format or self.format)


class TextMapper(Mapper):
    default_analyzer = analysis.StandardAnalyzer()

    def __init__(self, fieldpath: str, type: str) -> None:
        super().__init__(fieldpath, type)
        self._analyzer: analysis.Analyzer = self.default_analyzer

    @property
    def analyzer(self) -> analysis.Analyzer:
        return self._analyzer

    @analyzer.setter
    def analyzer(self, analyzer: str):
        self._analyzer = self.create_analyzer(analyzer)

    def create_analyzer(self, analyzer: str) -> analysis.Analyzer:
        if analyzer == "standard":
            return self.default_analyzer
        else:
            raise NotImplementedError(f"analyzer: {analyzer}")

    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        mergers: typing.List[typing.Callable[[], None]] = []
        for paramname in fieldmapping:
            if paramname == "analyzer":
                mergers.append(
                    lambda: (
                        setattr(self, "analyzer", fieldmapping["analyzer"])
                    )
                )
            else:
                raise MapperParsingException(
                    f"unknown parameter [{paramname}] on mapper "
                    f"[{self.fieldname}] of type [{self.type}]"
                )
        return mergers

    @typing.overload
    def map(self, body) -> typing.Iterable[Text]:
        ...

    @typing.overload
    def map(self, body, analyzer: analysis.Analyzer) -> typing.Iterable[Text]:
        ...

    def map(
        self, body, analyzer: typing.Optional[analysis.Analyzer] = None
    ) -> typing.Iterable[Text]:
        return Text.parse(body, analyzer or self.analyzer)


class GeopointMapper(Mapper):
    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        for paramname in fieldmapping:
            raise MapperParsingException(
                f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
            )
        return []

    def map(self, body) -> typing.Iterable[Geopoint]:
        return Geopoint.parse(body)


class GeoshapeMapper(Mapper):
    def merge(
        self, fieldmapping: typing.Mapping
    ) -> typing.Iterable[typing.Callable[[], None]]:
        for paramname in fieldmapping:
            raise MapperParsingException(
                f"unknown parameter [{paramname}] on mapper [{self.fieldname}] of type [{self.type}]"
            )
        return []

    def map(self, body) -> typing.Iterable[Geoshape]:
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

    def map(self, value) -> typing.Optional[typing.Mapping]:
        """Value in the document to infer data type.
        Returns None if the field should not be mapped.
        """
        if self.dynamic == DynamicMapping.Dynamic.false:
            return None

        if self.dynamic == DynamicMapping.Dynamic.strict:
            return None

        while utils.is_array(value):
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


class Mappers:

    MAPPERS: typing.Mapping[str, typing.Type[Mapper]] = {
        "object": ObjectMapper,
        "keyword": KeywordMapper,
        "boolean": BooleanMapper,
        "long": LongMapper,
        "float": FloatMapper,
        "double": DoubleMapper,
        "date": DateMapper,
        "text": TextMapper,
        "geo_point": GeopointMapper,
        "geo_shape": GeoshapeMapper,
    }

    def __init__(
        self,
        dynamic_mapping: typing.Optional[DynamicMapping] = None,
    ) -> None:
        self.dynamic_mapping: DynamicMapping
        if dynamic_mapping is None:
            self.dynamic_mapping = DynamicMapping()
        else:
            self.dynamic_mapping = dynamic_mapping

        self.fieldmappers: typing.Dict[str, Mapper] = {}

    def __iter__(self):
        yield from utils.flatten(self.fieldmappers)

    @property
    def mappings(self) -> typing.Mapping:
        return {
            "properties": {
                k: typing.cast(Mapper, v).mappings
                for k, v in self.fieldmappers.items()
            }
        }

    def put(
        self,
        fieldpath: str,
        fieldmapper: Mapper,
    ) -> None:
        segments = fieldpath.split(".")

        fragment = utils.get_from_mapping(segments[:-1], self.fieldmappers)
        fragment[segments[-1]] = fieldmapper

    def get(self, fieldpath: str, _default: typing.Any = MISSING) -> Mapper:
        segments = fieldpath.split(".")

        try:
            return utils.get_from_mapping(segments[:-1], self.fieldmappers)[
                segments[-1]
            ]
        except KeyError as e:
            if _default is MISSING:
                raise e
            return _default

    def has(self, fieldpath: str) -> bool:
        segments = fieldpath.split(".")

        try:
            fragment = utils.get_from_mapping(segments[:-1], self.fieldmappers)
            return segments[-1] in fragment
        except KeyError:
            return False

    def merge(self, mappings: typing.Mapping) -> None:
        patterns = [re.compile("^properties.\\w+$")]

        mergers: typing.List[typing.Callable[[], None]] = []
        for mappings_path, new_fieldmapping in utils.flatten(mappings):
            # Only if match any pattern is a field
            # otherwise could be a some field property
            if not any(p.match(mappings_path) for p in patterns):
                continue

            segments = mappings_path.split(".")[1::2]
            if len(segments) == 0:
                # Match the first "properties"
                continue

            fieldpath = ".".join(segments)

            actual_fieldmapper = self.get(fieldpath, None)

            if new_fieldmapping.get("type", "object") == "object":
                # Allow to iterate over object.properties
                patterns.append(
                    re.compile(f"^{mappings_path}.properties.\\w+$")
                )

            if actual_fieldmapper is None:
                # Will put new fieldmapper
                new_type = new_fieldmapping.get("type", "object")
                try:
                    clstype = Mappers.MAPPERS[new_type]
                except KeyError:
                    raise MapperParsingException(
                        f"No handler for type [{new_type}] declared on field [{segments[-1]}]"
                    )

                fieldmapper = clstype(fieldpath, new_type)
                assert isinstance(fieldmapper, Mapper)

                mergers.extend(
                    fieldmapper.merge(
                        {
                            k: v
                            for k, v in new_fieldmapping.items()
                            if k != "type"
                        }
                    )
                )
                self.put(fieldpath, fieldmapper)

            else:
                # Mappings already exists, should update
                # the actual fieldmapping and fieldmapper
                actual_type = actual_fieldmapper.type
                new_type = new_fieldmapping.get("type", "object")
                if actual_type != new_type:
                    raise IllegalArgumentException(
                        f"mapper [{segments[-1]}] cannot be changed from "
                        f"type [{actual_type}] to [{new_type}]"
                    )

                mergers.extend(
                    actual_fieldmapper.merge(
                        {
                            k: v
                            for k, v in new_fieldmapping.items()
                            if k != "type"
                        }
                    )
                )

        for merger in mergers:
            merger()

    def dynamic_map(self, source: dict) -> None:
        allowed_prefixes: typing.List[str] = []

        mergers: typing.List[typing.Callable[[], None]] = []
        for fieldpath, fieldvalue in utils.flatten(source):
            if fieldpath.count(".") > 0:
                # Child node
                if not any(fieldpath.startswith(f) for f in allowed_prefixes):
                    # If not child node of ObjectMapper, ignore
                    continue

            if self.has(fieldpath):
                # Let to visit child nodes if is ObjectMapper
                fieldmapper = self.get(fieldpath)
                if fieldmapper.type == "object":
                    allowed_prefixes.append(fieldpath)
                # Ignore known mapper
                continue

            segments = fieldpath.split(".")

            if self.dynamic_mapping.dynamic == DynamicMapping.Dynamic.strict:
                parent_segment = segments[-2] if len(segments) > 1 else "_doc"
                raise StrictDynamicMappingException(
                    f"mapping set to strict, dynamic introduction of [{fieldpath}] within [{parent_segment}] is not allowed"
                )

            fieldmapping = self.dynamic_mapping.map(fieldvalue)
            if fieldmapping is None:
                continue

            fieldtype = fieldmapping.get("type", "object")
            if fieldtype == "object":
                # Let visit object child nodes
                allowed_prefixes.append(fieldpath)

            try:
                clstype = Mappers.MAPPERS[fieldtype]
            except KeyError:
                raise MapperParsingException(
                    f"No handler for type [{fieldtype}] declared on field [{segments[-1]}]"
                )

            fieldmapper = clstype(fieldpath, fieldtype)
            assert isinstance(fieldmapper, Mapper)

            mergers.extend(
                fieldmapper.merge(
                    {k: v for k, v in fieldmapping.items() if k != "type"}
                )
            )
            self.put(fieldpath, fieldmapper)

        for merger in mergers:
            merger()

    def __repr__(self):
        return repr(self.fieldmappers)

    @classmethod
    def parse(cls, mappings: typing.Mapping) -> Mappers:
        if any(True for k in mappings.keys() if k != "properties"):
            raise MapperParsingException(
                f"Root mapping definition has unsupported parameters: {mappings}"
            )

        mappers = Mappers()

        patterns = [re.compile("^properties.\\w+$")]

        mergers: typing.List[typing.Callable[[], None]] = []
        for mapping_k, mapping_v in utils.flatten(mappings):
            if not any(p.match(mapping_k) for p in patterns):
                continue

            segments = mapping_k.split(".")[1::2]
            if len(segments) == 0:
                continue

            if not isinstance(mapping_v, collections.abc.Mapping):
                raise MapperParsingException(
                    f"Expected map for property [properties] "
                    f"on field [{segments[-1]}] but got {mapping_v}"
                )

            fieldpath = ".".join(segments)
            fieldtype = mapping_v.get("type", "object")

            if fieldtype == "object":
                # Let visit object child nodes
                patterns.append(re.compile(f"^{mapping_k}.properties.\\w+$"))

            try:
                clstype = Mappers.MAPPERS[fieldtype]
            except KeyError:
                raise MapperParsingException(
                    f"No handler for type [{fieldtype}] declared on field [{segments[-1]}]"
                )

            fieldmapper = clstype(fieldpath, fieldtype)
            assert isinstance(fieldmapper, Mapper)

            mergers.extend(
                fieldmapper.merge(
                    {k: v for k, v in mapping_v.items() if k != "type"}
                )
            )
            mappers.put(fieldpath, fieldmapper)

        for merger in mergers:
            merger()

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
        r"^(?P<anchor>\w+)\|\|((?P<delta_measure>[-+]\d+)(?P<delta_unit>[yMwdhHms]))?(/(?P<round_unit>[yMwdhHms]))?$"
    )

    def __init__(
        self, value: typing.Union[str, int, float], format: str
    ) -> None:
        super().__init__(value)
        self.format = format

        if format == "epoch_millis":
            self.datetime = datetime.datetime.utcfromtimestamp(
                float(value) / 1000
            )
        elif format == "epoch_second":
            self.datetime = datetime.datetime.utcfromtimestamp(float(value))
        else:
            self.datetime = datetime.datetime.strptime(
                str(value), utils.transpose_date_format(format)
            )

    def __repr__(self):
        if self.format in ["epoch_millis", "epoch_second"]:
            return f"Date({self.value}, '{self.format}')"
        else:
            return f"Date('{self.value}', '{self.format}')"

    def __ge__(self, __o: Date) -> bool:
        return self.datetime >= __o.datetime

    def __gt__(self, __o: Date) -> bool:
        return self.datetime > __o.datetime

    def __le__(self, __o: Date) -> bool:
        return self.datetime <= __o.datetime

    def __lt__(self, __o: Date) -> bool:
        return self.datetime < __o.datetime

    @classmethod
    def parse_date_format(cls, format: str) -> typing.Sequence[str]:
        formats = []

        for f in format.split("||"):
            f_upper = f.upper()

            if f_upper == "DATE_OPTIONAL_TIME":
                formats.extend(
                    [
                        "yyyy-MM-dd",
                        "yy-MM-dd",
                        "yyyy-MM-dd'T'HH:mm::ss.SSSZ",
                        "yy-MM-dd'T'HH:mm::ss.SSSZ",
                    ]
                )
            elif f_upper == "STRICT_DATE_OPTIONAL_TIME":
                formats.extend(["yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm::ss.SSSZ"])
            elif f_upper == "STRICT_DATE_OPTIONAL_TIME_NANOS":
                formats.extend(
                    ["yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm::ss.SSSSSSZ"]
                )
            elif f_upper == "BASIC_DATE":
                formats.extend(["yyyyMMdd"])
            elif f_upper == "BASIC_DATE_TIME":
                formats.extend(["yyyyMMdd'T'HHmmss.SSSZ"])
            elif f_upper == "BASIC_DATE_TIME_NO_MILLIS":
                formats.extend(["yyyyMMdd'T'HHmmssZ"])
            else:
                formats.extend([f])

        return formats

    @classmethod
    def match_date_format(
        cls, value: typing.Union[str, int, float], format: str
    ) -> bool:
        "Test if value match with java date format"
        for f in cls.parse_date_format(format):
            f_upper = f.upper()

            if f_upper == "EPOCH_MILLIS":
                if isinstance(value, str) and not str.isdigit(value):
                    continue

                try:
                    datetime.datetime.utcfromtimestamp(float(value) / 1000)
                    return True
                except ValueError:
                    pass

            elif f_upper == "EPOCH_SECOND":
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
                return Date(body, f)

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
            dt = list(
                cls.parse(match_anchor.group("anchor"), format="yyyy.MM.dd")
            )[0].datetime

            delta_measure = match_anchor.group("delta_measure")
            delta_unit = match_anchor.group("delta_unit")
            if delta_measure is not None and delta_unit is not None:
                dt = dt + cls.relativedelta(int(delta_measure), delta_unit)

            round_unit = match_anchor.group("round_unit")
            if round_unit is not None:
                dt = cls.round(dt, round_unit)
            return Date(dt.timestamp(), "epoch_millis")

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
            return Date(dt.timestamp(), "epoch_second")

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

    @classmethod
    def parse_single(cls, body: str) -> Geodistance:
        if isinstance(body, str):
            return cls.parse_string(body)
        raise ParsingException("geo_distance expected")

    @classmethod
    def parse_string(self, body: str) -> Geodistance:
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
                point1=self.point.coords[0],
                point2=__o.point.coords[0],
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
            lat = match.group("lat")
            lon = match.group("lon")
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
        shape: typing.Union[shapely.geometry.Point, shapely.geometry.Polygon],
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
        return Geoshape(body, shapely.wkt.loads(body))

    @classmethod
    def parse_object(cls, body: dict) -> Geoshape:
        t = typing.cast(str, body["type"])
        t = t.upper()

        if t == "POINT":
            coords = typing.cast(list, body["coordinates"])
            return Geoshape(body, shapely.geometry.Point(*coords))
        elif t == "POLYGON":
            coords = typing.cast(typing.List[list], body["coordinates"])
            return Geoshape(body, shapely.geometry.Polygon(*coords))

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
    fieldname: str, document: Document, _default: typing.Any = MISSING
) -> typing.Any:
    try:
        if fieldname in document:
            return _raise_if_missing(
                _missing_if_empty_array(
                    utils.get_from_mapping([fieldname], document)
                )
            )

        return _raise_if_missing(
            _missing_if_empty_array(
                utils.get_from_mapping(
                    fieldname.split("."), document["_source"]
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
