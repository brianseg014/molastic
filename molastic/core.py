from __future__ import annotations

import re
import sys
import json
import uuid
import abc
import enum
import copy
import functools
import typing
import datetime
import dateutil.relativedelta
import dataclasses
import itertools
import shapely.geometry
import shapely.wkt
import haversine
import pygeohash

from .utils import *

from . import painless
from . import java_json


class MISSING:
    pass


class ElasticError(Exception):
    pass


class IndexNotFoundException(ElasticError):
    pass


class MappingParsingException(ElasticError):
    pass


class MapperParsingException(ElasticError):
    pass


class IllegalArgumentException(ElasticError):
    pass


class DateTimeParseException(ElasticError):
    pass


class VersionConflictException(ElasticError):
    pass


class ActionRequestValidationException(ElasticError):
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


class OperationType(CaseInsensitveEnum):
    INDEX = "INDEX"
    CREATE = "CREATE"


class Refresh(CaseInsensitveEnum):
    TRUE = True
    FALSE = False
    WAIT_FOR = "WAIT_FOR"


class VersionType(CaseInsensitveEnum):
    EXTERNAL = "EXTERNAL"
    EXTERNAL_GTE = "EXTERNAL_GTE"


class OperationResult(CaseInsensitveEnum):
    NOOP = "NOOP"
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    DELETED = "DELETED"


@dataclasses.dataclass(frozen=True)
class IndexOperationResult:
    acknowledged: bool
    shards_acknowleged: bool
    index: str


@dataclasses.dataclass(frozen=True)
class DocumentOperationResult:
    class Shards(typing.TypedDict):
        total: int
        successful: int
        failed: int

    _shards: DocumentOperationResult.Shards
    _index: str
    _id: str
    _version: int
    _seq_no: int
    _primary_term: int
    result: OperationResult


@dataclasses.dataclass(frozen=True)
class Document:
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
        self._indices: typing.Dict[str, Indice] = {}

    def indice(self, _id: str, create: bool = False) -> Indice:
        if create and _id not in self._indices:
            self._indices[_id] = Indice(_id)

        try:
            return self._indices[_id]
        except KeyError:
            raise IndexNotFoundException(f"No such index [{_id}]")

    def indices(self, target: str) -> typing.Sequence[Indice]:
        return tuple(i for i in self._indices.values() if i._id == target)


class Indice:
    def __init__(self, _id: str) -> None:
        self._id = _id

        self._sequence = itertools.count()
        self._config = {"mappings": Mappings()}

        self._documents_by_id: typing.Dict[str, Document] = {}

    @property
    def mappings(self) -> Mappings:
        return self._config["mappings"]

    @property
    def documents(self) -> typing.Sequence[Document]:
        return self._documents_by_id.values()

    def config(self, body: dict) -> IndexOperationResult:
        if "mappings" in body:
            self._config["mappings"].merge(
                Mappings.from_dict(body["mappings"])
            )

        return IndexOperationResult(
            acknowledged=True, shards_acknowleged=True, index=self._id
        )

    def config_mappings(self, body: dict) -> IndexOperationResult:
        self._config["mappings"].merge(Mappings.from_dict(body))
        return IndexOperationResult(
            acknowledged=True, shards_acknowleged=True, index=self._id
        )

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
    ) -> DocumentOperationResult:

        if id is None:
            id = self.create_document_id()

        exists = self.exists(id)
        if exists and op_type == OperationType.CREATE:
            raise ElasticError("document already exists")

        _version: int = 1
        _stored_document = self._documents_by_id.get(id, None)

        if _stored_document is not None:
            _version = _stored_document._version + 1

        _source = body
        self.infer_and_map_properties(_source)

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
            _tier=Tier.DATA_HOT,
            _seq_no=next(self._sequence),
            _primary_term=1,
            _version=_version,
            _stored_fields=None,
        )

        self.make_searchable(_document)

        return DocumentOperationResult(
            _shards=DocumentOperationResult.Shards(
                total=1, successful=1, failed=0
            ),
            _index=self._id,
            _id=_document._id,
            _version=_document._version,
            _seq_no=_document._seq_no,
            _primary_term=_document._primary_term,
            result=(
                OperationResult.CREATED
                if not exists
                else OperationResult.UPDATED
            ),
        )

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
    ):
        _stored_document = self._documents_by_id.get(id, None)

        self._documents_by_id.pop(id)

        return DocumentOperationResult(
            _shards=DocumentOperationResult.Shards(
                total=1, successful=1, failed=0
            ),
            _index=self._id,
            _id=_stored_document._id,
            _version=_stored_document._version,
            _seq_no=_stored_document._seq_no,
            _primary_term=_stored_document._primary_term,
            result=OperationResult.DELETED,
        )

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
    ) -> DocumentOperationResult:

        _version: int = 1

        _stored_document = self._documents_by_id.get(id, None)

        exists = False
        if _stored_document is not None:
            exists = True

        if _stored_document is not None:
            _doc_base = _stored_document._source
            _version = _stored_document._version + 1
        elif body.get("doc_as_upsert", False):
            _doc_base = body["doc"]
        elif body.get("upsert", None) is not None:
            _doc_base = body["upsert"]
        else:
            raise DocumentMissingException("_doc", id)

        _doc_base_copy = copy.deepcopy(_doc_base)
        if "script" in body:
            _source = _doc_base_copy

            java_ctx = java_json.loads(json.dumps({"_source": _source}))

            scripting = Scripting.parse(body["script"])
            scripting.execute({"ctx": java_ctx})

            python_ctx = json.loads(java_json.dumps(java_ctx))
            
            _source = python_ctx["_source"]

        elif "doc" in body:
            _source = source_merger.merge(_doc_base_copy, body["doc"])

        self.infer_and_map_properties(_source)

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
            _tier=Tier.DATA_HOT,
            _seq_no=next(self._sequence),
            _primary_term=1,
            _version=_version,
            _stored_fields=None,
        )

        self.make_searchable(_document)

        return DocumentOperationResult(
            _shards=DocumentOperationResult.Shards(
                total=1, successful=1, failed=0
            ),
            _index=self._id,
            _id=_document._id,
            _version=_document._version,
            _seq_no=_document._seq_no,
            _primary_term=_document._primary_term,
            result=(
                OperationResult.CREATED
                if not exists
                else OperationResult.UPDATED
            ),
        )

    def multi_get(self):
        raise NotImplementedError()

    def bulk(self):
        raise NotImplementedError()

    def delete_by_query(self):
        raise NotImplementedError()

    def update_by_query(self):
        raise NotImplementedError()

    def exists(self, _id: str) -> bool:
        return _id in self._documents_by_id

    def create_document_id(self) -> str:
        return str(uuid.uuid4())

    def infer_and_map_properties(self, source: dict):
        self.mappings.merge(infer_dynamic_mapping(source), ignore_errors=True)

    def make_searchable(self, document: Document):
        self._documents_by_id[document._id] = document


class Mappings:
    def __init__(self) -> None:
        self._properties = Properties()

    def __iter__(self):
        return (
            (
                i[0]
                .replace(".properties.", ".")
                .replace("properties.", "")
                .replace(".properties.", ""),
                i[1],
            )
            for i in flatten(self._properties)
            if isinstance(i[1], (Properties, Type))
        )

    def map(self, field: str, prop: typing.Any) -> None:
        ref = self._properties
        keys = field.split(".")

        for key in keys[0:-1]:
            ref = ref[key]

        ref[keys[-1]] = prop

    def get(
        self, field: str, _default: typing.Any = MISSING
    ) -> typing.Union[Properties, Type]:
        ref = self._properties
        keys = field.split(".")

        try:
            for key in keys[0:-1]:
                ref = ref[key]

            return ref[keys[-1]]
        except KeyError as e:
            if _default is MISSING:
                raise e

            return _default

    def merge(self, mappings: Mappings, ignore_errors: bool = False) -> None:
        blacklist = []

        for k, new_mapped in iter(mappings):
            if any(k.startswith(i) for i in blacklist):
                continue

            old_mapped = self.get(k, None)

            if old_mapped is None:
                # k never been mapped, map
                self.map(k, new_mapped)
            elif isinstance(old_mapped, Type) and isinstance(
                new_mapped, Properties
            ):
                # k already mapped and new map as properties
                # ignore k and all sub-maps
                blacklist.append(k)
                continue
            elif isinstance(new_mapped, Properties):
                # only type should be mapped
                continue
            else:
                # if changing type, error
                # otherwise, ignore k and all sub-maps
                old_type = old_mapped["type"]
                new_type = new_mapped["type"]
                if old_type != new_type:
                    blacklist.append(k)

                    if not ignore_errors:
                        raise IllegalArgumentException(
                            f"mapper [{k}] cannot be changed from "
                            f"type [{old_type}] to [{new_type}]"
                        )

    def __repr__(self):
        return repr(self._properties)

    @classmethod
    def from_dict(cls, mappings: dict) -> Mappings:
        _mappings = Mappings()

        mapped_keys = []

        for k, v in flatten(mappings):
            if k == "properties":
                continue

            if k.endswith(".properties"):
                continue

            if any(k.startswith(mk) for mk in mapped_keys):
                continue

            cleansed_k = (
                k.replace(".properties.", ".")
                .replace("properties.", "")
                .replace(".properties", "")
            )

            if isinstance(v, dict):
                if "type" in v:
                    _mappings.map(cleansed_k, Type(v))
                elif "properties" in v:
                    _mappings.map(cleansed_k, Properties())
                else:
                    raise MappingParsingException(
                        f"No type specified for field [{cleansed_k}]"
                    )

                mapped_keys.append(k)

        return _mappings


class Properties(dict):
    def __init__(self, *args, **kwargs):
        dict.__setitem__(self, "properties", {})

        super().__init__(*args, **kwargs)

    def __setitem__(self, __k, __v) -> None:
        properties = dict.__getitem__(self, "properties")
        properties[__k] = __v

    def __getitem__(self, __k):
        if __k == "properties":
            return dict.__getitem__(self, __k)
        else:
            properties = dict.__getitem__(self, "properties")
            return properties[__k]


class Type(dict):
    pass


class Object(dict):
    def __repr__(self):
        return f'Object({{ {", ".join("%r: %r" % i for i in self.items())} }})'


class Value(abc.ABC):
    def __init__(
        self, value: typing.Union[str, int, float, bool, dict, None]
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

    def __eq__(self, __o: Value) -> bool:
        if not isinstance(__o, Keyword):
            return False

        return self.value == __o.value

    def __repr__(self):
        return f"Keyword('{self.value}')"

    @classmethod
    def parse(cls, body) -> typing.Iterable[Keyword]:
        return tuple(cls.parse_single(i) for i in walk_json_field(body))

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
        return tuple(cls.parse_single(i) for i in walk_json_field(body))

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

    @classmethod
    def match_pattern(cls, body: str) -> bool:
        return Float.PATTERN.match(body)

    @classmethod
    def parse(cls, body) -> typing.Iterable[Float]:
        return tuple(cls.parse_single(i) for i in walk_json_field(body))

    @classmethod
    def parse_single(cls, body: typing.Union[str, int, float]) -> Long:
        if isinstance(body, str):
            return cls.parse_string(body)
        if isinstance(body, (int, float)):
            return cls.parse_numeric(body)

        raise ParsingException("numeric expected")

    @classmethod
    def parse_string(cls, body: str) -> Float:
        if match_numeric_pattern(body):
            return cls.parse_numeric(float(body))

        raise NumberFormatException(f"For input string: \"{body}\"")

    @classmethod
    def parse_numeric(cls, body: typing.Union[int, float]) -> Float:
        return Float(float(body))


class Long(Value):
    PATTERN = re.compile(r"^\d+$")
    def __init__(self, value: int) -> None:
        super().__init__(value)

    def __repr__(self):
        return f"Long({self.value})"

    @classmethod
    def match_pattern(cls, value: str) -> bool:
        return Long.PATTERN.match(value)

    @classmethod
    def parse(cls, body) -> typing.Iterable[Long]:
        return tuple(cls.parse_single(i) for i in walk_json_field(body))

    @classmethod
    def parse_single(cls, body: typing.Union[str, int, float]) -> Long:
        if isinstance(body, str):
            return cls.parse_string(body)
        if isinstance(body, (int, float)):
            return cls.parse_numeric(body)

        raise ParsingException("numeric expected")

    @classmethod
    def parse_string(cls, body: str) -> Long:
        if match_numeric_pattern(body):
            return cls.parse_numeric(int(body))

        raise NumberFormatException(f"For input string: \"{body}\"")

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

    def __init__(self, value: typing.Union[str, int], format: str) -> None:
        super().__init__(value)
        self.format = format

        if format == "epoch_millis":
            self.datetime = datetime.datetime.utcfromtimestamp(value / 1000)
        elif format == "epoch_second":
            self.datetime = datetime.datetime.utcfromtimestamp(value)
        else:
            self.datetime = datetime.datetime.strptime(
                value, transpose_date_format(format)
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
    def match_date_format(cls, value: str, format: str) -> bool:
        "Test if value match with java date format"
        for f in cls.parse_date_format(format):
            f_upper = f.upper()

            if f_upper == "EPOCH_MILLIS":
                if isinstance(value, str) and not str.isdigit(value):
                    continue

                try:
                    datetime.datetime.utcfromtimestamp(value / 1000)
                    return True
                except ValueError:
                    pass

            elif f_upper == "EPOCH_SECOND":
                if isinstance(value, str) and not str.isdigit(value):
                    continue

                try:
                    datetime.datetime.utcfromtimestamp(value)
                    return True
                except ValueError:
                    pass

            else:
                try:
                    datetime.datetime.strptime(value, transpose_date_format(f))
                    return True
                except ValueError:
                    pass
                except re.error:
                    raise Exception(transpose_date_format(f))

        return False

    @classmethod
    def parse(cls, body, format: str) -> typing.Iterable[Date]:
        return tuple(cls.parse_single(i, format) for i in walk_json_field(body))

    @classmethod
    def parse_single(cls, body: typing.Union[str, int], format: str) -> Date:
        for f in cls.parse_date_format(format):
            if cls.match_date_format(body, f):
                return Date(body, f)

        raise DateTimeParseException(
            f"Text '{body}' could not be parsed with formats [{format}]"
        )

    @classmethod
    def match_date_math_pattern(cls, body: str) -> bool:
        return Date.ANCHOR_PATTERN.match(body) or Date.NOW_PATTERN.match(body)

    @classmethod
    def parse_date_math(cls, body: str) -> Date:
        match_anchor = Date.ANCHOR_PATTERN.match(body)
        if match_anchor is not None:
            dt = cls.parse(match_anchor.group("anchor"), format="yyyy.MM.dd")
            dt = dt.datetime

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


class Geodistance(Value):
    DISTANCE_PATTERN = re.compile(
        r"^(?P<measure>\d+)(?P<unit>mi|miles|yd|yards|ft|feet|in|inch|km|kilometers|m|meters|cm|centimeters|mm|millimeters|NM|nmi|nauticalmiles)$"
    )

    class Unit(CaseInsensitveEnum):
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
    class DistanceType(CaseInsensitveEnum):
        ARC = "ARC"
        PLANE = "PLANE"

    def __init__(
        self, value: typing.Union[str, dict], point: shapely.geometry.Point
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
            return Geodistance(None, measure, Geodistance.Unit.METER)
        else:
            raise ElasticError("bad distance type")


    @classmethod
    def parse(cls, body) -> typing.Iterable[Geopoint]:
        if is_array(body):
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
        elif is_array(body):
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
        except:
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
        except:
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
    class Orientation(CaseInsensitveEnum):
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
        return tuple(cls.parse_single(i) for i in walk_json_field(body))

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


class Scripting:
    def __init__(
        self, source: str, lang: str, params: dict, exec: typing.Callable
    ) -> None:
        self.source = source
        self.lang = lang
        self.params = params
        self.exec = exec

    def execute(self, variables: dict):
        try:
            self.exec(self.source, { **variables, "params": java_json.loads(json.dumps(self.params)) })
        except Exception as e:
            raise ScriptException("runtime error", e)

    @classmethod
    def parse(cls, body: typing.Union[str, dict]) -> Scripting:
        if isinstance(body, str):
            return cls.parse_string(body)
        if isinstance(body, dict):
            return cls.parse_object(body)

        raise ElasticError("params not supported")

    @classmethod
    def parse_string(cls, body: str) -> Scripting:
        return Scripting(body, "PainlessLang", {})

    @classmethod
    def parse_object(cls, body: dict) -> Scripting:
        body_source = body.get("source", None)
        body_lang = body.get("lang", "PainlessLang")
        body_params = body.get("params", {})

        if body_lang == "painless":
            exec = painless.execute
        else:
            raise NotImplementedError(f"lang [{body_lang}] not supported")

        return Scripting(body_source, body_lang, body_params, exec)


def infer_dynamic_mapping(
    source: dict, format: str = "strict_date_optional_time"
) -> Mappings:

    mappings = Mappings()
    for k, v in flatten(source):

        while isinstance(v, list):
            if len(v) == 0:
                v = None
            else:
                v = v[0]

        if v is None:
            continue

        if isinstance(v, bool):
            prop = Type({"type": "boolean"})
        elif isinstance(v, int):
            prop = Type({"type": "long"})
        elif isinstance(v, float):
            prop = Type({"type": "float"})
        elif isinstance(v, str):
            if Long.match_pattern(v):
                prop = Type({"type": "long"})
            elif Float.match_pattern(v):
                prop = Type({"type": "float"})
            elif Date.match_date_format(v, format):
                prop = Type({"type": "date"})
            else:
                prop = Type(
                    {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256}
                        },
                    }
                )
        elif isinstance(v, dict):
            prop = Properties()
        else:
            raise NotImplementedError(type(v), v)

        mappings.map(k, prop)

    return mappings


def map_json_to_value(
    body: typing.Any, fieldmapping: typing.Union[Properties, Type]
) -> typing.Iterable[Value]:
    if body is None:
        return Null()

    elif "properties" in fieldmapping:
        return Object()

    else:
        t = fieldmapping["type"]

        if t == "long":
            return Long.parse(body)
        elif t == "float":
            return Float.parse(body)
        elif t == "boolean":
            return Boolean.parse(body)
        elif t == "keyword":
            return Keyword.parse(body)
        elif t == "date":
            return Date.parse(
                body, fieldmapping.get("format", "strict_date_optional_time")
            )
        elif t == "geo_point":
            return Geopoint.parse(body)
        elif t == "geo_shape":
            return Geoshape.parse(body)
        else:
            raise NotImplementedError(t)


def read_from_document(
    fieldname: str, document: Document, _default: typing.Any = MISSING
) -> typing.Any:
    if fieldname in document.__dict__:
        return _missing_if_empty_array(document.__dict__[fieldname])

    try:
        return _missing_if_empty_array( functools.reduce(
            dict.get, fieldname.split("."), document._source
        ))
    except:
        return _default

def _missing_if_empty_array(v):
    if isinstance(v, (list, tuple)) and len(v) == 0:
        return MISSING

    return v


def is_array(v):
    return isinstance(v, (list, tuple))


def walk_json_field(v):
    if is_array(v):
        yield from (walk_json_field(i) for i in v)
    else:
        yield v


def match_numeric_pattern(v):
    PATTERN = re.compile(r"^\d+(\.\d+)?$")
    return PATTERN.match(v) is not None
