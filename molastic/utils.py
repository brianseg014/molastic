import contextlib
import re
import enum
import time
import typing
import functools
import collections.abc
import deepmerge


source_merger = deepmerge.Merger([], ["override"], ["override"])
mapping_merger = deepmerge.Merger(
    [(list, "append"), (dict, "merge"), (set, "union")], [], []
)
mapping_dynamic_merger = deepmerge.Merger(
    [(list, "append"), (dict, "merge"), (set, "union")], ["use_existing"], []
)


class CaseInsensitveEnum(enum.Enum):
    @classmethod
    def _missing_(cls, value: object) -> typing.Any:
        assert isinstance(value, str)

        for member in cls:
            if member.value == value.upper():
                return member


def match_numeric_pattern(v):
    PATTERN = re.compile(r"^\d+(\.\d+)?$")
    return PATTERN.match(v) is not None


def intersperse(lst, item):
    result = [item] * (len(lst) * 2 - 1)
    result[0::2] = lst
    return result


def flatten(
    value: collections.abc.Mapping,
) -> typing.Generator[typing.Tuple[str, typing.Any], None, None]:
    "Generate key,value pairs flatting a deep dict"

    def _keymap(keys: typing.Sequence[str], key: str):
        if len(keys) > 0:
            return ".".join(keys) + f".{key}"
        else:
            return key

    def _flatten(value: collections.abc.Mapping, keys: typing.List[str]):
        for k, v in value.items():
            if isinstance(v, collections.abc.Mapping):
                yield _keymap(keys, k), v
                yield from _flatten(v, keys + [k])
            elif not isinstance(v, str) and isinstance(
                v, collections.abc.Sequence
            ):
                if len(v) == 0:
                    continue
                yield _keymap(keys, k), v[0]
            else:
                yield _keymap(keys, k), v

    yield from _flatten(value, [])


def is_array(v):
    return not isinstance(v, str) and isinstance(v, collections.abc.Sequence)


def walk_json_field(v):
    if is_array(v):
        yield from (walk_json_field(i) for i in v)
    else:
        yield v


def get_from_mapping(segments: typing.Iterable[str], mapping: typing.Mapping):
    return functools.reduce(collections.abc.Mapping.get, segments, mapping)


def transpose_date_format(format: str) -> str:
    "Convert java date format into python date format"
    mappings = {
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MM": "%m",
        "DDD": "%j",
        "dd": "%d",
        "HH": "%H",
        "mm": "%M",
        "ss": "%S",
        "SSSSSS": "%f",
        "SSS": "%f",
        "'T'": "T",
        "Z": "%z",
    }

    for java_format, python_format in mappings.items():
        format = re.sub(java_format, python_format, format)

    return format.replace("'", "" "" "").replace('"', "")


@contextlib.contextmanager
def timer():
    start = time.time() * 1000
    yield lambda: time.time() * 1000 - start
