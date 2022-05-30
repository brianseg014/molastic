import re
import enum
import typing
import deepmerge


source_merger = deepmerge.Merger([], ["override"], ["override"])


class CaseInsensitveEnum(enum.Enum):
    @classmethod
    def _missing_(cls, value: str):
        for member in cls:
            if member.value == value.upper():
                return member


def flatten(
    value: dict,
) -> typing.Generator[typing.Tuple[str, typing.Any], None, None]:
    "Generate key,value pairs flatting a deep dict"

    def _keymap(keys: typing.Sequence[str], key: str):
        if len(keys) > 0:
            return ".".join(keys) + f".{key}"
        else:
            return key

    def _flatten(value: dict, keys: typing.Sequence[str]):
        for k, v in value.items():
            if isinstance(v, dict):
                yield _keymap(keys, k), v
                yield from _flatten(v, keys + [k])
            elif isinstance(v, list):
                if len(v) == 0:
                    continue
                yield _keymap(keys, k), v[0]
            else:
                yield _keymap(keys, k), v

    yield from _flatten(value, [])


def transpose_date_format(format: str) -> str:
    "Convert java date format into python date format"
    mappings = {
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MM": "%m",
        "dd": "%d",
        "HH": "%H",
        "mm": "%M",
        "ss": "%S",
        "SSSSSS": "%f",
        "SSS": "%f",
        "'T'": "T",
    }

    for java_format, python_format in mappings.items():
        format = re.sub(java_format, python_format, format)

    return format.replace("'", "" "" "").replace('"', "")
