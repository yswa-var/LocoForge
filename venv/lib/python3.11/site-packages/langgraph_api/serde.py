import asyncio
import re
import uuid
from base64 import b64encode
from collections import deque
from collections.abc import Mapping
from datetime import timedelta, timezone
from decimal import Decimal
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from pathlib import Path
from re import Pattern
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

import cloudpickle
import orjson
import structlog
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

logger = structlog.stdlib.get_logger(__name__)


class Fragment(NamedTuple):
    buf: bytes


def decimal_encoder(dec_value: Decimal) -> int | float:
    """
    Encodes a Decimal as int of there's no exponent, otherwise float

    This is useful when we use ConstrainedDecimal to represent Numeric(x,0)
    where a integer (but not int typed) is used. Encoding this as a float
    results in failed round-tripping between encode and parse.
    Our Id type is a prime example of this.

    >>> decimal_encoder(Decimal("1.0"))
    1.0

    >>> decimal_encoder(Decimal("1"))
    1
    """
    if dec_value.as_tuple().exponent >= 0:
        return int(dec_value)
    else:
        return float(dec_value)


def default(obj):
    # Only need to handle types that orjson doesn't serialize by default
    # https://github.com/ijl/orjson#serialize
    if isinstance(obj, Fragment):
        return orjson.Fragment(obj.buf)
    if (
        hasattr(obj, "model_dump")
        and callable(obj.model_dump)
        and not isinstance(obj, type)
    ):
        return obj.model_dump()
    elif hasattr(obj, "dict") and callable(obj.dict) and not isinstance(obj, type):
        return obj.dict()
    elif (
        hasattr(obj, "_asdict") and callable(obj._asdict) and not isinstance(obj, type)
    ):
        return obj._asdict()
    elif isinstance(obj, BaseException):
        return {"error": type(obj).__name__, "message": str(obj)}
    elif isinstance(obj, (set, frozenset, deque)):  # noqa: UP038
        return list(obj)
    elif isinstance(obj, (timezone, ZoneInfo)):  # noqa: UP038
        return obj.tzname(None)
    elif isinstance(obj, timedelta):
        return obj.total_seconds()
    elif isinstance(obj, Decimal):
        return decimal_encoder(obj)
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(  # noqa: UP038
        obj,
        (
            IPv4Address,
            IPv4Interface,
            IPv4Network,
            IPv6Address,
            IPv6Interface,
            IPv6Network,
            Path,
        ),
    ):
        return str(obj)
    elif isinstance(obj, Pattern):
        return obj.pattern
    elif isinstance(obj, bytes | bytearray):
        return b64encode(obj).decode()
    return None


_option = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NON_STR_KEYS

_SURROGATE_RE = re.compile(r"[\ud800-\udfff]")


def _strip_surr(s: str) -> str:
    return s if _SURROGATE_RE.search(s) is None else _SURROGATE_RE.sub("", s)


def _sanitise(o: Any) -> Any:
    if isinstance(o, str):
        return _strip_surr(o)
    if isinstance(o, Mapping):
        return {_sanitise(k): _sanitise(v) for k, v in o.items()}
    if isinstance(o, list | tuple | set):
        ctor = list if isinstance(o, list) else type(o)
        return ctor(_sanitise(x) for x in o)
    return o


def json_dumpb(obj) -> bytes:
    try:
        return orjson.dumps(obj, default=default, option=_option).replace(
            rb"\u0000", b""
        )  # null unicode char not allowed in json
    except TypeError as e:
        if "surrogates not allowed" not in str(e):
            raise
        surrogate_sanitized = _sanitise(obj)
        return orjson.dumps(
            surrogate_sanitized, default=default, option=_option
        ).replace(rb"\u0000", b"")


def json_loads(content: bytes | Fragment | dict) -> Any:
    if isinstance(content, Fragment):
        content = content.buf
    if isinstance(content, dict):
        return content
    return orjson.loads(content)


async def ajson_loads(content: bytes | Fragment) -> Any:
    return await asyncio.to_thread(json_loads, content)


class Serializer(JsonPlusSerializer):
    def dumps_typed(self, obj: Any) -> tuple[str, bytes]:
        try:
            return super().dumps_typed(obj)
        except TypeError:
            return "pickle", cloudpickle.dumps(obj)

    def loads_typed(self, data: tuple[str, bytes]) -> Any:
        if data[0] == "pickle":
            try:
                return cloudpickle.loads(data[1])
            except Exception as e:
                logger.warning(
                    "Failed to unpickle object, replacing w None", exc_info=e
                )
                return None
        return super().loads_typed(data)
