"""Compatibility layer for running the dialect on SQLAlchemy 2.0 and 2.1."""

from typing import Any

from sqlalchemy import exc, util
from sqlalchemy.engine.url import make_url


__all__ = [
    "ASYNC_FALLBACK_SUPPORTED",
    "COMPARABLE_OPERATOR_CLASSES",
    "URL_DATABASE_IS_DECODED",
    "async_fallback_enabled",
    "await_",
    "await_fallback",
]

try:
    from sqlalchemy.util.concurrency import await_
except ImportError:
    from sqlalchemy.util.concurrency import await_only as await_

try:
    from sqlalchemy.util.concurrency import await_fallback
except ImportError:
    await_fallback = None

try:
    from sqlalchemy.types import OperatorClass
except ImportError:
    OperatorClass = None

#: SQLAlchemy 2.1 removed the ``async_fallback`` mode, along with ``await_fallback``.
ASYNC_FALLBACK_SUPPORTED = await_fallback is not None

#: SQLAlchemy 2.1 percent-decodes ``URL.database``; 2.0 hands it over still encoded.
URL_DATABASE_IS_DECODED = make_url("aiotrino:///%2B").database == "+"

#: Operators Trino accepts on MAP and ROW values: equality, ``IN``, ``IS [NOT] NULL``, ``IS DISTINCT FROM``.
#: SQLAlchemy 2.0 has no operator classes and ignores the attribute.
COMPARABLE_OPERATOR_CLASSES = OperatorClass.BASE | OperatorClass.COMPARISON if OperatorClass is not None else None


def async_fallback_enabled(value: Any) -> bool:
    """Read an ``async_fallback`` option, refusing it where SQLAlchemy no longer supports it."""
    enabled = util.asbool(value)
    if enabled and not ASYNC_FALLBACK_SUPPORTED:
        raise exc.ArgumentError(
            "async_fallback is not supported on SQLAlchemy 2.1 and later, which removed it. "
            "Use create_async_engine() without async_fallback, or pin sqlalchemy<2.1."
        )
    return enabled
