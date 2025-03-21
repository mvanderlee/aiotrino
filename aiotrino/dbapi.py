# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""

This module implements the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

Fetch methods returns rows as a list of lists on purpose to let the caller
decide to convert then to a list of tuples.
"""
from __future__ import annotations

import datetime
import math
import uuid
from collections import OrderedDict
from decimal import Decimal
from threading import Lock
from time import time
from typing import Any, NamedTuple, Optional, Union
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import aiohttp
from aioitertools import islice as aio_islice

import aiotrino.client
import aiotrino.exceptions
import aiotrino.logging
from aiotrino import constants
from aiotrino.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from aiotrino.transaction import NO_TRANSACTION, IsolationLevel, Transaction
from aiotrino.utils import aiter, anext

__all__ = [
    # https://www.python.org/dev/peps/pep-0249/#globals
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    # https://www.python.org/dev/peps/pep-0249/#exceptions
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"

logger = aiotrino.logging.get_logger(__name__)


class TimeBoundLRUCache:
    """A bounded LRU cache which expires entries after a configured number of seconds.
    Note that expired entries will be evicted only on an attempted access (or through
    the LRU policy)."""

    def __init__(self, capacity: int, ttl_seconds: int):
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.cache = OrderedDict()
        self.lock = Lock()

    def get(self, key):
        with self.lock:
            if key not in self.cache:
                return None
            value, timestamp = self.cache[key]
            if time() - timestamp > self.ttl_seconds:
                self.cache.pop(key)
                return None
            self.cache.move_to_end(key)
            return value

    def put(self, key, value):
        with self.lock:
            self.cache[key] = value, time()
            self.cache.move_to_end(key)
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

    def __repr__(self):
        return f"LRUCache(capacity: {self.capacity}, ttl: {self.ttl_seconds} seconds, {self.cache})"


must_use_legacy_prepared_statements = TimeBoundLRUCache(1024, 3600)


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database.

    See class :py:class:`Connection` for arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


_USE_DEFAULT_ENCODING = object()


class Connection(object):
    """Trino supports transactions and the ability to either commit or rollback
    a sequence of SQL statements. A single query i.e. the execution of a SQL
    statement, can also be cancelled. Transactions are not supported by this
    client implementation yet.

    """

    def __init__(
        self,
        host: str,
        port: int = constants.DEFAULT_PORT,
        user: Optional[str] = None,
        source: str = constants.DEFAULT_SOURCE,
        catalog: str = constants.DEFAULT_CATALOG,
        schema: str = constants.DEFAULT_SCHEMA,
        session_properties: Optional[dict[str, str]] = None,
        http_headers: Optional[dict[str, str]] = None,
        http_scheme: str = constants.HTTP,
        auth: Optional[Any] = constants.DEFAULT_AUTH,
        extra_credential: Optional[list[tuple[str, str]]] = None,
        max_attempts: int = constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout: float = constants.DEFAULT_REQUEST_TIMEOUT,
        isolation_level: IsolationLevel = IsolationLevel.AUTOCOMMIT,
        verify: bool = True,
        http_session: Optional[aiohttp.ClientSession] = None,
        client_tags: Optional[list[str]] = None,
        legacy_primitive_types: bool = False,
        legacy_prepared_statements: Optional[Any] = None,
        roles: Optional[Union[dict[str, str], str]] = None,
        timezone: Optional[str] = None,
        encoding: Union[str, list[str]] = _USE_DEFAULT_ENCODING,
    ):
        # Automatically assign http_schema, port based on hostname
        parsed_host = urlparse(host, allow_fragments=False)

        if encoding is _USE_DEFAULT_ENCODING:
            encoding = [
                "json+zstd",
                "json+lz4",
                "json",
            ]

        self.host = host if parsed_host.hostname is None else parsed_host.hostname + parsed_host.path
        self.port = port if parsed_host.port is None else parsed_host.port
        self.user = user
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.session_properties = session_properties
        self._client_session = aiotrino.client.ClientSession(
            user=user,
            catalog=catalog,
            schema=schema,
            source=source,
            properties=session_properties,
            headers=http_headers,
            transaction_id=NO_TRANSACTION,
            extra_credential=extra_credential,
            client_tags=client_tags,
            roles=roles,
            timezone=timezone,
            encoding=encoding,
        )
        # mypy cannot follow module import
        if http_session is None:
            self._http_session = aiotrino.client.TrinoRequest.http.ClientSession(
                connector=aiotrino.client.TrinoTCPConnector(verify_ssl=verify)
            )
        else:
            self._http_session = http_session
        self.http_headers = http_headers
        self.http_scheme = http_scheme if not parsed_host.scheme else parsed_host.scheme
        self.auth = auth
        self.extra_credential = extra_credential
        self.max_attempts = max_attempts
        self.request_timeout = request_timeout
        self.client_tags = client_tags

        self._isolation_level = isolation_level
        self._request = None
        self._transaction = None
        self.legacy_primitive_types = legacy_primitive_types
        self.legacy_prepared_statements = legacy_prepared_statements
        self._verify_ssl = verify

    @property
    def isolation_level(self) -> IsolationLevel:
        return self._isolation_level

    @property
    def transaction(self) -> Transaction:
        return self._transaction

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        try:
            await self.commit()
        except Exception:
            await self.rollback()
        else:
            await self.close()

    async def close(self):
        """Trino does not have anything to close"""
        # TODO cancel outstanding queries?
        if self._http_session and not self._http_session.closed:
            await self._http_session.connector.close()

    async def start_transaction(self):
        self._transaction = Transaction(self._create_request())
        await self._transaction.begin()
        return self._transaction

    async def commit(self):
        if self.transaction is None:
            return
        await self._transaction.commit()
        await self._transaction.close()
        self._transaction = None

    async def rollback(self):
        if self.transaction is None:
            raise RuntimeError("no transaction was started")
        await self._transaction.rollback()
        await self._transaction.close()
        self._transaction = None

    def _create_request(self):
        # Creating session here because aiohttp requires it to be created inside the event loop.
        # The Connection only calls this from async functions so therefore it will be in event loop
        if self._http_session is None:
            self._http_session = aiohttp.ClientSession(connector=aiotrino.client.TrinoTCPConnector(verify_ssl=self._verify_ssl))  # type: ignore

        return aiotrino.client.TrinoRequest(
            host=self.host,
            port=self.port,
            client_session=self._client_session,
            http_session=self._http_session,
            http_scheme=self.http_scheme,
            auth=self.auth,
            max_attempts=self.max_attempts,
            request_timeout=self.request_timeout,
        )

    async def cursor(self, cursor_style: str = "row", legacy_primitive_types: bool = None) -> "Cursor":
        """Return a new :py:class:`Cursor` object using the connection."""
        if self.isolation_level != IsolationLevel.AUTOCOMMIT:
            if self.transaction is None:
                await self.start_transaction()

        if self.transaction is not None:
            request = self.transaction.request
        else:
            request = self._create_request()

        cursor_class = {
            # Add any custom Cursor classes here
            "segment": SegmentCursor,
            "row": Cursor,
        }.get(cursor_style.lower(), Cursor)

        return cursor_class(
            self,
            request,
            legacy_primitive_types=(
                legacy_primitive_types if legacy_primitive_types is not None else self.legacy_primitive_types
            ),
        )

    async def _use_legacy_prepared_statements(self):
        if self.legacy_prepared_statements is not None:
            return self.legacy_prepared_statements

        value = must_use_legacy_prepared_statements.get((self.host, self.port))
        if value is None:
            try:
                query = aiotrino.client.TrinoQuery(self._create_request(), query="EXECUTE IMMEDIATE 'SELECT 1'")
                await query.execute()
                value = False
            except Exception as e:
                logger.warning(
                    "EXECUTE IMMEDIATE not available for %s:%s; defaulting to legacy prepared statements (%s)",
                    self.host,
                    self.port,
                    e,
                )
                value = True
            must_use_legacy_prepared_statements.put((self.host, self.port), value)
        return value

    def is_closed(self) -> bool:
        return self._http_session and self._http_session.closed


class DescribeOutput(NamedTuple):
    name: str
    catalog: str
    schema: str
    table: str
    type: str
    type_size: int
    aliased: bool

    @classmethod
    def from_row(cls, row: list[Any]):
        return cls(*row)


class ColumnDescription(NamedTuple):
    name: str
    type_code: int
    display_size: int
    internal_size: int
    precision: int
    scale: int
    null_ok: bool

    @classmethod
    def from_column(cls, column: dict[str, Any]):
        type_signature = column["typeSignature"]
        raw_type = type_signature["rawType"]
        arguments = type_signature["arguments"]
        return cls(
            column["name"],  # name
            column["type"],  # type_code
            None,  # display_size
            arguments[0]["value"] if raw_type in constants.LENGTH_TYPES else None,  # internal_size
            arguments[0]["value"] if raw_type in constants.PRECISION_TYPES else None,  # precision
            arguments[1]["value"] if raw_type in constants.SCALE_TYPES else None,  # scale
            None,  # null_ok
        )


class Cursor(object):
    """Database cursor.

    Cursors are not isolated, i.e., any changes done to the database by a
    cursor are immediately visible by other cursors or connections.

    """

    def __init__(
        self,
        connection: Connection,
        request: aiotrino.client.TrinoRequest,
        legacy_primitive_types: bool = False,
    ):
        if not isinstance(connection, Connection):
            raise ValueError("connection must be a Connection object: {}".format(type(connection)))
        self._connection = connection
        self._request = request

        self.arraysize = 1
        self._iterator = None
        self._query = None
        self._legacy_primitive_types = legacy_primitive_types

    def __aiter__(self):
        return self._iterator

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @property
    def connection(self) -> Connection:
        return self._connection

    @property
    def info_uri(self) -> str:
        if self._query is not None:
            return self._query.info_uri
        return None

    @property
    def update_type(self) -> str:
        if self._query is not None:
            return self._query.update_type
        return None

    async def get_description(self) -> list[ColumnDescription]:
        if self._query is None or await self._query.get_columns() is None:
            return None

        return [ColumnDescription.from_column(col) for col in await self._query.get_columns()]

    @property
    def rowcount(self) -> int:
        """The rowcount will be returned for INSERT, UPDATE, DELETE, MERGE
        and CTAS statements based on `update_count` returned by the Trino
        API.

        If the rowcount can't be determined, -1 will be returned.

        Trino cannot reliably determine the number of rows returned for DQL
        queries. For example, the result of a SELECT query is streamed and
        the number of rows is only known when all rows have been retrieved.

        See https://peps.python.org/pep-0249/#rowcount
        """
        if self._query is not None and self._query.update_count is not None:
            return self._query.update_count
        return -1

    @property
    def stats(self) -> dict[Any, Any]:
        if self._query is not None:
            return self._query.stats
        return None

    @property
    def query_id(self) -> str:
        if self._query is not None:
            return self._query.query_id
        return None

    @property
    def query(self) -> str:
        if self._query is not None:
            return self._query.query
        return None

    @property
    def warnings(self) -> list[dict[Any, Any]]:
        if self._query is not None:
            return self._query.warnings
        return None

    def setinputsizes(self, sizes):
        raise aiotrino.exceptions.NotSupportedError

    def setoutputsize(self, size, column):
        raise aiotrino.exceptions.NotSupportedError

    async def _prepare_statement(self, statement: str, name: str) -> None:
        """
        Registers a prepared statement for the provided `operation` with the
        `name` assigned to it.

        :param statement: sql to be executed.
        :param name: name that will be assigned to the prepared statement.
        """
        sql = f"PREPARE {name} FROM {statement}"

        # Send prepare statement. Copy the _request object to avoid poluting the
        # one that is going to be used to execute the actual operation.
        query = aiotrino.client.TrinoQuery(
            self.connection._create_request(),
            query=sql,
            legacy_primitive_types=self._legacy_primitive_types,
        )
        await query.execute()

    def _execute_prepared_statement(self, statement_name: str, params) -> aiotrino.client.TrinoQuery:
        sql = "EXECUTE " + statement_name + " USING " + ",".join(map(self._format_prepared_param, params))
        return aiotrino.client.TrinoQuery(self._request, query=sql, legacy_primitive_types=self._legacy_primitive_types)

    def _execute_immediate_statement(self, statement: str, params) -> aiotrino.client.TrinoQuery:
        """
        Binds parameters and executes a statement in one call.

        :param statement: sql to be executed.
        :param params: parameters to be bound.
        """
        sql = (
            "EXECUTE IMMEDIATE '" + statement.replace("'", "''") + "' USING " + ",".join(map(self._format_prepared_param, params))
        )
        return aiotrino.client.TrinoQuery(
            self.connection._create_request(), query=sql, legacy_primitive_types=self._legacy_primitive_types
        )

    def _format_prepared_param(self, param):
        """
        Formats parameters to be passed in an
        EXECUTE statement.
        """
        if param is None:
            return "NULL"

        if isinstance(param, bool):
            return "true" if param else "false"

        if isinstance(param, int):
            # TODO represent numbers exceeding 64-bit (BIGINT) as DECIMAL
            return "%d" % param

        if isinstance(param, float):
            if param == float("+inf"):
                return "infinity()"
            if param == float("-inf"):
                return "-infinity()"
            if math.isnan(param):
                return "nan()"
            return "DOUBLE '%s'" % param

        if isinstance(param, str):
            return "'%s'" % param.replace("'", "''")

        if isinstance(param, (bytes, bytearray)):
            return "X'%s'" % param.hex()

        if isinstance(param, datetime.datetime) and param.tzinfo is None:
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            return "TIMESTAMP '%s'" % datetime_str

        if isinstance(param, datetime.datetime) and param.tzinfo is not None:
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            # named timezones
            if isinstance(param.tzinfo, ZoneInfo):
                return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.key)
            # offset-based timezones
            return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.tzname(param))

        # We can't calculate the offset for a time without a point in time
        if isinstance(param, datetime.time) and param.tzinfo is None:
            time_str = param.strftime("%H:%M:%S.%f")
            return "TIME '%s'" % time_str

        if isinstance(param, datetime.time) and param.tzinfo is not None:
            time_str = param.strftime("%H:%M:%S.%f")
            # named timezones
            if isinstance(param.tzinfo, ZoneInfo):
                utc_offset = datetime.datetime.now(tz=param.tzinfo).strftime("%z")
                return "TIME '%s %s:%s'" % (time_str, utc_offset[:3], utc_offset[3:])
            # offset-based timezones
            return "TIME '%s %s'" % (time_str, param.strftime("%Z")[3:])

        if isinstance(param, datetime.date):
            date_str = param.strftime("%Y-%m-%d")
            return "DATE '%s'" % date_str

        if isinstance(param, list):
            return "ARRAY[%s]" % ",".join(map(self._format_prepared_param, param))

        if isinstance(param, tuple):
            return "ROW(%s)" % ",".join(map(self._format_prepared_param, param))

        if isinstance(param, dict):
            keys = list(param.keys())
            values = [param[key] for key in keys]
            return "MAP({}, {})".format(self._format_prepared_param(keys), self._format_prepared_param(values))

        if isinstance(param, uuid.UUID):
            return "UUID '%s'" % param

        if isinstance(param, Decimal):
            return "DECIMAL '%s'" % format(param, "f")

        raise aiotrino.exceptions.NotSupportedError("Query parameter of type '%s' is not supported." % type(param))

    async def _deallocate_prepared_statement(self, statement_name: str) -> None:
        sql = "DEALLOCATE PREPARE " + statement_name
        query = aiotrino.client.TrinoQuery(
            self.connection._create_request(), query=sql, legacy_primitive_types=self._legacy_primitive_types
        )
        await query.execute()

    def _generate_unique_statement_name(self) -> str:
        return "st_" + uuid.uuid4().hex.replace("-", "")

    async def execute(self, operation, params=None):
        if params:
            assert isinstance(params, (list, tuple)), "params must be a list or tuple containing the query parameter values"

            if await self.connection._use_legacy_prepared_statements():
                statement_name = self._generate_unique_statement_name()
                await self._prepare_statement(operation, statement_name)

                try:
                    # Send execute statement and assign the return value to `results`
                    # as it will be returned by the function
                    self._query = self._execute_prepared_statement(statement_name, params)
                    self._iterator = aiter(await self._query.execute())
                finally:
                    # Send deallocate statement
                    # At this point the query can be deallocated since it has already
                    # been executed
                    # TODO: Consider caching prepared statements if requested by caller
                    await self._deallocate_prepared_statement(statement_name)
            else:
                self._query = self._execute_immediate_statement(operation, params)
                self._iterator = aiter(await self._query.execute())

        else:
            self._query = aiotrino.client.TrinoQuery(
                self._request, query=operation, legacy_primitive_types=self._legacy_primitive_types
            )
            self._iterator = aiter(await self._query.execute())
        return self

    async def executemany(self, operation, seq_of_params):
        """
        PEP-0249: Prepare a database operation (query or command) and then
        execute it against all parameter sequences or mappings found in the sequence seq_of_parameters.
        Modules are free to implement this method using multiple calls to
        the .execute() method or by using array operations to have the
        database process the sequence as a whole in one call.

        Use of this method for an operation which produces one or more result
        sets constitutes undefined behavior, and the implementation is permitted (but not required)
        to raise an exception when it detects that a result set has been created by an invocation of the operation.

        The same comments as for .execute() also apply accordingly to this method.

        Return values are not defined.
        """
        for parameters in seq_of_params[:-1]:
            await self.execute(operation, parameters)
            await self.fetchall()
            if self._query.update_type is None:
                raise NotSupportedError("Query must return update type")
        if seq_of_params:
            await self.execute(operation, seq_of_params[-1])
        else:
            await self.execute(operation)
        return self

    async def fetchone(self) -> Optional[list[Any]]:
        """

        PEP-0249: Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        try:
            assert self._iterator is not None
            return await anext(self._iterator)
        except StopAsyncIteration:
            return None
        except aiotrino.exceptions.HttpError as err:
            raise aiotrino.exceptions.OperationalError(str(err)) from None

    async def fetchmany(self, size: Optional[int] = None) -> list[list[Any]]:
        """
        PEP-0249: Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty sequence is
        returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If
        it is not given, the cursor's arraysize determines the number of rows
        to be fetched. The method should try to fetch as many rows as indicated
        by the size parameter. If this is not possible due to the specified
        number of rows not being available, fewer rows may be returned.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter. For optimal performance, it is usually best to use the
        .arraysize attribute. If the size parameter is used, then it is best
        for it to retain the same value from one .fetchmany() call to the next.
        """

        if size is None:
            size = self.arraysize

        return [row async for row in aio_islice(aiter(self.fetchone, None), size)]

    async def describe(self, sql: str) -> list[DescribeOutput]:
        """
        List the output columns of a SQL statement, including the column name (or alias), catalog, schema, table, type,
        type size in bytes, and a boolean indicating if the column is aliased.

        :param sql: SQL statement
        """
        statement_name = self._generate_unique_statement_name()
        await self._prepare_statement(sql, statement_name)
        try:
            sql = f"DESCRIBE OUTPUT {statement_name}"
            self._query = aiotrino.client.TrinoQuery(
                self._request,
                query=sql,
                legacy_primitive_types=self._legacy_primitive_types,
            )
            result = await self._query.execute()
        finally:
            await self._deallocate_prepared_statement(statement_name)

        return [DescribeOutput.from_row(x) async for x in result]

    def genall(self):
        return self._query.result

    async def fetchall(self) -> list[list[Any]]:
        return [row async for row in aiter(self.fetchone, None)]

    async def cancel(self):
        if self._query is None:
            return
        await self._query.cancel()

    async def close(self):
        await self.cancel()
        await self._request.close()


class SegmentCursor(Cursor):
    def __init__(self, connection, request, legacy_primitive_types: bool = False):
        super().__init__(connection, request, legacy_primitive_types=legacy_primitive_types)
        if self.connection._client_session.encoding is None:
            raise ValueError("SegmentCursor can only be used if encoding is set on the connection")

    async def execute(self, operation, params=None):
        if params:
            # TODO: refactor code to allow for params to be supported
            raise ValueError("params not supported")

        self._query = aiotrino.client.TrinoQuery(
            self._request,
            query=operation,
            legacy_primitive_types=self._legacy_primitive_types,
            fetch_mode="segments",
        )
        self._iterator = aiter(await self._query.execute())
        return self


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
TimestampFromTicks = datetime.datetime.fromtimestamp


def TimeFromTicks(ticks):
    return datetime.time(*datetime.localtime(ticks)[3:6])


def Binary(string):
    return string.encode("utf-8")


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = [v.lower() for v in values]

    def __eq__(self, other):
        return other.lower() in self.values


STRING = DBAPITypeObject("VARCHAR", "CHAR", "VARBINARY", "JSON", "IPADDRESS")

BINARY = DBAPITypeObject("ARRAY", "MAP", "ROW", "HyperLogLog", "P4HyperLogLog", "QDigest")

NUMBER = DBAPITypeObject("BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL")

DATETIME = DBAPITypeObject(
    "DATE",
    "TIME",
    "TIME WITH TIME ZONE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "INTERVAL YEAR TO MONTH",
    "INTERVAL DAY TO SECOND",
)

ROWID = DBAPITypeObject()  # nothing indicates row id in Trino
