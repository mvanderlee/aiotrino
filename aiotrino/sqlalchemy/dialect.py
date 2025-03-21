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
from __future__ import annotations

import asyncio
import datetime
import json
import time
from collections import deque
from collections.abc import Mapping, Sequence
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import unquote_plus

from sqlalchemy import exc, pool, sql, util
from sqlalchemy.engine import AdaptedConnection, Engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import sqltypes
from sqlalchemy.util.concurrency import await_fallback, await_only

from aiotrino import dbapi as aiotrino_dbapi
from aiotrino import logging
from aiotrino.auth import BasicAuthentication, JWTAuthentication
from aiotrino.client import TrinoRequest
from aiotrino.dbapi import ColumnDescription, Cursor, IsolationLevel, NotSupportedError
from aiotrino.sqlalchemy import compiler, datatype, error
from aiotrino.utils import aiter

from .datatype import JSONIndexType, JSONPathType

logger = logging.get_logger(__name__)

colspecs = {
    sqltypes.JSON.JSONIndexType: JSONIndexType,
    sqltypes.JSON.JSONPathType: JSONPathType,
}


class AsyncAdapt_aiotrino_cursor(Cursor):
    server_side = False

    def __init__(
        self,
        adapt_connection: AsyncAdapt_aiotrino_connection,
        request: TrinoRequest,
        legacy_primitive_types: bool = False,
    ):
        super().__init__(adapt_connection._connection, request, legacy_primitive_types=legacy_primitive_types)
        self._adapt_connection = adapt_connection
        self.await_ = adapt_connection.await_

        if not self.server_side:
            self._rows = deque()

    @property
    def description(self) -> list[ColumnDescription]:
        return self.await_(self.get_description())

    def _handle_exception(self, error):
        self._adapt_connection._handle_exception(error)

    async def _fetchall(self) -> list[list[Any]]:
        return [row async for row in aiter(super().fetchone, None)]

    async def _execute(self, operation, parameters):
        adapt_connection = self._adapt_connection

        async with adapt_connection._execute_mutex:
            if not adapt_connection._started:
                await adapt_connection._start_transaction()

            try:
                await super().execute(operation, parameters)

                if not self.server_side:
                    self._rows = deque(await self._fetchall())

            except Exception as error:
                self._handle_exception(error)

    async def _executemany(self, operation, seq_of_parameters):
        adapt_connection = self._adapt_connection

        async with adapt_connection._execute_mutex:
            if not adapt_connection._started:
                await adapt_connection._start_transaction()

            try:
                self._rows = deque()

                for parameters in seq_of_parameters[:-1]:
                    await super().execute(operation, parameters)
                    self._rows.extend(await self._fetchall())
                    if self._query.update_type is None:
                        raise NotSupportedError("Query must return update type")
                if seq_of_parameters:
                    await super().execute(operation, seq_of_parameters[-1])
                else:
                    await super().execute(operation)

                return self._rows

            except Exception as error:
                self._handle_exception(error)

    def execute(self, operation, parameters=None):
        self.await_(
            self._execute(operation, parameters)
        )

    def executemany(self, operation, seq_of_parameters):
        self.await_(
            self._executemany(operation, seq_of_parameters)
        )

    def __iter__(self):
        while self._rows:
            yield self._rows.popleft()

    def fetchone(self):
        if self._rows:
            return self._rows.popleft()
        else:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        rr = self._rows
        return [rr.popleft() for _ in range(min(size, len(rr)))]

    def fetchall(self):
        retval = list(self._rows)
        self._rows.clear()
        return retval


class AsyncAdapt_aiotrino_ss_cursor(AsyncAdapt_aiotrino_cursor):
    __slots__ = ()
    server_side = True

    async def _executemany(self, operation, seq_of_parameters):
        adapt_connection = self._adapt_connection

        async with adapt_connection._execute_mutex:
            if not adapt_connection._started:
                await adapt_connection._start_transaction()

            try:
                return await Cursor.executemany(self, operation, seq_of_parameters)
            except Exception as error:
                self._handle_exception(error)

    def execute(self, operation, parameters=None):
        self.await_(self._execute(operation, parameters))

    def executemany(self, operation, seq_of_parameters):
        self.await_(self._executemany(operation, seq_of_parameters))

    def fetchone(self):
        return self.await_(super().fetchone())

    def fetchmany(self, size=None):
        return self.await_(super().fetchmany(size))

    def fetchall(self):
        return self.await_(super().fetchall())

    def __iter__(self):
        iterator = super().__aiter__()
        while True:
            try:
                yield self.await_(iterator.__anext__())
            except StopAsyncIteration:
                break



class AsyncAdapt_aiotrino_connection(AdaptedConnection):
    await_ = staticmethod(await_only)

    _connection: aiotrino_dbapi.Connection

    @property
    def driver_connection(self) -> aiotrino_dbapi.Connection:
        """The connection object as returned by the driver after a connect."""
        return self._connection

    def __init__(
        self,
        dbapi: "AsyncAdapt_aiotrino_dbapi",
        connection: aiotrino_dbapi.Connection,
    ):
        self.dbapi = dbapi
        self._connection = connection
        self.isolation_level = self._isolation_setting = connection.isolation_level
        self.readonly = False
        self.deferrable = False
        self._transaction: aiotrino_dbapi.Transaction = None
        self._started = False
        self._invalidate_schema_cache_asof = time.time()
        self._execute_mutex = asyncio.Lock()

    def _handle_exception(self, error):
        if self._connection.is_closed():
            self._transaction = None
            self._started = False

        raise error

    @property
    def autocommit(self):
        return self.isolation_level == IsolationLevel.AUTOCOMMIT

    @autocommit.setter
    def autocommit(self, value):
        if value:
            self.isolation_level = IsolationLevel.AUTOCOMMIT
        else:
            self.isolation_level = self._isolation_setting

    def ping(self):
        try:
            _ = self.await_(self._async_ping())
        except Exception as error:
            self._handle_exception(error)

    async def _async_ping(self):
        if self._transaction is None and self.isolation_level != "autocommit":
            # create a tranasction explicitly to support pgbouncer
            # transaction mode.   See #10226
            tr: aiotrino_dbapi.Transaction = self._connection.transaction()
            await tr.begin()
            try:
                async with await self._connection.cursor() as curr:
                    await curr.fetchone(";")
            finally:
                await tr.rollback()
        else:
            async with await self._connection.cursor() as curr:
                await curr.fetchone(";")

    def set_isolation_level(self, level):
        if self._started:
            self.rollback()
        self.isolation_level = self._isolation_setting = level

    async def _start_transaction(self):
        if self.isolation_level == IsolationLevel.AUTOCOMMIT:
            return

        try:
            self._transaction = await self._connection.start_transaction()
        except Exception as error:
            self._handle_exception(error)
        else:
            self._started = True

    async def build_cursor(self, server_side: bool = False) -> AsyncAdapt_aiotrino_cursor:
        if self.isolation_level != IsolationLevel.AUTOCOMMIT:
            if self._transaction is None:
                await self._start_transaction()

        if self._transaction is not None:
            request = self._transaction.request
        else:
            request = self._connection._create_request()

        cursor_cls = AsyncAdapt_aiotrino_ss_cursor if server_side else AsyncAdapt_aiotrino_cursor

        return cursor_cls(
            self,
            request,
            legacy_primitive_types=self._connection.legacy_primitive_types,
        )

    def cursor(self, server_side: bool = False) -> AsyncAdapt_aiotrino_cursor:
        return self.await_(self.build_cursor(server_side))

    async def _rollback_and_discard(self):
        try:
            await self._transaction.rollback()
        finally:
            # if asyncpg .rollback() was actually called, then whether or
            # not it raised or succeeded, the transation is done, discard it
            self._transaction = None
            self._started = False

    async def _commit_and_discard(self):
        try:
            await self._transaction.commit()
        finally:
            # if asyncpg .commit() was actually called, then whether or
            # not it raised or succeeded, the transation is done, discard it
            self._transaction = None
            self._started = False

    def rollback(self):
        if self._started:
            try:
                self.await_(self._rollback_and_discard())
                self._transaction = None
                self._started = False
            except Exception as error:
                # don't dereference asyncpg transaction if we didn't
                # actually try to call rollback() on it
                self._handle_exception(error)

    def commit(self):
        if self._started:
            try:
                self.await_(self._commit_and_discard())
                self._transaction = None
                self._started = False
            except Exception as error:
                # don't dereference asyncpg transaction if we didn't
                # actually try to call commit() on it
                self._handle_exception(error)

    def close(self):
        self.rollback()

        self.await_(self._connection.close())

    def terminate(self):
        if util.concurrency.in_greenlet():
            # in a greenlet; this is the connection was invalidated
            # case.
            try:
                # try to gracefully close; see #10717
                # timeout added in asyncpg 0.14.0 December 2017
                self.await_(asyncio.shield(self._connection.close(timeout=2)))
            except (
                asyncio.TimeoutError,
                asyncio.CancelledError,
                OSError,
                self.dbapi.aiotrino.Error,
            ):
                # in the case where we are recycling an old connection
                # that may have already been disconnected, close() will
                # fail with the above timeout.  in this case, terminate
                # the connection without any further waiting.
                # see issue #8419
                self._connection.close()
        else:
            # not in a greenlet; this is the gc cleanup case
            self._connection.close()
        self._started = False

    @staticmethod
    def _default_name_func():
        return None


class AsyncAdaptFallback_aiotrino_connection(AsyncAdapt_aiotrino_connection):
    __slots__ = ()

    await_ = staticmethod(await_fallback)


class AsyncAdapt_aiotrino_dbapi:
    def __init__(self, aiotrino: aiotrino_dbapi):
        self.aiotrino = aiotrino
        self.paramstyle = "qmark"

    def connect(self, *arg, **kw):
        async_fallback = kw.pop("async_fallback", False)
        creator_fn = kw.pop("async_creator_fn", self.aiotrino.connect)

        if util.asbool(async_fallback):
            return AsyncAdaptFallback_aiotrino_connection(
                self,
                creator_fn(*arg, **kw),
            )
        else:
            return AsyncAdapt_aiotrino_connection(
                self,
                creator_fn(*arg, **kw),
            )

    # PEP-249 compliance
    class Error(Exception):
        pass

    class Warning(Exception):
        pass

    class InterfaceError(Error):
        pass

    class DatabaseError(Error):
        pass

    class InternalError(DatabaseError):
        pass

    class OperationalError(DatabaseError):
        pass

    class ProgrammingError(DatabaseError):
        pass

    class IntegrityError(DatabaseError):
        pass

    class DataError(DatabaseError):
        pass

    class NotSupportedError(DatabaseError):
        pass

    Date = datetime.date
    Time = datetime.time
    Timestamp = datetime.datetime
    DateFromTicks = datetime.date.fromtimestamp
    TimestampFromTicks = datetime.datetime.fromtimestamp
    TimeFromTicks = aiotrino_dbapi.TimeFromTicks

    Binary = aiotrino_dbapi.Binary

    STRING = aiotrino_dbapi.STRING
    BINARY = aiotrino_dbapi.BINARY
    NUMBER = aiotrino_dbapi.NUMBER
    DATETIME = aiotrino_dbapi.DATETIME
    ROWID = aiotrino_dbapi.ROWID


class AIOTrinoDialect(DefaultDialect):

    await_ = staticmethod(await_only)

    def __init__(self, json_serializer=None, json_deserializer=None, **kwargs):
        DefaultDialect.__init__(self, **kwargs)
        self._json_serializer = json_serializer
        self._json_deserializer = json_deserializer

    driver = "aiotrino"

    is_async = True

    statement_compiler = compiler.TrinoSQLCompiler
    ddl_compiler = compiler.TrinoDDLCompiler
    type_compiler = compiler.TrinoTypeCompiler
    preparer = compiler.TrinoIdentifierPreparer

    # Data Type
    supports_native_enum = False
    supports_native_boolean = True
    supports_native_decimal = True

    # Column options
    supports_sequences = False
    supports_comments = True
    inline_comments = True
    supports_default_values = False

    # DDL
    supports_alter = True

    # DML
    # Queries of the form `INSERT () VALUES ()` is not supported by Trino.
    supports_empty_insert = False
    supports_multivalues_insert = True
    postfetch_lastrowid = False

    # Caching
    # Warnings are generated by SQLAlchmey if this flag is not explicitly set
    # and tests are needed before being enabled
    supports_statement_cache = False

    # Support proper ordering of CTEs in regard to an INSERT statement
    cte_follows_insert = True
    colspecs = colspecs

    @classmethod
    def dbapi(cls):
        """
        ref: https://www.python.org/dev/peps/pep-0249/#module-interface
        """
        return AsyncAdapt_aiotrino_dbapi(aiotrino_dbapi)

    @classmethod
    def import_dbapi(cls):
        """
        ref: https://www.python.org/dev/peps/pep-0249/#module-interface
        """
        return AsyncAdapt_aiotrino_dbapi(aiotrino_dbapi)

    @classmethod
    def get_pool_class(cls, url):
        async_fallback = url.query.get("async_fallback", False)

        if util.asbool(async_fallback):
            return pool.FallbackAsyncAdaptedQueuePool
        else:
            return pool.AsyncAdaptedQueuePool

    def create_connect_args(self, url: URL) -> Tuple[Sequence[Any], Mapping[str, Any]]:
        args: Sequence[Any] = []
        kwargs: Dict[str, Any] = {"host": url.host}

        if url.port:
            kwargs["port"] = url.port

        db_parts = (url.database or "system").split("/")
        if len(db_parts) == 1:
            kwargs["catalog"] = unquote_plus(db_parts[0])
        elif len(db_parts) == 2:
            kwargs["catalog"] = unquote_plus(db_parts[0])
            kwargs["schema"] = unquote_plus(db_parts[1])
        else:
            raise ValueError(f"Unexpected database format {url.database}")

        if url.username:
            kwargs["user"] = unquote_plus(url.username)

        if url.password:
            if not url.username:
                raise ValueError("Username is required when specify password in connection URL")
            kwargs["auth"] = BasicAuthentication(unquote_plus(url.username), unquote_plus(url.password))

        if "access_token" in url.query:
            kwargs["auth"] = JWTAuthentication(unquote_plus(url.query["access_token"]))

        # if "cert" in url.query and "key" in url.query:
        #     kwargs["auth"] = CertificateAuthentication(unquote_plus(url.query['cert']), unquote_plus(url.query['key']))

        # if "externalAuthentication" in url.query:
        #     kwargs["auth"] = OAuth2Authentication()

        if "source" in url.query:
            kwargs["source"] = unquote_plus(url.query["source"])
        else:
            kwargs["source"] = "aiotrino-sqlalchemy"

        if "session_properties" in url.query:
            kwargs["session_properties"] = json.loads(unquote_plus(url.query["session_properties"]))

        if "http_headers" in url.query:
            kwargs["http_headers"] = json.loads(unquote_plus(url.query["http_headers"]))

        if "extra_credential" in url.query:
            kwargs["extra_credential"] = [
                tuple(extra_credential) for extra_credential in json.loads(unquote_plus(url.query["extra_credential"]))
            ]

        if "client_tags" in url.query:
            kwargs["client_tags"] = json.loads(unquote_plus(url.query["client_tags"]))

        if "legacy_primitive_types" in url.query:
            kwargs["legacy_primitive_types"] = json.loads(unquote_plus(url.query["legacy_primitive_types"]))

        if "legacy_prepared_statements" in url.query:
            kwargs["legacy_prepared_statements"] = json.loads(unquote_plus(url.query["legacy_prepared_statements"]))

        if "verify" in url.query:
            kwargs["verify"] = json.loads(unquote_plus(url.query["verify"]))

        if "roles" in url.query:
            kwargs["roles"] = json.loads(url.query["roles"])

        return args, kwargs

    def get_columns(self, connection: Connection, table_name: str, schema: str = None, **kw) -> List[Dict[str, Any]]:
        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(f"schema={schema}, table={table_name}")
        return self._get_columns(connection, table_name, schema, **kw)

    def _get_columns(self, connection: Connection, table_name: str, schema: str = None, **kw) -> List[Dict[str, Any]]:
        schema = schema or self._get_default_schema_name(connection)
        query = dedent(
            """
            SELECT
                "column_name",
                "data_type",
                "column_default",
                UPPER("is_nullable") AS "is_nullable"
            FROM "information_schema"."columns"
            WHERE "table_schema" = :schema
              AND "table_name" = :table
            ORDER BY "ordinal_position" ASC
        """
        ).strip()
        res = connection.execute(sql.text(query), {"schema": schema, "table": table_name})
        columns = []
        for record in res:
            column = {
                "name": record.column_name,
                "type": datatype.parse_sqltype(record.data_type),
                "nullable": record.is_nullable == "YES",
                "default": record.column_default,
            }
            columns.append(column)
        return columns

    def _get_partitions(self, connection: Connection, table_name: str, schema: str = None) -> List[Dict[str, List[Any]]]:
        schema = schema or self._get_default_schema_name(connection)
        query = dedent(
            f"""
            SELECT * FROM {schema}."{table_name}$partitions"
        """
        ).strip()
        res = connection.execute(sql.text(query))
        partition_names = [desc[0] for desc in await_only(res.cursor.get_description())]
        return partition_names

    def get_pk_constraint(self, connection: Connection, table_name: str, schema: str = None, **kw) -> Dict[str, Any]:
        """Trino has no support for primary keys. Returns a dummy"""
        return {"name": None, "constrained_columns": []}

    def get_primary_keys(self, connection: Connection, table_name: str, schema: str = None, **kw) -> List[str]:
        pk = self.get_pk_constraint(connection, table_name, schema)
        return pk.get("constrained_columns")  # type: ignore

    def get_foreign_keys(self, connection: Connection, table_name: str, schema: str = None, **kw) -> List[Dict[str, Any]]:
        """Trino has no support for foreign keys. Returns an empty list."""
        return []

    def get_catalog_names(self, connection: Connection, **kw) -> List[str]:
        query = dedent(
            """
            SELECT "table_cat"
            FROM "system"."jdbc"."catalogs"
        """
        ).strip()
        res = connection.execute(sql.text(query))
        return [row.table_cat for row in res]

    def get_schema_names(self, connection: Connection, **kw) -> List[str]:
        query = dedent(
            """
            SELECT "schema_name"
            FROM "information_schema"."schemata"
        """
        ).strip()
        res = connection.execute(sql.text(query))
        return [row.schema_name for row in res]

    def get_table_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        schema = schema or self._get_default_schema_name(connection)
        if schema is None:
            raise exc.NoSuchTableError("schema is required")
        query = dedent(
            """
            SELECT "table_name"
            FROM "information_schema"."tables"
            WHERE "table_schema" = :schema
              AND "table_type" = 'BASE TABLE'
        """
        ).strip()
        res = connection.execute(sql.text(query), {"schema": schema})
        return [row.table_name for row in res]

    def get_temp_table_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        """Trino has no support for temporary tables. Returns an empty list."""
        return []

    def get_view_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        schema = schema or self._get_default_schema_name(connection)
        if schema is None:
            raise exc.NoSuchTableError("schema is required")

        # Querying the information_schema.views table is subpar as it compiles the view definitions.
        query = dedent(
            """
            SELECT "table_name"
            FROM "information_schema"."tables"
            WHERE "table_schema" = :schema
              AND "table_type" = 'VIEW'
        """
        ).strip()
        res = connection.execute(sql.text(query), {"schema": schema})
        return [row.table_name for row in res]

    def get_temp_view_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        """Trino has no support for temporary views. Returns an empty list."""
        return []

    def get_view_definition(self, connection: Connection, view_name: str, schema: str = None, **kw) -> str:
        schema = schema or self._get_default_schema_name(connection)
        if schema is None:
            raise exc.NoSuchTableError("schema is required")
        query = dedent(
            """
            SELECT "view_definition"
            FROM "information_schema"."views"
            WHERE "table_schema" = :schema
              AND "table_name" = :view
        """
        ).strip()
        res = connection.execute(sql.text(query), {"schema": schema, "view": view_name})
        return res.scalar()

    def get_indexes(self, connection: Connection, table_name: str, schema: str = None, **kw) -> List[Dict[str, Any]]:
        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(f"schema={schema}, table={table_name}")

        partitioned_columns = None
        try:
            partitioned_columns = self._get_partitions(connection, f"{table_name}", schema)
        except Exception as e:
            # e.g. it's not a Hive table or an unpartitioned Hive table
            logger.debug("Couldn't fetch partition columns. schema: %s, table: %s, error: %s", schema, table_name, e)
        if not partitioned_columns:
            return []
        partition_index = {"name": "partition", "column_names": partitioned_columns, "unique": False}
        return [partition_index]

    def get_sequence_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        """Trino has no support for sequences. Returns an empty list."""
        return []

    def get_unique_constraints(self, connection: Connection, table_name: str, schema: str = None, **kw) -> List[Dict[str, Any]]:
        """Trino has no support for unique constraints. Returns an empty list."""
        return []

    def get_check_constraints(self, connection: Connection, table_name: str, schema: str = None, **kw) -> List[Dict[str, Any]]:
        """Trino has no support for check constraints. Returns an empty list."""
        return []

    def get_table_comment(self, connection: Connection, table_name: str, schema: str = None, **kw) -> Dict[str, Any]:
        catalog_name = self._get_default_catalog_name(connection)
        if catalog_name is None:
            raise exc.NoSuchTableError("catalog is required in connection")
        schema_name = schema or self._get_default_schema_name(connection)
        if schema_name is None:
            raise exc.NoSuchTableError("schema is required")
        query = dedent(
            """
            SELECT "comment"
            FROM "system"."metadata"."table_comments"
            WHERE "catalog_name" = :catalog_name
              AND "schema_name" = :schema_name
              AND "table_name" = :table_name
        """
        ).strip()
        try:
            res = connection.execute(
                sql.text(query), {"catalog_name": catalog_name, "schema_name": schema_name, "table_name": table_name}
            )
            return {"text": res.scalar()}
        except error.TrinoQueryError as e:
            if e.error_name in (error.PERMISSION_DENIED,):
                return {"text": None}
            raise

    def has_schema(self, connection: Connection, schema: str) -> bool:
        query = dedent(
            """
            SELECT "schema_name"
            FROM "information_schema"."schemata"
            WHERE "schema_name" = :schema
        """
        ).strip()
        res = connection.execute(sql.text(query), {"schema": schema})
        return res.first() is not None

    def has_table(self, connection: Connection, table_name: str, schema: str = None, **kw) -> bool:
        schema = schema or self._get_default_schema_name(connection)
        if schema is None:
            return False
        query = dedent(
            """
            SELECT "table_name"
            FROM "information_schema"."tables"
            WHERE "table_schema" = :schema
              AND "table_name" = :table
        """
        ).strip()
        res = connection.execute(sql.text(query), {"schema": schema, "table": table_name})
        return res.first() is not None

    def has_sequence(self, connection: Connection, sequence_name: str, schema: str = None, **kw) -> bool:
        """Trino has no support for sequence. Returns False indicate that given sequence does not exists."""
        return False

    ## TODO: No idea how to do this async. May need to just fire a manual http request?
    # @classmethod
    # def _get_server_version_info(cls, connection: Connection) -> Any:
    #     def get_server_version_info(_):
    #         query = "SELECT version()"
    #         try:
    #             res = connection.execute(sql.text(query))
    #             version = res.scalar()
    #             return (version,)
    #         except exc.ProgrammingError as e:
    #             logger.debug(f"Failed to get server version: {e.orig.message}")
    #             return None

    #     # Make server_version_info lazy in order to only make HTTP calls if user explicitly requests it.
    #     cls.server_version_info = property(get_server_version_info, lambda instance, value: None)

    def _raw_connection(self, connection: Union[Engine, Connection]) -> aiotrino_dbapi.Connection:
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def _get_default_catalog_name(self, connection: Connection) -> Optional[str]:
        dbapi_connection: AsyncAdapt_aiotrino_connection = self._raw_connection(connection)
        return dbapi_connection._connection.catalog

    def _get_default_schema_name(self, connection: Connection) -> Optional[str]:
        dbapi_connection: AsyncAdapt_aiotrino_connection = self._raw_connection(connection)
        return dbapi_connection._connection.schema

    def set_isolation_level(self, dbapi_conn: AsyncAdapt_aiotrino_connection, level: str) -> None:
        dbapi_conn._isolation_level = aiotrino_dbapi.IsolationLevel[level]

    def get_isolation_level(self, dbapi_conn: AsyncAdapt_aiotrino_connection) -> str:
        return dbapi_conn.isolation_level.name

    def get_default_isolation_level(self, dbapi_conn: AsyncAdapt_aiotrino_connection) -> str:
        return aiotrino_dbapi.IsolationLevel.AUTOCOMMIT.name

    def _get_full_table(self, table_name: str, schema: str = None, quote: bool = True) -> str:
        table_part = self.identifier_preparer.quote_identifier(table_name) if quote else table_name
        if schema:
            schema_part = self.identifier_preparer.quote_identifier(schema) if quote else schema
            return f"{schema_part}.{table_part}"

        return table_part
