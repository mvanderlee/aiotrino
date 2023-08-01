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

import datetime
import math
import uuid
from typing import Any, List, Optional  # NOQA for mypy types

import aiohttp

import aiotrino.client
import aiotrino.exceptions
import aiotrino.logging
from aiotrino import constants
from aiotrino.transaction import NO_TRANSACTION, IsolationLevel, Transaction
from aiotrino.utils import aiter, anext

__all__ = ["connect", "Connection", "Cursor"]


apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"

logger = aiotrino.logging.get_logger(__name__)


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database.

    See class :py:class:`Connection` for arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    """Trino supports transactions and the ability to either commit or rollback
    a sequence of SQL statements. A single query i.e. the execution of a SQL
    statement, can also be cancelled. Transactions are not supported by this
    client implementation yet.

    """

    def __init__(
        self,
        host,
        port=constants.DEFAULT_PORT,
        user=None,
        source=constants.DEFAULT_SOURCE,
        catalog=constants.DEFAULT_CATALOG,
        schema=constants.DEFAULT_SCHEMA,
        session_properties=None,
        http_headers=None,
        http_scheme=constants.HTTP,
        auth=constants.DEFAULT_AUTH,
        redirect_handler=None,
        max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
        isolation_level=IsolationLevel.AUTOCOMMIT,
        verify=True
    ):
        self.host = host
        self.port = port
        self.user = user
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.session_properties = session_properties
        # mypy cannot follow module import
        self._http_session = None
        self.http_headers = http_headers
        self.http_scheme = http_scheme
        self.auth = auth
        self.redirect_handler = redirect_handler
        self.max_attempts = max_attempts
        self.request_timeout = request_timeout

        self._isolation_level = isolation_level
        self._request = None
        self._transaction = None
        self._verify_ssl = verify

    @property
    def isolation_level(self):
        return self._isolation_level

    @property
    def transaction(self):
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
            self._http_session = aiohttp.ClientSession(
                connector=aiotrino.client.TrinoTCPConnector(verify_ssl=self._verify_ssl)
            )  # type: ignore

        return aiotrino.client.TrinoRequest(
            self.host,
            self.port,
            self.user,
            self.source,
            self.catalog,
            self.schema,
            self.session_properties,
            self._http_session,
            self.http_headers,
            NO_TRANSACTION,
            self.http_scheme,
            self.auth,
            self.redirect_handler,
            self.max_attempts,
            self.request_timeout,
        )

    async def cursor(self):
        """Return a new :py:class:`Cursor` object using the connection."""
        if self.isolation_level != IsolationLevel.AUTOCOMMIT:
            if self.transaction is None:
                await self.start_transaction()
            request = self.transaction._request
        else:
            request = self._create_request()
        return Cursor(self, request)


class Cursor(object):
    """Database cursor.

    Cursors are not isolated, i.e., any changes done to the database by a
    cursor are immediately visible by other cursors or connections.

    """

    def __init__(self, connection: Connection, request: aiotrino.client.TrinoRequest):
        if not isinstance(connection, Connection):
            raise ValueError(
                "connection must be a Connection object: {}".format(type(connection))
            )
        self._connection = connection
        self._request = request

        self.arraysize = 1
        self._iterator = None
        self._query = None

    def __aiter__(self):
        return self._iterator

    @property
    def connection(self):
        return self._connection

    @property
    def description(self):
        if self._query.columns is None:
            return None

        # [ (name, type_code, display_size, internal_size, precision, scale, null_ok) ]
        return [
            (col["name"], col["type"], None, None, None, None, None)
            for col in self._query.columns
        ]

    @property
    def rowcount(self):
        """Not supported.

        Trino cannot reliablity determine the number of rows returned by an
        operation. For example, the result of a SELECT query is streamed and
        the number of rows is only knowns when all rows have been retrieved.
        """

        return -1

    @property
    def stats(self):
        if self._query is not None:
            return self._query.stats
        return None

    @property
    def warnings(self):
        if self._query is not None:
            return self._query.warnings
        return None

    def setinputsizes(self, sizes):
        raise aiotrino.exceptions.NotSupportedError

    def setoutputsize(self, size, column):
        raise aiotrino.exceptions.NotSupportedError

    async def _prepare_statement(self, operation, statement_name):
        """
        Prepends the given `operation` with "PREPARE <statement_name> FROM" and
        executes as a prepare statement.

        :param operation: sql to be executed.
        :param statement_name: name that will be assigned to the prepare
            statement.

        :raises aiotrino.exceptions.FailedToObtainAddedPrepareHeader: Error raised
            when unable to find the 'X-Trino-Added-Prepare' for the PREPARE
            statement request.

        :return: string representing the value of the 'X-Trino-Added-Prepare'
            header.
        """
        sql = 'PREPARE {statement_name} FROM {operation}'.format(
            statement_name=statement_name,
            operation=operation
        )

        # Send prepare statement. Copy the _request object to avoid poluting the
        # one that is going to be used to execute the actual operation.
        query = aiotrino.client.TrinoQuery(self._request.clone(), sql=sql)
        result = await query.execute()

        # Iterate until the 'X-Trino-Added-Prepare' header is found or
        # until there are no more results
        async for _ in result:
            response_headers = result.response_headers

            if constants.HEADERS.ADDED_PREPARE in response_headers:
                return response_headers[constants.HEADERS.ADDED_PREPARE]
        else:
            # 406 no longer returns results so there is nothing to loop over.
            response_headers = result.response_headers
            if constants.HEADERS.ADDED_PREPARE in response_headers:
                return response_headers[constants.HEADERS.ADDED_PREPARE]

        raise aiotrino.exceptions.FailedToObtainAddedPrepareHeader

    def _get_added_prepare_statement_trino_query(
        self,
        statement_name,
        params
    ):
        sql = 'EXECUTE ' + statement_name + ' USING ' + ','.join(map(self._format_prepared_param, params))

        # No need to deepcopy _request here because this is the actual request
        # operation
        return aiotrino.client.TrinoQuery(self._request, sql=sql)

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
            return ("'%s'" % param.replace("'", "''"))

        if isinstance(param, bytes):
            return "X'%s'" % param.hex()

        if isinstance(param, datetime.datetime):
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f %Z")
            # strip trailing whitespace if param has no zone
            datetime_str = datetime_str.rstrip(" ")
            return "TIMESTAMP '%s'" % datetime_str

        if isinstance(param, list):
            return "ARRAY[%s]" % ','.join(map(self._format_prepared_param, param))

        if isinstance(param, dict):
            keys = list(param.keys())
            values = [param[key] for key in keys]
            return "MAP(%s, %s)" % (
                self._format_prepared_param(keys),
                self._format_prepared_param(values)
            )

        if isinstance(param, uuid.UUID):
            return "UUID '%s'" % param

        raise aiotrino.exceptions.NotSupportedError("Query parameter of type '%s' is not supported." % type(param))

    async def _deallocate_prepare_statement(self, added_prepare_header, statement_name):
        sql = 'DEALLOCATE PREPARE ' + statement_name

        # Send deallocate statement. Copy the _request object to avoid poluting the
        # one that is going to be used to execute the actual operation.
        query = aiotrino.client.TrinoQuery(self._request.clone(), sql=sql)
        result = await query.execute(
            additional_http_headers={
                constants.HEADERS.PREPARED_STATEMENT: added_prepare_header
            }
        )

        # Iterate until the 'X-Trino-Deallocated-Prepare' header is found or
        # until there are no more results
        async for _ in result:
            response_headers = result.response_headers

            if constants.HEADERS.DEALLOCATED_PREPARE in response_headers:
                return response_headers[constants.HEADERS.DEALLOCATED_PREPARE]
        else:
            # 406 no longer returns results so there is nothing to loop over.
            response_headers = result.response_headers
            if constants.HEADERS.DEALLOCATED_PREPARE in response_headers:
                return response_headers[constants.HEADERS.DEALLOCATED_PREPARE]

        raise aiotrino.exceptions.FailedToObtainDeallocatedPrepareHeader

    def _generate_unique_statement_name(self):
        return 'st_' + uuid.uuid4().hex.replace('-', '')

    async def execute(self, operation, params=None):
        if params:
            assert isinstance(params, (list, tuple)), (
                'params must be a list or tuple containing the query '
                'parameter values'
            )

            statement_name = self._generate_unique_statement_name()
            # Send prepare statement
            added_prepare_header = await self._prepare_statement(
                operation, statement_name
            )

            try:
                # Send execute statement and assign the return value to `results`
                # as it will be returned by the function
                self._query = self._get_added_prepare_statement_trino_query(
                    statement_name, params
                )
                result = await self._query.execute(
                    additional_http_headers={
                        constants.HEADERS.PREPARED_STATEMENT: added_prepare_header
                    }
                )
            finally:
                # Send deallocate statement
                # At this point the query can be deallocated since it has already
                # been executed
                await self._deallocate_prepare_statement(added_prepare_header, statement_name)

        else:
            self._query = aiotrino.client.TrinoQuery(self._request, sql=operation)
            result = await self._query.execute()
        self._iterator = aiter(result)
        return result

    def executemany(self, operation, seq_of_params):
        raise aiotrino.exceptions.NotSupportedError

    async def fetchone(self):
        # type: () -> Optional[List[Any]]
        """

        PEP-0249: Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        try:
            return await anext(self._iterator)
        except StopAsyncIteration:
            return None
        except aiotrino.exceptions.HttpError as err:
            raise aiotrino.exceptions.OperationalError(str(err))

    async def fetchmany(self, size=None):
        # type: (Optional[int]) -> List[List[Any]]
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

        result = []
        for _ in range(size):
            row = await self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def genall(self):
        return self._query.result

    async def fetchall(self):
        # type: () -> List[List[Any]]
        return [row async for row in self.genall()]

    async def cancel(self):
        if self._query is None:
            raise aiotrino.exceptions.OperationalError(
                "Cancel query failed; no running query"
            )
        await self._query.cancel()

    async def close(self):
        await self._request.close()
        await self._connection.close()


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

BINARY = DBAPITypeObject(
    "ARRAY", "MAP", "ROW", "HyperLogLog", "P4HyperLogLog", "QDigest"
)

NUMBER = DBAPITypeObject(
    "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL"
)

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
