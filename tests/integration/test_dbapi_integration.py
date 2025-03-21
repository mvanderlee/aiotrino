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
import math
import sys
import time as t
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import AsyncGenerator, Tuple
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
import requests
from tzlocal import get_localzone_name  # type: ignore

import aiotrino
from aiotrino import constants
from aiotrino.client import SegmentIterator
from aiotrino.dbapi import Connection, Cursor, DescribeOutput, TimeBoundLRUCache
from aiotrino.exceptions import NotSupportedError, TrinoQueryError, TrinoUserError
from aiotrino.mapper import RowMapperFactory
from aiotrino.transaction import IsolationLevel
from tests.integration.conftest import trino_version


@pytest_asyncio.fixture(params=[None, "json+zstd", "json+lz4", "json"], loop_scope="session")
async def trino_connection(request, run_trino) -> AsyncGenerator[Connection, None,]:
    host, port = run_trino
    encoding = request.param

    conn = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", source="test", max_attempts=1, encoding=encoding
    )
    yield conn
    await conn.close()


@pytest_asyncio.fixture(loop_scope="session")
async def trino_connection_with_transaction(run_trino) -> AsyncGenerator[Connection, None]:
    host, port = run_trino

    conn =  aiotrino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        max_attempts=1,
        isolation_level=IsolationLevel.READ_UNCOMMITTED,
    )
    yield conn
    await conn.close()


@pytest_asyncio.fixture(loop_scope="session")
async def trino_connection_in_autocommit(run_trino) -> AsyncGenerator[Connection, None]:
    host, port = run_trino

    conn = aiotrino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        max_attempts=1,
        isolation_level=IsolationLevel.AUTOCOMMIT,
    )
    yield conn
    await conn.close()


@pytest_asyncio.fixture(loop_scope="session")
async def trino_connection_with_legacy_prepared_statements(legacy_prepared_statements, run_trino) -> AsyncGenerator[Connection, None]:
    host, port = run_trino

    conn = aiotrino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        legacy_prepared_statements=legacy_prepared_statements,
    )
    yield conn
    await conn.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_select_query(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT * FROM system.runtime.nodes")
        rows = await cur.fetchall()
        assert len(rows) > 0
        row = rows[0]
        if trino_version() == sys.maxsize:
            assert row[2] is not None
        else:
            assert row[2] == str(trino_version())
        columns = dict([desc[:2] for desc in await cur.get_description()])
        assert columns["node_id"] == "varchar"
        assert columns["http_uri"] == "varchar"
        assert columns["node_version"] == "varchar"
        assert columns["coordinator"] == "boolean"
        assert columns["state"] == "varchar"
        assert cur.query_id is not None
        assert cur.query == "SELECT * FROM system.runtime.nodes"
        assert cur.stats is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_select_query_result_iteration(trino_connection: Connection):
    async with await trino_connection.cursor() as cur0:
        await cur0.execute("SELECT custkey FROM tpch.sf1.customer LIMIT 10")
        rows0 = [row async for row in cur0.genall()]

    async with await trino_connection.cursor() as cur1:
        await cur1.execute("SELECT custkey FROM tpch.sf1.customer LIMIT 10")
        rows1 = await cur1.fetchall()

    assert len(rows0) == len(rows1)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_select_query_result_iteration_statement_params(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute(
            """
            SELECT * FROM (
                values
                (1, 'one', 'a'),
                (2, 'two', 'b'),
                (3, 'three', 'c'),
                (4, 'four', 'd'),
                (5, 'five', 'e')
            ) x (id, name, letter)
            WHERE id >= ?
            """,
            params=(3,)  # expecting all the rows with id >= 3
        )
        rows = await cur.fetchall()
        assert len(rows) == 3
        assert [3, 'three', 'c'] in rows
        assert [4, 'four', 'd'] in rows
        assert [5, 'five', 'e'] in rows


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_none_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(None,))
        rows = await cur.fetchall()

        assert rows[0][0] is None
        await assert_cursor_description(cur, trino_type="unknown")


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_string_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=("six'",))
        rows = await cur.fetchall()

        assert rows[0][0] == "six'"
        await assert_cursor_description(cur, trino_type="varchar(4)", size=4)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_execute_many(trino_connection_with_legacy_prepared_statements: Connection):
    try:
        async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
            await cur.execute("CREATE TABLE memory.default.test_execute_many (key int, value varchar)")
            await cur.fetchall()
            operation = "INSERT INTO memory.default.test_execute_many (key, value) VALUES (?, ?)"
            await cur.executemany(operation, [(1, "value1")])
            await cur.fetchall()
            await cur.execute("SELECT * FROM memory.default.test_execute_many ORDER BY key")
            rows = await cur.fetchall()
            assert len(list(rows)) == 1
            assert rows[0] == [1, "value1"]

            operation = "INSERT INTO memory.default.test_execute_many (key, value) VALUES (?, ?)"
            await cur.executemany(operation, [(2, "value2"), (3, "value3")])
            await cur.fetchall()

            await cur.execute("SELECT * FROM memory.default.test_execute_many ORDER BY key")
            rows = await cur.fetchall()
            assert len(list(rows)) == 3
            assert rows[0] == [1, "value1"]
            assert rows[1] == [2, "value2"]
            assert rows[2] == [3, "value3"]
    finally:
        async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS memory.default.test_execute_many")


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_execute_many_without_params(trino_connection_with_legacy_prepared_statements: Connection):
    try:
        async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
            await cur.execute("CREATE TABLE memory.default.test_execute_many_without_param (value varchar)")
            await cur.fetchall()
            with pytest.raises(TrinoUserError) as e:
                await cur.executemany("INSERT INTO memory.default.test_execute_many_without_param (value) VALUES (?)", [])
                await cur.fetchall()
            assert "Incorrect number of parameters: expected 1 but found 0" in str(e.value)
    finally:
        async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS memory.default.test_execute_many_without_param")


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_execute_many_select(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        with pytest.raises(NotSupportedError) as e:
            await cur.executemany("SELECT ?, ?", [(1, "value1"), (2, "value2")])
        assert "Query must return update type" in str(e.value)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("connection_legacy_primitive_types", [None, True, False])
@pytest.mark.parametrize("cursor_legacy_primitive_types", [None, True, False])
async def test_legacy_primitive_types_with_connection_and_cursor(
        connection_legacy_primitive_types,
        cursor_legacy_primitive_types,
        run_trino
):
    host, port = run_trino

    connection = aiotrino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        legacy_primitive_types=connection_legacy_primitive_types,
    )

    cur = await connection.cursor(legacy_primitive_types=cursor_legacy_primitive_types)

    # If legacy_primitive_types is passed to cursor, take value from it.
    # If not, take value from legacy_primitive_types passed to connection.
    # If legacy_primitive_types is not passed to cursor nor connection, default to False.
    if cursor_legacy_primitive_types is not None:
        expected_legacy_primitive_types = cursor_legacy_primitive_types
    elif connection_legacy_primitive_types is not None:
        expected_legacy_primitive_types = connection_legacy_primitive_types
    else:
        expected_legacy_primitive_types = False

    test_query = """
    SELECT
        DECIMAL '0.142857',
        DATE '2018-01-01',
        TIMESTAMP '2019-01-01 00:00:00.000+01:00',
        TIMESTAMP '2019-01-01 00:00:00.000 UTC',
        TIMESTAMP '2019-01-01 00:00:00.000',
        TIME '00:00:00.000'
    """
    # Check values which cannot be represented by Python types
    if expected_legacy_primitive_types:
        test_query += """
        ,DATE '-2001-08-22'
        """
    await cur.execute(test_query)
    rows = await cur.fetchall()

    if not expected_legacy_primitive_types:
        assert len(rows[0]) == 6
        assert rows[0][0] == Decimal('0.142857')
        assert rows[0][1] == date(2018, 1, 1)
        assert rows[0][2] == datetime(2019, 1, 1, tzinfo=timezone(timedelta(hours=1)))
        assert rows[0][3] == datetime(2019, 1, 1, tzinfo=ZoneInfo('UTC'))
        assert rows[0][4] == datetime(2019, 1, 1)
        assert rows[0][5] == time(0, 0, 0, 0)
    else:
        for value in rows[0]:
            assert isinstance(value, str)

        assert len(rows[0]) == 7
        assert rows[0][0] == '0.142857'
        assert rows[0][1] == '2018-01-01'
        assert rows[0][2] == '2019-01-01 00:00:00.000 +01:00'
        assert rows[0][3] == '2019-01-01 00:00:00.000 UTC'
        assert rows[0][4] == '2019-01-01 00:00:00.000'
        assert rows[0][5] == '00:00:00.000'
        assert rows[0][6] == '-2001-08-22'


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_decimal_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(Decimal('1112.142857'),))
        rows = await cur.fetchall()

        assert rows[0][0] == Decimal('1112.142857')
        await assert_cursor_description(cur, trino_type="decimal(10, 6)", precision=10, scale=6)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_decimal_scientific_notation_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(Decimal('0E-10'),))
        rows = await cur.fetchall()

        assert rows[0][0] == Decimal('0E-10')
        await assert_cursor_description(cur, trino_type="decimal(10, 10)", precision=10, scale=10)

        # Ensure we don't convert to floats
        assert Decimal('0.1') == Decimal('1E-1') != 0.1

        await cur.execute("SELECT ?", params=(Decimal('1E-1'),))
        rows = await cur.fetchall()

        assert rows[0][0] == Decimal('1E-1')
        await assert_cursor_description(cur, trino_type="decimal(1, 1)", precision=1, scale=1)


@pytest.mark.asyncio(loop_scope="session")
async def test_null_decimal(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CAST(NULL AS DECIMAL)")
        rows = await cur.fetchall()

        assert rows[0][0] is None
        await assert_cursor_description(cur, trino_type="decimal(38, 0)", precision=38, scale=0)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_biggest_decimal(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = Decimal('99999999999999999999999999999999999999')
        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="decimal(38, 0)", precision=38, scale=0)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_smallest_decimal(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = Decimal('-99999999999999999999999999999999999999')
        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="decimal(38, 0)", precision=38, scale=0)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_highest_precision_decimal(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = Decimal('0.99999999999999999999999999999999999999')
        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="decimal(38, 38)", precision=38, scale=38)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_datetime_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = datetime(2020, 1, 1, 16, 43, 22, 320000)

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="timestamp(6)", precision=6)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_datetime_with_utc_time_zone_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=ZoneInfo('UTC'))

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="timestamp(6) with time zone", precision=6)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_datetime_with_numeric_offset_time_zone_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        tz = timezone(-timedelta(hours=5, minutes=30))

        params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=tz)

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="timestamp(6) with time zone", precision=6)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_datetime_with_named_time_zone_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=ZoneInfo('America/Los_Angeles'))

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="timestamp(6) with time zone", precision=6)


@pytest.mark.asyncio(loop_scope="session")
async def test_datetime_with_trailing_zeros(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT TIMESTAMP '2001-08-22 03:04:05.321000'")
        rows = await cur.fetchall()

        assert rows[0][0] == datetime.strptime("2001-08-22 03:04:05.321000", "%Y-%m-%d %H:%M:%S.%f")
        await assert_cursor_description(cur, trino_type="timestamp(6)", precision=6)


@pytest.mark.asyncio(loop_scope="session")
async def test_null_datetime_with_time_zone(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CAST(NULL AS TIMESTAMP WITH TIME ZONE)")
        rows = await cur.fetchall()

        assert rows[0][0] is None
        await assert_cursor_description(cur, trino_type="timestamp(3) with time zone", precision=3)


@pytest.mark.asyncio(loop_scope="session")
async def test_datetime_with_time_zone_numeric_offset(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT TIMESTAMP '2001-08-22 03:04:05.321 -08:00'")
        rows = await cur.fetchall()

        assert rows[0][0] == datetime.strptime("2001-08-22 03:04:05.321 -08:00", "%Y-%m-%d %H:%M:%S.%f %z")
        await assert_cursor_description(cur, trino_type="timestamp(3) with time zone", precision=3)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_datetimes_with_time_zone_in_dst_gap_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        # This is a datetime that lies within a DST transition and not actually exists.
        params = datetime(2021, 3, 28, 2, 30, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        with pytest.raises(aiotrino.exceptions.TrinoUserError):
            await cur.execute("SELECT ?", params=(params,))
            await cur.fetchall()


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
@pytest.mark.parametrize('fold', [0, 1])
async def test_doubled_datetimes(fold, trino_connection_with_legacy_prepared_statements: Connection):
    # Trino doesn't distinguish between doubled datetimes that lie within a DST transition.
    # See also https://github.com/trinodb/trino/issues/5781
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = datetime(2002, 10, 27, 1, 30, 0, tzinfo=ZoneInfo('US/Eastern'), fold=fold)

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == datetime(2002, 10, 27, 1, 30, 0, tzinfo=ZoneInfo('US/Eastern'))


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_date_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = datetime(2020, 1, 1, 0, 0, 0).date()

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="date")


@pytest.mark.asyncio(loop_scope="session")
async def test_null_date(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CAST(NULL AS DATE)")
        rows = await cur.fetchall()

        assert rows[0][0] is None
        await assert_cursor_description(cur, trino_type="date")


@pytest.mark.asyncio(loop_scope="session")
async def test_unsupported_python_dates(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        # dates below python min (1-1-1) or above max date (9999-12-31) are not supported
        for unsupported_date in [
            '-0001-01-01',
            '0000-01-01',
            '10000-01-01',
            '-4999999-01-01',  # Trino min date
            '5000000-12-31',  # Trino max date
        ]:
            with pytest.raises(aiotrino.exceptions.TrinoDataError):
                await cur.execute(f"SELECT DATE '{unsupported_date}'")
                await cur.fetchall()


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_supported_special_dates_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        for params in (
            # min python date
            date(1, 1, 1),
            # before julian->gregorian switch
            date(1500, 1, 1),
            # During julian->gregorian switch
            date(1752, 9, 4),
            # before epoch
            date(1952, 4, 3),
            date(1970, 1, 1),
            date(1970, 2, 3),
            # summer on northern hemisphere (possible DST)
            date(2017, 7, 1),
            # winter on northern hemisphere (possible DST on southern hemisphere)
            date(2017, 1, 1),
            # winter on southern hemisphere (possible DST on northern hemisphere)
            date(2017, 12, 31),
            date(1983, 4, 1),
            date(1983, 10, 1),
            # max python date
            date(9999, 12, 31),
        ):
            await cur.execute("SELECT ?", params=(params,))
            rows = await cur.fetchall()

            assert rows[0][0] == params


@pytest.mark.asyncio(loop_scope="session")
async def test_char(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CHAR 'trino'")
        rows = await cur.fetchall()

        assert rows[0][0] == 'trino'
        await assert_cursor_description(cur, trino_type="char(5)", size=5)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_time_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = time(12, 3, 44, 333000)

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params
        await assert_cursor_description(cur, trino_type="time(6)", precision=6)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_time_with_named_time_zone_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = time(16, 43, 22, 320000, tzinfo=ZoneInfo('Asia/Shanghai'))

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        # Asia/Shanghai
        assert rows[0][0].tzinfo == timezone(timedelta(seconds=28800))


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_time_with_numeric_offset_time_zone_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        tz = timezone(-timedelta(hours=8, minutes=0))
        params = time(16, 43, 22, 320000, tzinfo=tz)

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params


@pytest.mark.asyncio(loop_scope="session")
async def test_time(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT TIME '01:02:03.456'")
        rows = await cur.fetchall()

        assert rows[0][0] == time(1, 2, 3, 456000)
        await assert_cursor_description(cur, trino_type="time(3)", precision=3)


@pytest.mark.asyncio(loop_scope="session")
async def test_null_time(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CAST(NULL AS TIME)")
        rows = await cur.fetchall()

        assert rows[0][0] is None
        await assert_cursor_description(cur, trino_type="time(3)", precision=3)


@pytest.mark.asyncio(loop_scope="session")
async def test_time_with_time_zone_negative_offset(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT TIME '01:02:03.456 -08:00'")
        rows = await cur.fetchall()

        tz = timezone(-timedelta(hours=8, minutes=0))

        assert rows[0][0] == time(1, 2, 3, 456000, tzinfo=tz)
        await assert_cursor_description(cur, trino_type="time(3) with time zone", precision=3)


@pytest.mark.asyncio(loop_scope="session")
async def test_time_with_time_zone_positive_offset(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT TIME '01:02:03.456 +08:00'")
        rows = await cur.fetchall()

        tz = timezone(timedelta(hours=8, minutes=0))

        assert rows[0][0] == time(1, 2, 3, 456000, tzinfo=tz)
        await assert_cursor_description(cur, trino_type="time(3) with time zone", precision=3)


@pytest.mark.asyncio(loop_scope="session")
async def test_null_date_with_time_zone(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CAST(NULL AS TIME WITH TIME ZONE)")
        rows = await cur.fetchall()

        assert rows[0][0] is None
        await assert_cursor_description(cur, trino_type="time(3) with time zone", precision=3)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
@pytest.mark.parametrize(
    "binary_input",
    [
        bytearray("a", "utf-8"),
        bytearray("a", "ascii"),
        bytearray(b'\x00\x00\x00\x00'),
        bytearray(4),
        bytearray([1, 2, 3]),
    ],
)
async def test_binary_query_param(binary_input, trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(binary_input,))
        rows = await cur.fetchall()

        assert rows[0][0] == binary_input


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_array_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=([1, 2, 3],))
        rows = await cur.fetchall()

        assert rows[0][0] == [1, 2, 3]

        await cur.execute("SELECT ?", params=([[1, 2, 3], [4, 5, 6]],))
        rows = await cur.fetchall()

        assert rows[0][0] == [[1, 2, 3], [4, 5, 6]]

        await cur.execute("SELECT TYPEOF(?)", params=([1, 2, 3],))
        rows = await cur.fetchall()

        assert rows[0][0] == "array(integer)"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_array_none_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = [None, None]

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params

        await cur.execute("SELECT TYPEOF(?)", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == "array(unknown)"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_array_none_and_another_type_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = [None, 1]

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params

        await cur.execute("SELECT TYPEOF(?)", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == "array(integer)"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_array_timestamp_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = [datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 2, 0, 0, 0)]

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params

        await cur.execute("SELECT TYPEOF(?)", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == "array(timestamp(6))"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_array_timestamp_with_timezone_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = [
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=ZoneInfo('UTC')),
            datetime(2020, 1, 2, 0, 0, 0, tzinfo=ZoneInfo('UTC')),
        ]

        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params

        await cur.execute("SELECT TYPEOF(?)", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == "array(timestamp(6) with time zone)"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_dict_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=({"foo": "bar"},))
        rows = await cur.fetchall()

        assert rows[0][0] == {"foo": "bar"}

        await cur.execute("SELECT TYPEOF(?)", params=({"foo": "bar"},))
        rows = await cur.fetchall()

        assert rows[0][0] == "map(varchar(3), varchar(3))"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_dict_timestamp_query_param_types(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = {"foo": datetime(2020, 1, 1, 16, 43, 22, 320000)}
        await cur.execute("SELECT ?", params=(params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_boolean_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(True,))
        rows = await cur.fetchall()

        assert rows[0][0] is True

        await cur.execute("SELECT ?", params=(False,))
        rows = await cur.fetchall()

        assert rows[0][0] is False


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_row(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = (1, Decimal("2.0"), datetime(2020, 1, 1, 0, 0, 0))
        await cur.execute("SELECT ?", (params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_nested_row(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        params = ((1, "test", Decimal("3.1")), Decimal("2.0"), datetime(2020, 1, 1, 0, 0, 0))
        await cur.execute("SELECT ?", (params,))
        rows = await cur.fetchall()

        assert rows[0][0] == params


@pytest.mark.asyncio(loop_scope="session")
async def test_named_row(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE))")
        rows = await cur.fetchall()

    assert rows[0][0] == (1, 2.0)
    assert rows[0][0][0] == 1
    assert rows[0][0][1] == 2.0
    assert rows[0][0].x == 1
    assert rows[0][0].y == 2.0

    assert rows[0][0].__annotations__["names"] == ['x', 'y']
    assert rows[0][0].__annotations__["types"] == ['bigint', 'double']


@pytest.mark.asyncio(loop_scope="session")
async def test_named_row_duplicate_names(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CAST(ROW(1, 2e0) AS ROW(x BIGINT, x DOUBLE))")
        rows = await cur.fetchall()

    assert rows[0][0] == (1, 2.0)
    with pytest.raises(ValueError, match="Ambiguous row field reference: x"):
        rows[0][0].x

    assert rows[0][0].__annotations__["names"] == ['x', 'x']
    assert rows[0][0].__annotations__["types"] == ['bigint', 'double']
    assert str(rows[0][0]) == "(1, 2.0)"


@pytest.mark.asyncio(loop_scope="session")
async def test_nested_named_row(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT CAST(ROW(DECIMAL '2.3', ROW(1, 'test')) AS ROW(x DECIMAL(3,2), y ROW(x BIGINT, y VARCHAR)))")
        rows = await cur.fetchall()

    assert rows[0][0] == (Decimal('2.3'), (1, 'test'))
    assert rows[0][0][0] == Decimal('2.3')
    assert rows[0][0][1] == (1, 'test')
    assert rows[0][0][1][0] == 1
    assert rows[0][0][1][1] == 'test'
    assert rows[0][0].x == Decimal('2.3')
    assert rows[0][0].y.x == 1
    assert rows[0][0].y.y == 'test'

    assert rows[0][0].__annotations__["names"] == ['x', 'y']
    assert rows[0][0].__annotations__["types"] == ['decimal', 'row']

    assert rows[0][0].y.__annotations__["names"] == ['x', 'y']
    assert rows[0][0].y.__annotations__["types"] == ['bigint', 'varchar']
    assert str(rows[0][0]) == "(x: Decimal('2.30'), y: (x: 1, y: 'test'))"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_float_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(1.1,))
        rows = await cur.fetchall()

        await assert_cursor_description(cur, trino_type="double")
        assert rows[0][0] == 1.1


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_float_nan_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(float("nan"),))
        rows = await cur.fetchall()

        await assert_cursor_description(cur, trino_type="double")
        assert isinstance(rows[0][0], float)
        assert math.isnan(rows[0][0])


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_float_inf_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(float("inf"),))
        rows = await cur.fetchall()

        await assert_cursor_description(cur, trino_type="double")
        assert rows[0][0] == float("inf")

        await cur.execute("SELECT ?", params=(float("-inf"),))
        rows = await cur.fetchall()

        assert rows[0][0] == float("-inf")


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_int_query_param(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        await cur.execute("SELECT ?", params=(3,))
        rows = await cur.fetchall()

        assert rows[0][0] == 3
        await assert_cursor_description(cur, trino_type="integer")

        await cur.execute("SELECT ?", params=(9223372036854775807,))
        rows = await cur.fetchall()

        assert rows[0][0] == 9223372036854775807
        await assert_cursor_description(cur, trino_type="bigint")


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
@pytest.mark.parametrize('params', [
    'NOT A LIST OR TUPPLE',
    {'invalid', 'params'},
    object,
])
async def test_select_query_invalid_params(params, trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        with pytest.raises(AssertionError):
            await cur.execute('SELECT ?', params=params)


@pytest.mark.asyncio(loop_scope="session")
async def test_select_cursor_iteration(trino_connection: Connection):
    async with await trino_connection.cursor() as cur0:
        await cur0.execute("SELECT nationkey FROM tpch.sf1.nation")
        rows0 = []
        async for row in cur0:
            rows0.append(row)

    async with await trino_connection.cursor() as cur1:
        await cur1.execute("SELECT nationkey FROM tpch.sf1.nation")
        rows1 = await cur1.fetchall()

    assert len(rows0) == len(rows1)
    assert sorted(rows0) == sorted(rows1)


@pytest.mark.asyncio(loop_scope="session")
async def test_execute_chaining(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        assert (await (await cur.execute('SELECT 1')).fetchone())[0] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_select_query_no_result(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT * FROM system.runtime.nodes WHERE false")
        rows = await cur.fetchall()
        assert len(rows) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_select_query_stats(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")

        query_id = cur.stats["queryId"]
        completed_splits = cur.stats["completedSplits"]
        cpu_time_millis = cur.stats["cpuTimeMillis"]
        processed_bytes = cur.stats["processedBytes"]
        processed_rows = cur.stats["processedRows"]
        wall_time_millis = cur.stats["wallTimeMillis"]

        while await cur.fetchone() is not None:
            assert query_id == cur.stats["queryId"]
            assert completed_splits <= cur.stats["completedSplits"]
            assert cpu_time_millis <= cur.stats["cpuTimeMillis"]
            assert processed_bytes <= cur.stats["processedBytes"]
            assert processed_rows <= cur.stats["processedRows"]
            assert wall_time_millis <= cur.stats["wallTimeMillis"]

        query_id = cur.stats["queryId"]
        completed_splits = cur.stats["completedSplits"]
        cpu_time_millis = cur.stats["cpuTimeMillis"]
        processed_bytes = cur.stats["processedBytes"]
        processed_rows = cur.stats["processedRows"]
        wall_time_millis = cur.stats["wallTimeMillis"]


@pytest.mark.asyncio(loop_scope="session")
async def test_select_failed_query(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        with pytest.raises(aiotrino.exceptions.TrinoUserError):
            await cur.execute("SELECT * FROM catalog.schema.do_not_exist")
            await cur.fetchall()


@pytest.mark.asyncio(loop_scope="session")
async def test_select_tpch_1000(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = await cur.fetchall()
        assert len(rows) == 1000


@pytest.mark.asyncio(loop_scope="session")
async def test_fetch_cursor(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        for _ in range(100):
            await cur.fetchone()
        assert len(await cur.fetchmany(400)) == 400
        assert len(await cur.fetchall()) == 500


@pytest.mark.asyncio(loop_scope="session")
async def test_cancel_query(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT * FROM tpch.sf1.customer")
        await cur.fetchone()
        await cur.cancel()  # would raise an exception if cancel fails

    # verify that it doesn't fail in the absence of a previously running query
    async with await trino_connection.cursor() as cur:
        await cur.cancel()


@pytest.mark.asyncio(loop_scope="session")
async def test_close_cursor(trino_connection: Connection):
    cur = await trino_connection.cursor()
    await cur.execute("SELECT * FROM tpch.sf1.customer")
    await cur.fetchone()
    await cur.close()  # would raise an exception if cancel fails

    # verify that it doesn't fail in the absence of a previously running query
    cur = await trino_connection.cursor()
    await cur.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_session_properties(run_trino):
    host, port = run_trino

    connection = aiotrino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        session_properties={"query_max_run_time": "10m", "query_priority": "1"},
        max_attempts=1,
    )
    cur = await connection.cursor()
    await cur.execute("SHOW SESSION")
    rows = await cur.fetchall()
    assert len(rows) > 2
    for prop, value, _, _, _ in rows:
        if prop == "query_max_run_time":
            assert value == "10m"
        elif prop == "query_priority":
            assert value == "1"


@pytest.mark.asyncio(loop_scope="session")
async def test_transaction_single(trino_connection_with_transaction: Connection):
    connection = trino_connection_with_transaction
    for _ in range(3):
        cur = await connection.cursor()
        await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = await cur.fetchall()
        await connection.commit()
        assert len(rows) == 1000


@pytest.mark.asyncio(loop_scope="session")
async def test_transaction_rollback(trino_connection_with_transaction: Connection):
    connection = trino_connection_with_transaction
    for _ in range(3):
        cur = await connection.cursor()
        await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = await cur.fetchall()
        await connection.rollback()
        assert len(rows) == 1000


@pytest.mark.asyncio(loop_scope="session")
async def test_transaction_multiple(trino_connection_with_transaction: Connection):
    async with trino_connection_with_transaction as connection:
        cur1 = await connection.cursor()
        await cur1.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows1 = await cur1.fetchall()

        cur2 = await connection.cursor()
        await cur2.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows2 = await cur2.fetchall()

    assert len(rows1) == 1000
    assert len(rows2) == 1000


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.skipif(trino_version() == 351, reason="Autocommit behaves "
                                                   "differently in older Trino versions")
async def test_transaction_autocommit(trino_connection_in_autocommit: Connection):
    async with trino_connection_in_autocommit as connection:
        await connection.start_transaction()
        cur = await connection.cursor()
        with pytest.raises(TrinoUserError) as transaction_error:
            await cur.execute(
                """
                CREATE TABLE memory.default.nation
                AS SELECT * from tpch.tiny.nation
                """)
            await cur.fetchall()
        assert "Catalog only supports writes using autocommit: memory" \
               in str(transaction_error.value)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
async def test_invalid_query_throws_correct_error(trino_connection_with_legacy_prepared_statements: Connection):
    """Tests that an invalid query raises the correct exception
    """
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        with pytest.raises(TrinoQueryError):
            await cur.execute(
                """
                SELECT * FRMO foo WHERE x = ?;
                """,
                params=(3,),
            )


@pytest.mark.asyncio(loop_scope="session")
async def test_eager_loading_cursor_description(trino_connection: Connection):
    description_expected = [
        ('node_id', 'varchar', None, None, None, None, None),
        ('http_uri', 'varchar', None, None, None, None, None),
        ('node_version', 'varchar', None, None, None, None, None),
        ('coordinator', 'boolean', None, None, None, None, None),
        ('state', 'varchar', None, None, None, None, None),
    ]

    async with await trino_connection.cursor() as cur:
        await cur.execute('SELECT * FROM system.runtime.nodes')
        description_before = await cur.get_description()

        assert description_before is not None
        assert len(description_before) == len(description_expected)
        assert all([b == e] for b, e in zip(description_before, description_expected))

        await cur.fetchone()
        description_after = await cur.get_description()
        assert description_after is not None
        assert len(description_after) == len(description_expected)
        assert all([a == e] for a, e in zip(description_after, description_expected))


@pytest.mark.asyncio(loop_scope="session")
async def test_info_uri(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        assert cur.info_uri is None
        await cur.execute('SELECT * FROM system.runtime.nodes')
        assert cur.info_uri is not None
        assert cur._query.query_id in cur.info_uri
        await cur.fetchall()
        assert cur.info_uri is not None
        assert cur._query.query_id in cur.info_uri


@pytest.mark.asyncio(loop_scope="session")
async def test_client_tags_single_tag(run_trino):
    client_tags = ["foo"]
    query_client_tags = await retrieve_client_tags_from_query(run_trino, client_tags)
    assert query_client_tags == client_tags


@pytest.mark.asyncio(loop_scope="session")
async def test_client_tags_multiple_tags(run_trino):
    client_tags = ["foo", "bar"]
    query_client_tags = await retrieve_client_tags_from_query(run_trino, client_tags)
    assert query_client_tags == client_tags


@pytest.mark.asyncio(loop_scope="session")
async def test_client_tags_special_characters(run_trino):
    client_tags = ["foo %20", "bar=test"]
    query_client_tags = await retrieve_client_tags_from_query(run_trino, client_tags)
    assert query_client_tags == client_tags


@pytest.mark.asyncio(loop_scope="session")
async def retrieve_client_tags_from_query(run_trino, client_tags):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        client_tags=client_tags,
    )

    async with await trino_connection.cursor() as cur:
        await cur.execute('SELECT 1')
        await cur.fetchall()

    api_url = "http://" + trino_connection.host + ":" + str(trino_connection.port)
    query_info = requests.post(api_url + "/ui/login", data={
        "username": "admin",
        "password": "",
        "redirectPath": api_url + '/ui/api/query/' + cur._query.query_id
    }).json()

    query_client_tags = query_info['session']['clientTags']
    return query_client_tags


@pytest.mark.skipif(trino_version() == 351, reason="current_catalog not supported in older Trino versions")
@pytest.mark.asyncio(loop_scope="session")
async def test_use_catalog_schema(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute('SELECT current_catalog, current_schema')
        result = await cur.fetchall()
        assert result[0][0] is None
        assert result[0][1] is None

        await cur.execute('USE tpch.tiny')
        await cur.fetchall()
        await cur.execute('SELECT current_catalog, current_schema')
        result = await cur.fetchall()
        assert result[0][0] == 'tpch'
        assert result[0][1] == 'tiny'

        await cur.execute('USE tpcds.sf1')
        await cur.fetchall()
        await cur.execute('SELECT current_catalog, current_schema')
        result = await cur.fetchall()
        assert result[0][0] == 'tpcds'
        assert result[0][1] == 'sf1'


@pytest.mark.skipif(trino_version() == 351, reason="current_catalog not supported in older Trino versions")
@pytest.mark.asyncio(loop_scope="session")
async def test_use_schema(run_trino):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", source="test", catalog="tpch", max_attempts=1
    )

    async with await trino_connection.cursor() as cur:
        await cur.execute('SELECT current_catalog, current_schema')
        result = await cur.fetchall()
        assert result[0][0] == 'tpch'
        assert result[0][1] is None

        await cur.execute('USE tiny')
        await cur.fetchall()
        await cur.execute('SELECT current_catalog, current_schema')
        result = await cur.fetchall()
        assert result[0][0] == 'tpch'
        assert result[0][1] == 'tiny'

        await cur.execute('USE sf1')
        await cur.fetchall()
        await cur.execute('SELECT current_catalog, current_schema')
        result = await cur.fetchall()
        assert result[0][0] == 'tpch'
        assert result[0][1] == 'sf1'


@pytest.mark.asyncio(loop_scope="session")
async def test_set_role(run_trino):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch"
    )

    async with await trino_connection.cursor() as cur:
        await cur.execute('SHOW TABLES FROM information_schema')
        await cur.fetchall()
        assert cur._request._client_session.roles == {}

        await cur.execute("SET ROLE ALL")
        await cur.fetchall()
        if trino_version() == 351:
            assert_role_headers(cur, "tpch=ALL")
        else:
            # Newer Trino versions return the system role
            assert_role_headers(cur, "system=ALL")


@pytest.mark.asyncio(loop_scope="session")
async def test_set_role_in_connection(run_trino):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch", roles={"system": "ALL"}
    )

    async with await trino_connection.cursor() as cur:
        await cur.execute('SHOW TABLES FROM information_schema')
        await cur.fetchall()
        assert_role_headers(cur, "system=ALL")


@pytest.mark.asyncio(loop_scope="session")
async def test_set_system_role_in_connection(run_trino):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch", roles="ALL"
    )
    async with await trino_connection.cursor() as cur:
        await cur.execute('SHOW TABLES FROM information_schema')
        await cur.fetchall()
        assert_role_headers(cur, "system=ALL")


@pytest.mark.asyncio(loop_scope="session")
async def assert_role_headers(cursor, expected_header):
    assert cursor._request.http_headers[constants.HEADER_ROLE] == expected_header


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(None, marks=pytest.mark.skipif(
            trino_version() > 417,
            reason="This would use EXECUTE IMMEDIATE"))
    ]
)
@pytest.mark.asyncio(loop_scope="session")
async def test_prepared_statements(trino_connection_with_legacy_prepared_statements: Connection):
    async with await trino_connection_with_legacy_prepared_statements.cursor() as cur:
        # Implicit prepared statements must work and deallocate statements on finish
        assert cur._request._client_session.prepared_statements == {}
        await cur.execute('SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?', (1,))
        result = await cur.fetchall()
        assert result[0][0] == 1
        assert cur._request._client_session.prepared_statements == {}

        # Explicit prepared statements must also work
        await cur.execute('PREPARE test_prepared_statements FROM SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?')
        await cur.fetchall()
        assert 'test_prepared_statements' in cur._request._client_session.prepared_statements
        await cur.execute('EXECUTE test_prepared_statements USING 1')
        await cur.fetchall()
        assert result[0][0] == 1

        # An implicit prepared statement must not deallocate explicit statements
        await cur.execute('SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?', (1,))
        result = await cur.fetchall()
        assert result[0][0] == 1
        assert 'test_prepared_statements' in cur._request._client_session.prepared_statements

        assert 'test_prepared_statements' in cur._request._client_session.prepared_statements
        await cur.execute('DEALLOCATE PREPARE test_prepared_statements')
        await cur.fetchall()
        assert cur._request._client_session.prepared_statements == {}


@pytest.mark.asyncio(loop_scope="session")
async def test_set_timezone_in_connection(run_trino):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch", timezone="Europe/Brussels"
    )

    async with await trino_connection.cursor() as cur:
        await cur.execute('SELECT current_timezone()')
        res = await cur.fetchall()
        assert res[0][0] == "Europe/Brussels"


@pytest.mark.asyncio(loop_scope="session")
async def test_connection_without_timezone(run_trino):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch"
    )

    async with await trino_connection.cursor() as cur:
        await cur.execute('SELECT current_timezone()')
        res = await cur.fetchall()
        session_tz = res[0][0]
        localzone = get_localzone_name()
        assert session_tz == localzone or \
            (session_tz == "UTC" and localzone == "Etc/UTC") \
            # Workaround for difference between Trino timezone and tzlocal for UTC


@pytest.mark.asyncio(loop_scope="session")
async def test_describe(run_trino):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch",
    )

    async with await trino_connection.cursor() as cur:
        result = await cur.describe("SELECT 1, DECIMAL '1.0' as a")

        assert result == [
            DescribeOutput(name='_col0', catalog='', schema='', table='', type='integer', type_size=4, aliased=False),
            DescribeOutput(name='a', catalog='', schema='', table='', type='decimal(2,1)', type_size=8, aliased=True)
        ]


@pytest.mark.asyncio(loop_scope="session")
async def test_describe_table_query(run_trino):
    host, port = run_trino

    trino_connection = aiotrino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch",
    )

    async with await trino_connection.cursor() as cur:
        result = await cur.describe("SELECT * from tpch.tiny.nation")

        assert result == [
            DescribeOutput(
                name='nationkey',
                catalog='tpch',
                schema='tiny',
                table='nation',
                type='bigint',
                type_size=8,
                aliased=False,
            ),
            DescribeOutput(
                name='name',
                catalog='tpch',
                schema='tiny',
                table='nation',
                type='varchar(25)',
                type_size=0,
                aliased=False,
            ),
            DescribeOutput(
                name='regionkey',
                catalog='tpch',
                schema='tiny',
                table='nation',
                type='bigint',
                type_size=8,
                aliased=False,
            ),
            DescribeOutput(
                name='comment',
                catalog='tpch',
                schema='tiny',
                table='nation',
                type='varchar(152)',
                type_size=0,
                aliased=False,
            )
        ]


@pytest.mark.asyncio(loop_scope="session")
async def test_rowcount_select(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("SELECT 1 as a")
        await cur.fetchall()
        assert cur.rowcount == -1


@pytest.mark.asyncio(loop_scope="session")
async def test_rowcount_create_table(trino_connection: Connection):
    async with _TestTable(trino_connection, "memory.default.test_rowcount_create_table", "(a varchar)") as (_, cur):
        assert cur.rowcount == -1


@pytest.mark.asyncio(loop_scope="session")
async def test_rowcount_create_table_as_select(trino_connection: Connection):
    async with _TestTable(
        trino_connection,
        "memory.default.test_rowcount_ctas", "AS SELECT 1 a UNION ALL SELECT 2"
    ) as (_, cur):
        assert cur.rowcount == 2


@pytest.mark.asyncio(loop_scope="session")
async def test_rowcount_insert(trino_connection: Connection):
    async with _TestTable(trino_connection, "memory.default.test_rowcount_ctas", "(a VARCHAR)") as (table, cur):
        await cur.execute(f"INSERT INTO {table.table_name} (a) VALUES ('test')")
        assert cur.rowcount == 1


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(False, marks=pytest.mark.skipif(
            trino_version() <= 417,
            reason="EXECUTE IMMEDIATE was introduced in version 418")),
        None
    ]
)
@pytest.mark.asyncio(loop_scope="session")
async def test_prepared_statement_capability_autodetection(legacy_prepared_statements, run_trino):
    # start with an empty cache
    aiotrino.dbapi.must_use_legacy_prepared_statements = TimeBoundLRUCache(1024, 3600)
    user_name = f"user_{t.monotonic_ns()}"

    host, port = run_trino
    connection = aiotrino.dbapi.Connection(
        host=host,
        port=port,
        user=user_name,
        legacy_prepared_statements=legacy_prepared_statements,
    )
    cur = await connection.cursor()
    await cur.execute("SELECT ?", [42])
    await cur.fetchall()
    another = await connection.cursor()
    await another.execute("SELECT ?", [100])
    await another.fetchall()

    verify = await connection.cursor()
    rows = await verify.execute("SELECT query FROM system.runtime.queries WHERE user = ?", [user_name])
    statements = [
        stmt
        async for row in rows
        for stmt in row
    ]
    assert statements.count("EXECUTE IMMEDIATE 'SELECT 1'") == (1 if legacy_prepared_statements is None else 0)
    await connection.close()


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.skipif(
    trino_version() <= 466,
    reason="spooling protocol was introduced in version 466"
)
async def test_select_query_spooled_segments(trino_connection: Connection):
    async with await trino_connection.cursor() as cur:
        await cur.execute("""SELECT l.*
        FROM tpch.tiny.lineitem l, TABLE(sequence(
            start => 1,
            stop => 5,
            step => 1)) n""")
        rows = await cur.fetchall()
        assert len(rows) == 300875
        for row in rows:
            assert isinstance(row[0], int), f"Expected integer for orderkey, got {type(row[0])}"
            assert isinstance(row[1], int), f"Expected integer for partkey, got {type(row[1])}"
            assert isinstance(row[2], int), f"Expected integer for suppkey, got {type(row[2])}"
            assert isinstance(row[3], int), f"Expected int for linenumber, got {type(row[3])}"
            assert isinstance(row[4], float), f"Expected float for quantity, got {type(row[4])}"
            assert isinstance(row[5], float), f"Expected float for extendedprice, got {type(row[5])}"
            assert isinstance(row[6], float), f"Expected float for discount, got {type(row[6])}"
            assert isinstance(row[7], float), f"Expected string for tax, got {type(row[7])}"
            assert isinstance(row[8], str), f"Expected string for returnflag, got {type(row[8])}"
            assert isinstance(row[9], str), f"Expected string for linestatus, got {type(row[9])}"
            assert isinstance(row[10], date), f"Expected date for shipdate, got {type(row[10])}"
            assert isinstance(row[11], date), f"Expected date for commitdate, got {type(row[11])}"
            assert isinstance(row[12], date), f"Expected date for receiptdate, got {type(row[12])}"
            assert isinstance(row[13], str), f"Expected string for shipinstruct, got {type(row[13])}"


@pytest.mark.skipif(
    trino_version() <= 466,
    reason="spooling protocol was introduced in version 466"
)
@pytest.mark.asyncio(loop_scope="session")
async def test_segments_cursor(trino_connection: Connection):
    if trino_connection._client_session.encoding is None:
        with pytest.raises(ValueError, match=".*encoding.*"):
            await trino_connection.cursor("segment")
        return

    async with await trino_connection.cursor("segment") as cur:
        await cur.execute("""SELECT l.*
        FROM tpch.tiny.lineitem l, TABLE(sequence(
            start => 1,
            stop => 5,
            step => 1)) n""")
        segments = await cur.fetchall()
        assert len(segments) > 0
        row_mapper = RowMapperFactory().create(
            columns=await cur._query.get_columns(),
            legacy_primitive_types=False,
        )
        total = 0
        for segment in segments:
            assert segment.encoding == trino_connection._client_session.encoding
            assert isinstance(segment.segment.uri, str), f"Expected string for uri, got {segment.segment.uri}"
            assert isinstance(segment.segment.ack_uri, str), f"Expected string for ack_uri, got {segment.segment.ack_uri}"
            total += len([row async for row in SegmentIterator(segment, row_mapper)])
        assert total == 300875, f"Expected total rows 300875, got {total}"


async def assert_cursor_description(cur: Cursor, trino_type, size=None, precision=None, scale=None):
    assert (await cur.get_description())[0][1] == trino_type
    assert (await cur.get_description())[0][2] is None
    assert (await cur.get_description())[0][3] is size
    assert (await cur.get_description())[0][4] is precision
    assert (await cur.get_description())[0][5] is scale
    assert (await cur.get_description())[0][6] is None


class _TestTable:
    def __init__(self, conn: Connection, table_name_prefix: str, table_definition) -> None:
        self._conn = conn
        self._table_name = table_name_prefix + '_' + str(uuid.uuid4().hex)
        self._table_definition = table_definition

    async def __aenter__(self) -> Tuple["_TestTable", Cursor]:
        cur = await self._conn.cursor()
        return (
            self,
            await cur.execute(f"CREATE TABLE {self._table_name} {self._table_definition}")
        )

    async def __aexit__(self, exc_type, exc_value, exc_tb) -> None:
        cur = await self._conn.cursor()
        await cur.execute(f"DROP TABLE {self._table_name}")

    @property
    def table_name(self):
        return self._table_name
