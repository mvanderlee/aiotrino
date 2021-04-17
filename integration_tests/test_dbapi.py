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
from datetime import datetime

import pytest
import pytz
# Need to specify the fixture for it to load properly
from fixtures import TRINO_VERSION, run_trino

import aiotrino
from aiotrino.exceptions import TrinoQueryError
from aiotrino.transaction import IsolationLevel


@pytest.fixture
def trino_connection(run_trino):
    _, host, port = run_trino

    yield aiotrino.dbapi.Connection(
        host=host, port=port, user="test", source="test", max_attempts=1
    )


@pytest.fixture
def trino_connection_with_transaction(run_trino):
    _, host, port = run_trino

    yield aiotrino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        max_attempts=1,
        isolation_level=IsolationLevel.READ_UNCOMMITTED,
    )


@pytest.mark.asyncio
async def test_select_query(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("select * from system.runtime.nodes")
    rows = await cur.fetchall()
    assert len(rows) > 0
    row = rows[0]
    assert row[2] == TRINO_VERSION
    columns = dict([desc[:2] for desc in cur.description])
    assert columns["node_id"] == "varchar"
    assert columns["http_uri"] == "varchar"
    assert columns["node_version"] == "varchar"
    assert columns["coordinator"] == "boolean"
    assert columns["state"] == "varchar"


@pytest.mark.asyncio
async def test_select_query_result_iteration(trino_connection: aiotrino.dbapi.Connection):
    cur0 = await trino_connection.cursor()
    await cur0.execute("select custkey from tpch.sf1.customer LIMIT 10")
    rows0 = [row async for row in cur0.genall()]

    cur1 = await trino_connection.cursor()
    await cur1.execute("select custkey from tpch.sf1.customer LIMIT 10")
    rows1 = await cur1.fetchall()

    assert len(list(rows0)) == len(rows1)


@pytest.mark.asyncio
async def test_select_query_result_iteration_statement_params(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute(
        """
        select * from (
            values
            (1, 'one', 'a'),
            (2, 'two', 'b'),
            (3, 'three', 'c'),
            (4, 'four', 'd'),
            (5, 'five', 'e')
        ) x (id, name, letter)
        where id >= ?
        """,
        params=(3,)  # expecting all the rows with id >= 3
    )


@pytest.mark.asyncio
async def test_none_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("SELECT ?", params=(None,))
    rows = await cur.fetchall()

    assert rows[0][0] is None


@pytest.mark.asyncio
async def test_string_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()

    await cur.execute("SELECT ?", params=("six'",))
    rows = await cur.fetchall()

    assert rows[0][0] == "six'"


@pytest.mark.asyncio
async def test_datetime_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()

    await cur.execute(
        "SELECT ?",
        params=(datetime(2020, 1, 1, 0, 0, 0),)
    )
    rows = await cur.fetchall()

    assert rows[0][0] == "2020-01-01 00:00:00.000"

    await cur.execute(
        "SELECT ?",
        params=(datetime(2020, 1, 1, 0, 0, 0, tzinfo=pytz.utc),)
    )
    rows = await cur.fetchall()

    assert rows[0][0] == "2020-01-01 00:00:00.000 UTC"
    assert cur.description[0][1] == "timestamp with time zone"


@pytest.mark.asyncio
async def test_array_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()

    await cur.execute("SELECT ?", params=([1, 2, 3],))
    rows = await cur.fetchall()

    assert rows[0][0] == [1, 2, 3]

    await cur.execute(
        "SELECT ?",
        params=([[1, 2, 3], [4, 5, 6]],)
    )
    rows = await cur.fetchall()

    assert rows[0][0] == [[1, 2, 3], [4, 5, 6]]

    await cur.execute("SELECT TYPEOF(?)", params=([1, 2, 3],))
    rows = await cur.fetchall()

    assert rows[0][0] == "array(integer)"


@pytest.mark.asyncio
async def test_dict_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()

    await cur.execute("SELECT ?", params=({"foo": "bar"},))
    rows = await cur.fetchall()

    assert rows[0][0] == {"foo": "bar"}

    await cur.execute("SELECT TYPEOF(?)", params=({"foo": "bar"},))
    rows = await cur.fetchall()

    assert rows[0][0] == "map(varchar(3), varchar(3))"


@pytest.mark.asyncio
async def test_boolean_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()

    await cur.execute("SELECT ?", params=(True,))
    rows = await cur.fetchall()

    assert rows[0][0] is True

    await cur.execute("SELECT ?", params=(False,))
    rows = await cur.fetchall()

    assert rows[0][0] is False


@pytest.mark.asyncio
async def test_float_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("SELECT ?", params=(1.1,))
    rows = await cur.fetchall()

    assert cur.description[0][1] == "double"
    assert rows[0][0] == 1.1


@pytest.mark.skip(reason="Nan currently not returning the correct python type for nan")
@pytest.mark.asyncio
async def test_float_nan_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("SELECT ?", params=(float("nan"),))
    rows = await cur.fetchall()

    assert cur.description[0][1] == "double"
    assert isinstance(rows[0][0], float)
    assert math.isnan(rows[0][0])


@pytest.mark.skip(reason="Nan currently not returning the correct python type fon inf")
@pytest.mark.asyncio
async def test_float_inf_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("SELECT ?", params=(float("inf"),))
    rows = await cur.fetchall()

    assert rows[0][0] == float("inf")

    await cur.execute("SELECT ?", params=(-float("-inf"),))
    rows = await cur.fetchall()

    assert rows[0][0] == float("-inf")


@pytest.mark.asyncio
async def test_int_query_param(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("SELECT ?", params=(3,))
    rows = await cur.fetchall()

    assert rows[0][0] == 3
    assert cur.description[0][1] == "integer"

    await cur.execute("SELECT ?", params=(9223372036854775807,))
    rows = await cur.fetchall()

    assert rows[0][0] == 9223372036854775807
    assert cur.description[0][1] == "bigint"


@pytest.mark.parametrize('params', [
    'NOT A LIST OR TUPPLE',
    {'invalid', 'params'},
    object,
])
@pytest.mark.asyncio
async def test_select_query_invalid_params(trino_connection: aiotrino.dbapi.Connection, params):
    cur = await trino_connection.cursor()
    with pytest.raises(AssertionError):
        await cur.execute('select ?', params=params)


@pytest.mark.asyncio
async def test_select_cursor_iteration(trino_connection: aiotrino.dbapi.Connection):
    cur0 = await trino_connection.cursor()
    await cur0.execute("select nationkey from tpch.sf1.nation")
    rows0 = []
    async for row in cur0:
        rows0.append(row)

    cur1 = await trino_connection.cursor()
    await cur1.execute("select nationkey from tpch.sf1.nation")
    rows1 = await cur1.fetchall()

    assert len(rows0) == len(rows1)
    assert sorted(rows0) == sorted(rows1)


@pytest.mark.asyncio
async def test_select_query_no_result(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("select * from system.runtime.nodes where false")
    rows = await cur.fetchall()
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_select_query_stats(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
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


@pytest.mark.asyncio
async def test_select_failed_query(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    with pytest.raises(aiotrino.exceptions.TrinoUserError):
        await cur.execute("select * from catalog.schema.do_not_exist")
        await cur.fetchall()


@pytest.mark.asyncio
async def test_select_tpch_1000(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
    rows = await cur.fetchall()
    assert len(rows) == 1000


@pytest.mark.asyncio
async def test_cancel_query(trino_connection: aiotrino.dbapi.Connection):
    cur = await trino_connection.cursor()
    await cur.execute("select * from tpch.sf1.customer")
    await cur.fetchone()  # TODO (https://github.com/trinodb/trino/issues/2683) test with and without .fetchone
    await cur.cancel()  # would raise an exception if cancel fails

    cur = await trino_connection.cursor()
    with pytest.raises(Exception) as cancel_error:
        await cur.cancel()
    assert "Cancel query failed; no running query" in str(cancel_error.value)


@pytest.mark.asyncio
async def test_session_properties(run_trino):
    _, host, port = run_trino

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


@pytest.mark.asyncio
async def test_transaction_single(trino_connection_with_transaction: aiotrino.dbapi.Connection):
    connection = trino_connection_with_transaction
    for _ in range(3):
        cur = await connection.cursor()
        await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = await cur.fetchall()
        await connection.commit()
        assert len(rows) == 1000


@pytest.mark.asyncio
async def test_transaction_rollback(trino_connection_with_transaction: aiotrino.dbapi.Connection):
    connection = trino_connection_with_transaction
    for _ in range(3):
        cur = await connection.cursor()
        await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = await cur.fetchall()
        await connection.rollback()
        assert len(rows) == 1000


@pytest.mark.asyncio
async def test_transaction_multiple(trino_connection_with_transaction: aiotrino.dbapi.Connection):
    async with trino_connection_with_transaction as connection:
        cur1 = await connection.cursor()
        await cur1.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows1 = await cur1.fetchall()

        cur2 = await connection.cursor()
        await cur2.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows2 = await cur2.fetchall()

    assert len(rows1) == 1000
    assert len(rows2) == 1000


@pytest.mark.asyncio
async def test_invalid_query_throws_correct_error(trino_connection: aiotrino.dbapi.Connection):
    """
    tests that an invalid query raises the correct exception
    """
    cur = await trino_connection.cursor()
    with pytest.raises(TrinoQueryError):
        await cur.execute(
            """
            select * FRMO foo WHERE x = ?;
            """,
            params=(3,),
        )
