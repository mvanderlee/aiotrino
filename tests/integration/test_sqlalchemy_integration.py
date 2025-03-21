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
# limitations under the License
import math
import uuid
from decimal import Decimal
from typing import AsyncGenerator

import pytest
import pytest_asyncio
import sqlalchemy as sqla
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.sql import and_, not_, or_
from sqlalchemy.types import ARRAY

from aiotrino.sqlalchemy.datatype import JSON, MAP, ROW
from tests.integration.conftest import trino_version
from tests.unit.conftest import sqlalchemy_version


@pytest_asyncio.fixture(loop_scope="session")
async def trino_connection(run_trino, request) -> AsyncGenerator[tuple[AsyncEngine, AsyncConnection], None]:
    host, port = run_trino
    connect_args = {"source": "test", "max_attempts": 1}
    if trino_version() <= 417:
        connect_args["legacy_prepared_statements"] = True
    engine = create_async_engine(
        f"aiotrino://test@{host}:{port}/{request.param}",
        connect_args=connect_args
    )
    async with engine.connect() as conn:
        yield engine, conn


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_select_query(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection
    metadata = sqla.MetaData()

    async with engine.begin() as conn2:
        nations = await conn2.run_sync(lambda conn: sqla.Table('nation', metadata, schema='tiny', autoload_with=conn))

    assert_column(nations, "nationkey", sqla.sql.sqltypes.BigInteger)
    assert_column(nations, "name", sqla.sql.sqltypes.String)
    assert_column(nations, "regionkey", sqla.sql.sqltypes.BigInteger)
    assert_column(nations, "comment", sqla.sql.sqltypes.String)
    query = sqla.select(nations)
    result = await conn.execute(query)
    rows = result.fetchall()
    assert len(rows) == 25
    for row in rows:
        assert isinstance(row.nationkey, int)
        assert isinstance(row.name, str)
        assert isinstance(row.regionkey, int)
        assert isinstance(row.comment, str)


def assert_column(table, column_name, column_type):
    assert getattr(table.c, column_name).name == column_name
    assert isinstance(getattr(table.c, column_name).type, column_type)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['system'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_select_specific_columns(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection
    metadata = sqla.MetaData()

    async with engine.begin() as conn2:
        nodes = await conn2.run_sync(lambda conn: sqla.Table('nodes', metadata, schema='runtime', autoload_with=conn))

    assert_column(nodes, "node_id", sqla.sql.sqltypes.String)
    assert_column(nodes, "state", sqla.sql.sqltypes.String)
    query = sqla.select(nodes.c.node_id, nodes.c.state)
    result = await conn.execute(query)
    rows = result.fetchall()
    assert len(rows) > 0
    for row in rows:
        assert isinstance(row.node_id, str)
        assert isinstance(row.state, str)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_define_and_create_table(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection
    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        try:
            sqla.Table('users',
                    metadata,
                    sqla.Column('id', sqla.Integer),
                    sqla.Column('name', sqla.String),
                    sqla.Column('fullname', sqla.String),
                    schema="test")
            await connection.run_sync(metadata.create_all)
            assert await connection.run_sync(lambda conn: sqla.inspect(conn).has_table('users', schema="test"))
            users = await connection.run_sync(lambda conn: sqla.Table('users', metadata, schema='test', autoload_with=conn))
            assert_column(users, "id", sqla.sql.sqltypes.Integer)
            assert_column(users, "name", sqla.sql.sqltypes.String)
            assert_column(users, "fullname", sqla.sql.sqltypes.String)
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_insert(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        try:
            users = sqla.Table('users',
                            metadata,
                            sqla.Column('id', sqla.Integer),
                            sqla.Column('name', sqla.String),
                            sqla.Column('fullname', sqla.String),
                            schema="test")
            await connection.run_sync(metadata.create_all)
            ins = users.insert()
            await conn.execute(ins, {"id": 2, "name": "wendy", "fullname": "Wendy Williams"})
            query = sqla.select(users)
            result = await conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0] == (2, "wendy", "Wendy Williams")
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "2.0",
    reason="sqlalchemy.Uuid only exists with SQLAlchemy 2.0 and above"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_define_and_create_table_uuid(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        try:
            sqla.Table('users',
                    metadata,
                    sqla.Column('guid', sqla.Uuid),
                    schema="test")
            await connection.run_sync(metadata.create_all)
            assert await connection.run_sync(lambda conn: sqla.inspect(conn).has_table('users', schema="test"))
            users = await connection.run_sync(lambda conn: sqla.Table('users', metadata, schema='test', autoload_with=conn))
            assert_column(users, "guid", sqla.sql.sqltypes.Uuid)
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "2.0",
    reason="sqlalchemy.Uuid only exists with SQLAlchemy 2.0 and above"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_insert_uuid(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        try:
            users = sqla.Table('users',
                            metadata,
                            sqla.Column('guid', sqla.Uuid),
                            schema="test")
            await connection.run_sync(metadata.create_all)
            ins = users.insert()
            guid = uuid.uuid4()
            await conn.execute(ins, {"guid": guid})
            query = sqla.select(users)
            result = await conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0] == (guid,)
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_insert_multiple_statements(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    async with engine.begin() as connection:
        try:
            metadata = sqla.MetaData()
            users = sqla.Table('users',
                            metadata,
                            sqla.Column('id', sqla.Integer),
                            sqla.Column('name', sqla.String),
                            sqla.Column('fullname', sqla.String),
                            schema="test")
            await connection.run_sync(metadata.create_all)
            ins = users.insert()
            await conn.execute(ins, [
                {"id": 2, "name": "wendy", "fullname": "Wendy Williams"},
                {"id": 3, "name": "john", "fullname": "John Doe"},
                {"id": 4, "name": "mary", "fullname": "Mary Hopkins"},
            ])
            query = sqla.select(users)
            result = await conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 3
            assert frozenset(rows) == frozenset([
                (2, "wendy", "Wendy Williams"),
                (3, "john", "John Doe"),
                (4, "mary", "Mary Hopkins"),
            ])
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_operators(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        customers = await connection.run_sync(lambda conn: sqla.Table('nation', metadata, schema='tiny', autoload_with=conn))

    query = sqla.select(customers).where(customers.c.nationkey == 2)
    result = await conn.execute(query)
    rows = result.fetchall()
    assert len(rows) == 1
    for row in rows:
        assert isinstance(row.nationkey, int)
        assert isinstance(row.name, str)
        assert isinstance(row.regionkey, int)
        assert isinstance(row.comment, str)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_conjunctions(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        customers = await connection.run_sync(lambda conn: sqla.Table('customer', metadata, schema='tiny', autoload_with=conn))
        query = sqla.select(customers).where(and_(
            customers.c.name.like('%12%'),
            customers.c.nationkey == 15,
            or_(
                customers.c.mktsegment == 'AUTOMOBILE',
                customers.c.mktsegment == 'HOUSEHOLD'
            ),
            not_(customers.c.acctbal < 0)))
        result = await conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 1


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_textual_sql(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    _, conn = trino_connection

    s = sqla.text("SELECT * from tiny.customer where nationkey = :e1 AND acctbal < :e2")
    result = await conn.execute(s, {"e1": 15, "e2": 0})
    rows = result.fetchall()
    assert len(rows) == 3
    for row in rows:
        assert isinstance(row.custkey, int)
        assert isinstance(row.name, str)
        assert isinstance(row.address, str)
        assert isinstance(row.nationkey, int)
        assert isinstance(row.phone, str)
        assert isinstance(row.acctbal, float)
        assert isinstance(row.mktsegment, str)
        assert isinstance(row.comment, str)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_alias(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        nations = await connection.run_sync(lambda conn: sqla.Table('nation', metadata, schema='tiny', autoload_with=conn))

    nations1 = nations.alias("o1")
    nations2 = nations.alias("o2")
    s = sqla.select(nations1) \
        .join(nations2, and_(
            nations1.c.regionkey == nations2.c.regionkey,
            nations1.c.nationkey != nations2.c.nationkey,
            nations1.c.regionkey == 1
        )) \
        .distinct()
    result = await conn.execute(s)
    rows = result.fetchall()
    assert len(rows) == 5


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_subquery(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        nations = await connection.run_sync(lambda conn: sqla.Table('nation', metadata, schema='tiny', autoload_with=conn))
        customers = await connection.run_sync(lambda conn: sqla.Table('customer', metadata, schema='tiny', autoload_with=conn))

    automobile_customers = sqla.select(customers.c.nationkey).where(customers.c.acctbal < -900)
    automobile_customers_subquery = automobile_customers.subquery()
    s = sqla.select(nations.c.name).where(nations.c.nationkey.in_(sqla.select(automobile_customers_subquery)))
    result = await conn.execute(s)
    rows = result.fetchall()
    assert len(rows) == 15


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_joins(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        nations = await connection.run_sync(lambda conn: sqla.Table('nation', metadata, schema='tiny', autoload_with=conn))
        customers = await connection.run_sync(lambda conn: sqla.Table('customer', metadata, schema='tiny', autoload_with=conn))

    s = sqla.select(nations.c.name) \
        .select_from(nations.join(customers, nations.c.nationkey == customers.c.nationkey)) \
        .where(customers.c.acctbal < -900) \
        .distinct()
    result = await conn.execute(s)
    rows = result.fetchall()
    assert len(rows) == 15


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_cte(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        nations = await connection.run_sync(lambda conn: sqla.Table('nation', metadata, schema='tiny', autoload_with=conn))
        customers = await connection.run_sync(lambda conn: sqla.Table('customer', metadata, schema='tiny', autoload_with=conn))

    automobile_customers = sqla.select(customers.c.nationkey).where(customers.c.acctbal < -900)
    automobile_customers_cte = automobile_customers.cte()
    s = sqla.select(nations).where(nations.c.nationkey.in_(sqla.select(automobile_customers_cte)))
    result = await conn.execute(s)
    rows = result.fetchall()
    assert len(rows) == 15


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize(
    'trino_connection,json_object',
    [
        ('memory', None),
        ('memory', 1),
        ('memory', 'test'),
        ('memory', [1, 'test']),
        ('memory', {'test': 1}),
    ],
    indirect=['trino_connection']
)
@pytest.mark.asyncio(loop_scope="session")
async def test_json_column(trino_connection: tuple[AsyncEngine, AsyncConnection], json_object):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    async with engine.begin() as connection:
        metadata = sqla.MetaData()
        try:
            table_with_json = sqla.Table(
                'table_with_json',
                metadata,
                sqla.Column('id', sqla.Integer),
                sqla.Column('json_column', JSON),
                schema="test"
            )
            await connection.run_sync(metadata.create_all)
            ins = table_with_json.insert()
            await conn.execute(ins, {"id": 1, "json_column": json_object})
            query = sqla.select(table_with_json)
            assert isinstance(table_with_json.c.json_column.type, JSON)
            result = await conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0] == (1, json_object)
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_json_column_operations(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    metadata = sqla.MetaData()

    json_object = {
        "a": {"c": 1},
        100: {"z": 200},
        "b": 2,
        10: 20,
        "foo-bar": {"z": 200}
    }

    async with engine.begin() as connection:
        try:
            table_with_json = sqla.Table(
                'table_with_json',
                metadata,
                sqla.Column('json_column', JSON),
                schema="default"
            )
            await connection.run_sync(metadata.create_all)
            ins = table_with_json.insert()
            await conn.execute(ins, {"json_column": json_object})

            # JSONPathType
            query = sqla.select(table_with_json.c.json_column["a", "c"])
            await conn.execute(query)
            result = await conn.execute(query)
            assert result.fetchall()[0][0] == 1

            query = sqla.select(table_with_json.c.json_column[100, "z"])
            await conn.execute(query)
            result = await conn.execute(query)
            assert result.fetchall()[0][0] == 200

            query = sqla.select(table_with_json.c.json_column["foo-bar", "z"])
            await conn.execute(query)
            result = await conn.execute(query)
            assert result.fetchall()[0][0] == 200

            # JSONIndexType
            query = sqla.select(table_with_json.c.json_column["b"])
            await conn.execute(query)
            result = await conn.execute(query)
            assert result.fetchall()[0][0] == 2

            query = sqla.select(table_with_json.c.json_column[10])
            await conn.execute(query)
            result = await conn.execute(query)
            assert result.fetchall()[0][0] == 20

            query = sqla.select(table_with_json.c.json_column["foo-bar"])
            await conn.execute(query)
            result = await conn.execute(query)
            assert result.fetchall()[0][0] == {'z': 200}

        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize(
    'trino_connection,map_object,sqla_type',
    [
        ('memory', None, MAP(sqla.sql.sqltypes.String, sqla.sql.sqltypes.Integer)),
        ('memory', {}, MAP(sqla.sql.sqltypes.String, sqla.sql.sqltypes.Integer)),
        ('memory', {True: False, False: True}, MAP(sqla.sql.sqltypes.Boolean, sqla.sql.sqltypes.Boolean)),
        ('memory', {1: 1, 2: None}, MAP(sqla.sql.sqltypes.Integer, sqla.sql.sqltypes.Integer)),
        ('memory', {1.4: 1.4, math.inf: math.inf}, MAP(sqla.sql.sqltypes.Float, sqla.sql.sqltypes.Float)),
        ('memory', {1.4: 1.4, math.inf: math.inf}, MAP(sqla.sql.sqltypes.REAL, sqla.sql.sqltypes.REAL)),
        ('memory',
         {Decimal("1.2"): Decimal("1.2")},
         MAP(sqla.sql.sqltypes.DECIMAL(2, 1), sqla.sql.sqltypes.DECIMAL(2, 1))),
        ('memory', {"hello": "world"}, MAP(sqla.sql.sqltypes.String, sqla.sql.sqltypes.String)),
        ('memory', {"a   ": "a", "null": "n"}, MAP(sqla.sql.sqltypes.CHAR(4), sqla.sql.sqltypes.CHAR(1))),
        ('memory', {b'': b'eh?', b'\x00': None}, MAP(sqla.sql.sqltypes.BINARY, sqla.sql.sqltypes.BINARY)),
    ],
    indirect=['trino_connection']
)
@pytest.mark.asyncio(loop_scope="session")
async def test_map_column(trino_connection: tuple[AsyncEngine, AsyncConnection], map_object, sqla_type):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    metadata = sqla.MetaData()

    async with engine.begin() as connection:
        try:
            table_with_map = sqla.Table(
                'table_with_map',
                metadata,
                sqla.Column('id', sqla.Integer),
                sqla.Column('map_column', sqla_type),
                schema="test"
            )
            await connection.run_sync(metadata.create_all)
            ins = table_with_map.insert()
            await conn.execute(ins, {"id": 1, "map_column": map_object})
            query = sqla.select(table_with_map)
            result = await conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0] == (1, map_object)
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize(
    'trino_connection,array_object,sqla_type',
    [
        ('memory', None, ARRAY(sqla.sql.sqltypes.String)),
        ('memory', [], ARRAY(sqla.sql.sqltypes.String)),
        ('memory', [True, False, True], ARRAY(sqla.sql.sqltypes.Boolean)),
        ('memory', [1, 2, None], ARRAY(sqla.sql.sqltypes.Integer)),
        ('memory', [1.4, 2.3, math.inf], ARRAY(sqla.sql.sqltypes.Float)),
        ('memory', [Decimal("1.2"), Decimal("2.3")], ARRAY(sqla.sql.sqltypes.DECIMAL(2, 1))),
        ('memory', ["hello", "world"], ARRAY(sqla.sql.sqltypes.String)),
        ('memory', ["a   ", "null"], ARRAY(sqla.sql.sqltypes.CHAR(4))),
        ('memory', [b'eh?', None, b'\x00'], ARRAY(sqla.sql.sqltypes.BINARY)),
    ],
    indirect=['trino_connection']
)
@pytest.mark.asyncio(loop_scope="session")
async def test_array_column(trino_connection: tuple[AsyncEngine, AsyncConnection], array_object, sqla_type):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    metadata = sqla.MetaData()

    async with engine.begin() as connection:
        try:
            table_with_array = sqla.Table(
                'table_with_array',
                metadata,
                sqla.Column('id', sqla.Integer),
                sqla.Column('array_column', sqla_type),
                schema="test"
            )
            await connection.run_sync(metadata.create_all)
            ins = table_with_array.insert()
            await conn.execute(ins, {"id": 1, "array_column": array_object})
            query = sqla.select(table_with_array)
            result = await conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0] == (1, array_object)
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize(
    'trino_connection,row_object,sqla_type',
    [
        ('memory', None, ROW([('field1', sqla.sql.sqltypes.String),
                              ('field2', sqla.sql.sqltypes.String)])),
        ('memory', ('hello', 'world'), ROW([('field1', sqla.sql.sqltypes.String),
                                            ('field2', sqla.sql.sqltypes.String)])),
        ('memory', (True, False), ROW([('field1', sqla.sql.sqltypes.Boolean),
                                       ('field2', sqla.sql.sqltypes.Boolean)])),
        ('memory', (1, 2), ROW([('field1', sqla.sql.sqltypes.Integer),
                                ('field2', sqla.sql.sqltypes.Integer)])),
        ('memory', (1.4, float('inf')), ROW([('field1', sqla.sql.sqltypes.Float),
                                             ('field2', sqla.sql.sqltypes.Float)])),
        ('memory', (Decimal("1.2"), Decimal("2.3")), ROW([('field1', sqla.sql.sqltypes.DECIMAL(2, 1)),
                                                          ('field2', sqla.sql.sqltypes.DECIMAL(3, 1))])),
        ('memory', ("hello", "world"), ROW([('field1', sqla.sql.sqltypes.String),
                                            ('field2', sqla.sql.sqltypes.String)])),
        ('memory', ("a   ", "null"), ROW([('field1', sqla.sql.sqltypes.CHAR(4)),
                                          ('field2', sqla.sql.sqltypes.CHAR(4))])),
        ('memory', (b'eh?', b'oh?'), ROW([('field1', sqla.sql.sqltypes.BINARY),
                                          ('field2', sqla.sql.sqltypes.BINARY)])),
    ],
    indirect=['trino_connection']
)
@pytest.mark.asyncio(loop_scope="session")
async def test_row_column(trino_connection: tuple[AsyncEngine, AsyncConnection], row_object, sqla_type):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    metadata = sqla.MetaData()

    async with engine.begin() as connection:
        try:
            table_with_row = sqla.Table(
                'table_with_row',
                metadata,
                sqla.Column('id', sqla.Integer),
                sqla.Column('row_column', sqla_type),
                schema="test"
            )
            await connection.run_sync(metadata.create_all)
            ins = table_with_row.insert()
            await conn.execute(ins, {"id": 1, "row_column": row_object})
            query = sqla.select(table_with_row)
            result = await conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0] == (1, row_object)
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.parametrize('trino_connection', ['system'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_get_catalog_names(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        schemas = await connection.run_sync(lambda conn: engine.dialect.get_catalog_names(conn))
    assert len(schemas) == 5
    assert set(schemas) == {"jmx", "memory", "system", "tpcds", "tpch"}


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_get_table_comment(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        if not (await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, "test"))):
            await connection.execute(sqla.schema.CreateSchema("test"))

    metadata = sqla.MetaData()

    async with engine.begin() as connection:
        try:
            sqla.Table(
                'table_with_id',
                metadata,
                sqla.Column('id', sqla.Integer),
                schema="test",
                # comment="This is a comment" TODO: Support comment creation through sqlalchemy api
            )
            await connection.run_sync(metadata.create_all)
            actual = await connection.run_sync(lambda conn: sqla.inspect(conn).get_table_comment('table_with_id', schema="test"))
            assert actual['text'] is None
        finally:
            await connection.run_sync(metadata.drop_all)


@pytest.mark.parametrize('trino_connection', ['memory/test'], indirect=True)
@pytest.mark.parametrize('schema', [None, 'test'])
@pytest.mark.asyncio(loop_scope="session")
async def test_get_table_names(trino_connection: tuple[AsyncEngine, AsyncConnection], schema):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        schema_name = schema or await connection.run_sync(lambda conn: engine.dialect._get_default_schema_name(conn))
        metadata = sqla.MetaData(schema=schema_name)

        if not await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, schema_name)):
            await connection.execute(sqla.schema.CreateSchema(schema_name))

    async with engine.begin() as connection:
        try:
            sqla.Table(
                'test_get_table_names',
                metadata,
                sqla.Column('id', sqla.Integer),
            )
            await connection.run_sync(metadata.create_all)
            view_name = schema_name + ".test_view"
            await conn.execute(sqla.text(f"CREATE VIEW {view_name} AS SELECT * FROM test_get_table_names"))
            assert await connection.run_sync(lambda conn: sqla.inspect(conn).get_table_names(schema_name)) == ['test_get_table_names']
        finally:
            await conn.execute(sqla.text(f"DROP VIEW IF EXISTS {view_name}"))
            await connection.run_sync(metadata.drop_all)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_get_table_names_raises(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, _ = trino_connection

    async with engine.begin() as connection:
        with pytest.raises(sqla.exc.NoSuchTableError):
            await connection.run_sync(lambda conn: sqla.inspect(conn).get_table_names(None))


@pytest.mark.parametrize('trino_connection', ['memory/test'], indirect=True)
@pytest.mark.parametrize('schema', [None, 'test'])
@pytest.mark.asyncio(loop_scope="session")
async def test_get_view_names(trino_connection: tuple[AsyncEngine, AsyncConnection], schema):
    engine, conn = trino_connection

    async with engine.begin() as connection:
        schema_name = schema or await connection.run_sync(lambda conn: engine.dialect._get_default_schema_name(conn))
        metadata = sqla.MetaData(schema=schema_name)

        if not await connection.run_sync(lambda conn: engine.dialect.has_schema(conn, schema_name)):
            await connection.execute(sqla.schema.CreateSchema(schema_name))

    async with engine.begin() as connection:
        try:
            sqla.Table(
                'test_table',
                metadata,
                sqla.Column('id', sqla.Integer),
            )
            await connection.run_sync(metadata.create_all)
            view_name = schema_name + ".test_get_view_names"
            await conn.execute(sqla.text(f"CREATE VIEW {view_name} AS SELECT * FROM test_table"))
            assert await connection.run_sync(lambda conn: sqla.inspect(conn).get_view_names(schema_name)) == ['test_get_view_names']
        finally:
            await conn.execute(sqla.text(f"DROP VIEW IF EXISTS {view_name}"))
            await connection.run_sync(metadata.drop_all)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
@pytest.mark.asyncio(loop_scope="session")
async def test_get_view_names_raises(trino_connection: tuple[AsyncEngine, AsyncConnection]):
    engine, _ = trino_connection

    async with engine.begin() as connection:
        with pytest.raises(sqla.exc.NoSuchTableError):
            await connection.run_sync(lambda conn: sqla.inspect(conn).get_view_names(None))


# TODO: No idea how to do this async.
# @pytest.mark.parametrize('trino_connection', ['system'], indirect=True)
# @pytest.mark.skipif(trino_version() == 351, reason="version() not supported in older Trino versions")
# @pytest.mark.asyncio(loop_scope="session")
# async def test_version_is_lazy(trino_connection: tuple[AsyncEngine, AsyncConnection]):
#     _, conn = trino_connection

#     result = await conn.execute(sqla.text("SELECT 1"))
#     result.fetchall()
#     num_queries = await _num_queries_containing_string(conn, "SELECT version()")
#     assert num_queries == 0
#     version_info = conn.dialect.server_version_info
#     assert isinstance(version_info, tuple)
#     num_queries = await _num_queries_containing_string(conn, "SELECT version()")
#     assert num_queries == 1


async def _num_queries_containing_string(connection: AsyncConnection, query_string):
    statement = sqla.text("select query from system.runtime.queries order by query_id desc offset 1 limit 1")
    result = await connection.execute(statement)
    rows = result.fetchall()
    return len(list(filter(lambda rec: query_string in rec[0], rows)))
