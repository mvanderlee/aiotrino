from typing import Any
from unittest import mock

import pytest
from sqlalchemy import exc, pool
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import create_async_engine

from aiotrino.auth import BasicAuthentication, JWTAuthentication
from aiotrino.dbapi import Connection
from aiotrino.sqlalchemy import URL as trino_url
from aiotrino.sqlalchemy import _compat
from aiotrino.sqlalchemy.dialect import AIOTrinoDialect, AsyncAdaptFallback_aiotrino_connection
from aiotrino.transaction import IsolationLevel


class TestTrinoDialect:
    def setup_method(self):
        self.dialect = AIOTrinoDialect()

    @pytest.mark.parametrize(
        "url, generated_url, expected_args, expected_kwargs",
        [
            (
                make_url(
                    trino_url(
                        user="user",
                        host="localhost",
                    )
                ),
                "aiotrino://user@localhost:8080/?source=aiotrino-sqlalchemy",
                [],
                {
                    "host": "localhost",
                    "catalog": "system",
                    "user": "user",
                    "port": 8080,
                    "source": "aiotrino-sqlalchemy",
                },
            ),
            (
                make_url(
                    trino_url(
                        user="user",
                        host="localhost",
                        port=443,
                    )
                ),
                "aiotrino://user@localhost:443/?source=aiotrino-sqlalchemy",
                [],
                {
                    "host": "localhost",
                    "port": 443,
                    "catalog": "system",
                    "user": "user",
                    "source": "aiotrino-sqlalchemy",
                },
            ),
            (
                make_url(
                    trino_url(
                        user="user",
                        password="pass",
                        host="localhost",
                        source="aiotrino-rulez",
                    )
                ),
                "aiotrino://user:***@localhost:8080/?source=aiotrino-rulez",
                [],
                {
                    "host": "localhost",
                    "port": 8080,
                    "catalog": "system",
                    "user": "user",
                    "auth": BasicAuthentication("user", "pass"),
                    "source": "aiotrino-rulez",
                },
            ),
            # (
            #     make_url(trino_url(
            #         user="user",
            #         host="localhost",
            #         cert="/my/path/to/cert",
            #         key="afdlsdfk%4#'",
            #     )),
            #     'aiotrino://user@localhost:8080/'
            #     '?cert=%2Fmy%2Fpath%2Fto%2Fcert'
            #     '&key=afdlsdfk%254%23%27'
            #     '&source=aiotrino-sqlalchemy',
            #     list(),
            #     dict(
            #         host="localhost",
            #         port=8080,
            #         catalog="system",
            #         user="user",
            #         auth=CertificateAuthentication("/my/path/to/cert", "afdlsdfk%4#'"),
            #         source="aiotrino-sqlalchemy"
            #     ),
            # ),
            # (
            #     make_url(trino_url(
            #         user="user",
            #         host="localhost",
            #         access_token="afdlsdfk%4#'",
            #     )),
            #     'aiotrino://user@localhost:8080/'
            #     '?access_token=afdlsdfk%254%23%27'
            #     '&source=aiotrino-sqlalchemy',
            #     list(),
            #     dict(
            #         host="localhost",
            #         port=8080,
            #         catalog="system",
            #         user="user",
            #         auth=JWTAuthentication("afdlsdfk%4#'"),
            #         source="aiotrino-sqlalchemy"
            #     ),
            # ),
            (
                make_url(
                    trino_url(
                        user="user",
                        host="localhost",
                        session_properties={"query_max_run_time": "1d"},
                        http_headers={"trino": 1},
                        extra_credential=[("a", "b"), ("c", "d")],
                        client_tags=["1", "sql"],
                        legacy_primitive_types=False,
                    )
                ),
                "aiotrino://user@localhost:8080/"
                "?client_tags=%5B%221%22%2C+%22sql%22%5D"
                "&extra_credential=%5B%5B%22a%22%2C+%22b%22%5D%2C+%5B%22c%22%2C+%22d%22%5D%5D"
                "&http_headers=%7B%22trino%22%3A+1%7D"
                "&legacy_primitive_types=false"
                "&session_properties=%7B%22query_max_run_time%22%3A+%221d%22%7D"
                "&source=aiotrino-sqlalchemy",
                [],
                {
                    "host": "localhost",
                    "port": 8080,
                    "catalog": "system",
                    "user": "user",
                    "source": "aiotrino-sqlalchemy",
                    "session_properties": {"query_max_run_time": "1d"},
                    "http_headers": {"trino": 1},
                    "extra_credential": [("a", "b"), ("c", "d")],
                    "client_tags": ["1", "sql"],
                    "legacy_primitive_types": False,
                },
            ),
            # url encoding
            (
                make_url(
                    trino_url(
                        user="user@test.org/my_role",
                        password="pass /*&",
                        host="localhost",
                        session_properties={"query_max_run_time": "1d"},
                        http_headers={"trino": 1},
                        extra_credential=[
                            ("user1@test.org/my_role", "user2@test.org/my_role"),
                            ("user3@test.org/my_role", "user36@test.org/my_role"),
                        ],
                        legacy_primitive_types=False,
                        client_tags=['1 @& /"', "sql"],
                        verify=False,
                    )
                ),
                "aiotrino://user%40test.org%2Fmy_role:***@localhost:8080/"
                "?client_tags=%5B%221+%40%26+%2F%5C%22%22%2C+%22sql%22%5D"
                "&extra_credential=%5B%5B%22user1%40test.org%2Fmy_role%22%2C+"
                "%22user2%40test.org%2Fmy_role%22%5D%2C+"
                "%5B%22user3%40test.org%2Fmy_role%22%2C+"
                "%22user36%40test.org%2Fmy_role%22%5D%5D"
                "&http_headers=%7B%22trino%22%3A+1%7D"
                "&legacy_primitive_types=false"
                "&session_properties=%7B%22query_max_run_time%22%3A+%221d%22%7D"
                "&source=aiotrino-sqlalchemy"
                "&verify=false",
                [],
                {
                    "host": "localhost",
                    "port": 8080,
                    "catalog": "system",
                    "user": "user@test.org/my_role",
                    "auth": BasicAuthentication("user@test.org/my_role", "pass /*&"),
                    "source": "aiotrino-sqlalchemy",
                    "session_properties": {"query_max_run_time": "1d"},
                    "http_headers": {"trino": 1},
                    "extra_credential": [
                        ("user1@test.org/my_role", "user2@test.org/my_role"),
                        ("user3@test.org/my_role", "user36@test.org/my_role"),
                    ],
                    "legacy_primitive_types": False,
                    "client_tags": ['1 @& /"', "sql"],
                    "verify": False,
                },
            ),
            (
                make_url(
                    trino_url(
                        user="user",
                        host="localhost",
                        roles={
                            "hive": "finance",
                            "system": "analyst",
                        },
                    )
                ),
                "aiotrino://user@localhost:8080/"
                "?roles=%7B%22hive%22%3A+%22finance%22%2C+%22system%22%3A+%22analyst%22%7D&source=aiotrino-sqlalchemy",
                [],
                {
                    "host": "localhost",
                    "port": 8080,
                    "catalog": "system",
                    "user": "user",
                    "roles": {"hive": "finance", "system": "analyst"},
                    "source": "aiotrino-sqlalchemy",
                },
            ),
            (
                make_url(
                    trino_url(
                        user="user",
                        host="localhost",
                        client_tags=["1", "sql"],
                        legacy_prepared_statements=False,
                    )
                ),
                "aiotrino://user@localhost:8080/"
                "?client_tags=%5B%221%22%2C+%22sql%22%5D"
                "&legacy_prepared_statements=false"
                "&source=aiotrino-sqlalchemy",
                [],
                {
                    "host": "localhost",
                    "port": 8080,
                    "catalog": "system",
                    "user": "user",
                    "source": "aiotrino-sqlalchemy",
                    "client_tags": ["1", "sql"],
                    "legacy_prepared_statements": False,
                },
            ),
        ],
    )
    def test_create_connect_args(
        self, url: URL, generated_url: str, expected_args: list[Any], expected_kwargs: dict[str, Any]
    ):
        assert repr(url) == generated_url

        actual_args, actual_kwargs = self.dialect.create_connect_args(url)

        assert actual_args == expected_args
        assert actual_kwargs == expected_kwargs

    def test_create_connect_args_missing_user_when_specify_password(self):
        url = make_url("aiotrino://:pass@localhost")
        with pytest.raises(ValueError, match="Username is required when specify password in connection URL"):
            self.dialect.create_connect_args(url)

    def test_create_connect_args_wrong_db_format(self):
        url = make_url("aiotrino://abc@localhost/catalog/schema/foobar")
        with pytest.raises(ValueError, match="Unexpected database format catalog/schema/foobar"):
            self.dialect.create_connect_args(url)

    def test_get_default_isolation_level(self):
        isolation_level = self.dialect.get_default_isolation_level(mock.Mock())
        assert isolation_level == "AUTOCOMMIT"

    async def test_isolation_level(self):
        dbapi_conn = Connection(host="localhost")

        self.dialect.set_isolation_level(dbapi_conn, "SERIALIZABLE")
        assert dbapi_conn._isolation_level == IsolationLevel.SERIALIZABLE

        isolation_level = self.dialect.get_isolation_level(dbapi_conn)
        assert isolation_level == "SERIALIZABLE"


def test_trino_connection_basic_auth():
    dialect = AIOTrinoDialect()
    username = "trino-user"
    password = "trino-bunny"
    url = make_url(f"aiotrino://{username}:{password}@host")
    _, cparams = dialect.create_connect_args(url)

    assert isinstance(cparams["auth"], BasicAuthentication)
    assert cparams["auth"]._username == username
    assert cparams["auth"]._password == password


def test_create_connect_args_does_not_double_decode_password():
    # SQLAlchemy already decodes %2B to '+'. The dialect must not decode it again into a space.
    dialect = AIOTrinoDialect()
    url = make_url("aiotrino://user:pass%2Bword@localhost:8080/system")
    _, cparams = dialect.create_connect_args(url)

    assert cparams["user"] == "user"
    assert cparams["auth"]._password == "pass+word"


@pytest.mark.parametrize(
    "host",
    ["https://example.com", "example.com:8080", "example.com/path", "user:pw@example.com", "[::1"],
)
def test_url_rejects_non_hostname_host(host):
    with pytest.raises(exc.ArgumentError, match="host must be a hostname only"):
        trino_url(host=host)


def test_url_accepts_plain_host():
    assert "example.com" in trino_url(host="example.com")


def test_get_columns_includes_comment():
    dialect = AIOTrinoDialect()
    record = mock.Mock(
        column_name="id",
        data_type="integer",
        column_default=None,
        is_nullable="YES",
        comment="the id column",
    )
    connection = mock.Mock()
    connection.execute.return_value = [record]

    columns = dialect._get_columns(connection, "t", schema="s")

    assert columns[0]["name"] == "id"
    assert columns[0]["comment"] == "the id column"


# def test_trino_connection_jwt_auth():
#     dialect = AIOTrinoDialect()
#     access_token = 'sample-token'
#     url = make_url(f'aiotrino://host/?access_token={access_token}')
#     _, cparams = dialect.create_connect_args(url)

#     assert isinstance(cparams['auth'], JWTAuthentication)
#     assert cparams['auth'].token == access_token


# def test_trino_connection_certificate_auth():
#     dialect = AIOTrinoDialect()
#     cert = '/path/to/cert.pem'
#     key = '/path/to/key.pem'
#     url = make_url(f'aiotrino://host/?cert={cert}&key={key}')
#     _, cparams = dialect.create_connect_args(url)

#     assert isinstance(cparams['auth'], CertificateAuthentication)
#     assert cparams['auth']._cert == cert
#     assert cparams['auth']._key == key


# def test_trino_connection_certificate_auth_cert_and_key_required():
#     dialect = AIOTrinoDialect()
#     cert = '/path/to/cert.pem'
#     key = '/path/to/key.pem'
#     url = make_url(f'aiotrino://host/?cert={cert}')
#     _, cparams = dialect.create_connect_args(url)

#     assert 'auth' not in cparams

#     url = make_url(f'aiotrino://host/?key={key}')
#     _, cparams = dialect.create_connect_args(url)

#     assert 'auth' not in cparams


# def test_trino_connection_oauth2_auth():
#     dialect = AIOTrinoDialect()
#     url = make_url('aiotrino://host/?externalAuthentication=true')
#     _, cparams = dialect.create_connect_args(url)

#     assert isinstance(cparams['auth'], OAuth2Authentication)


def test_trino_connection_query_params_preserve_plus():
    # SQLAlchemy already decodes query values, so a '+' round-tripped through the URL builder must
    # survive. Catalog and schema come from url.database, which only SQLAlchemy 2.1 decodes.
    dialect = AIOTrinoDialect()
    url = make_url(
        trino_url(
            host="host",
            user="us+er",
            password="pa+ss",
            catalog="ca+t",
            schema="sc+h",
            source="so+urce",
            session_properties={"prop": "1+1"},
            http_headers={"x-trino": "a+b"},
            extra_credential=[("cred", "va+lue")],
            client_tags=["ta+g"],
            roles={"system": "ro+le"},
        )
    )
    _, cparams = dialect.create_connect_args(url)

    assert cparams["user"] == "us+er"
    assert cparams["auth"]._password == "pa+ss"
    assert cparams["catalog"] == "ca+t"
    assert cparams["schema"] == "sc+h"
    assert cparams["source"] == "so+urce"
    assert cparams["session_properties"] == {"prop": "1+1"}
    assert cparams["http_headers"] == {"x-trino": "a+b"}
    assert cparams["extra_credential"] == [("cred", "va+lue")]
    assert cparams["client_tags"] == ["ta+g"]
    assert cparams["roles"] == {"system": "ro+le"}


@pytest.mark.parametrize(
    "url, catalog, schema",
    [
        (trino_url(host="localhost", catalog="ca t", schema="sc+h"), "ca t", "sc+h"),
        (trino_url(host="localhost", catalog="ca%2Bt", schema="sc%h"), "ca%2Bt", "sc%h"),
        ("aiotrino://localhost/ca%2Bt/sc%20h", "ca+t", "sc h"),
    ],
)
def test_create_connect_args_decodes_catalog_and_schema_once(url: str, catalog: str, schema: str):
    _, cparams = AIOTrinoDialect().create_connect_args(make_url(url))

    assert cparams["catalog"] == catalog
    assert cparams["schema"] == schema


def test_async_fallback_disabled_uses_async_pool():
    url = make_url("aiotrino://localhost/?async_fallback=false")

    assert AIOTrinoDialect.get_pool_class(url) is pool.AsyncAdaptedQueuePool


@pytest.mark.skipif(not _compat.ASYNC_FALLBACK_SUPPORTED, reason="SQLAlchemy 2.1 removed async_fallback")
class TestAsyncFallbackSupported:
    def test_url_query_selects_fallback_pool(self):
        url = make_url("aiotrino://localhost/?async_fallback=true")

        assert AIOTrinoDialect.get_pool_class(url) is pool.FallbackAsyncAdaptedQueuePool

    async def test_connect_args_select_fallback_connection(self):
        connection = AIOTrinoDialect.import_dbapi().connect(host="localhost", async_fallback=True)

        assert isinstance(connection, AsyncAdaptFallback_aiotrino_connection)


@pytest.mark.skipif(_compat.ASYNC_FALLBACK_SUPPORTED, reason="SQLAlchemy 2.0 still supports async_fallback")
class TestAsyncFallbackRemoved:
    def test_url_query_raises(self):
        with pytest.raises(exc.ArgumentError, match=r"async_fallback is not supported on SQLAlchemy 2\.1"):
            create_async_engine("aiotrino://user@localhost:8080/system?async_fallback=true")

    async def test_connect_args_raise(self):
        engine = create_async_engine("aiotrino://user@localhost:8080/system", connect_args={"async_fallback": True})
        try:
            with pytest.raises(exc.ArgumentError, match=r"async_fallback is not supported on SQLAlchemy 2\.1"):
                async with engine.connect():
                    pass
        finally:
            await engine.dispose()


def test_trino_connection_jwt_auth_preserves_plus():
    dialect = AIOTrinoDialect()
    url = make_url("aiotrino://host/?access_token=tok%2Ben")
    _, cparams = dialect.create_connect_args(url)

    assert isinstance(cparams["auth"], JWTAuthentication)
    assert cparams["auth"].token == "tok+en"
