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
from unittest.mock import patch

import pytest
from aiohttp import ClientSession

from aiotrino import constants
from aiotrino.dbapi import Connection, connect


class aiter_mock():
    def __aiter__(self):
        async def gen():
            yield None

        return gen()

async def mock_execute():
    return aiter_mock()



@pytest.mark.asyncio
@patch("aiotrino.dbapi.aiotrino.client")
async def test_http_session_is_correctly_passed_in(mock_client):
    mock_client.TrinoQuery.return_value.execute.return_value = mock_execute()
    # GIVEN
    test_session = ClientSession()
    # test_session.proxies = {"http": "some.http.proxy:81", "https": "some.http.proxy:81"}

    # WHEN
    async with connect("sample_trino_cluster:443", http_session=test_session) as conn:
        curr = await conn.cursor()
        await curr.execute("SOME FAKE QUERY")

    # THEN
    _, request_kwargs = mock_client.TrinoRequest.call_args
    assert test_session == request_kwargs['http_session']


@pytest.mark.asyncio
@patch("aiotrino.dbapi.aiotrino.client")
async def test_http_session_is_defaulted_when_not_specified(mock_client):
    mock_client.TrinoQuery.return_value.execute.return_value = mock_execute()
    # WHEN
    async with connect("sample_trino_cluster:443") as conn:
        curr = await conn.cursor()
        await curr.execute("SOME FAKE QUERY")

    # THEN
    _, request_kwargs = mock_client.TrinoRequest.call_args
    assert mock_client.TrinoRequest.http.ClientSession.return_value == request_kwargs['http_session']


@pytest.mark.asyncio
@patch("aiotrino.dbapi.aiotrino.client")
async def test_tags_are_set_when_specified(mock_client):
    mock_client.TrinoQuery.return_value.execute.return_value = mock_execute()
    client_tags = ["TAG1", "TAG2"]
    async with connect("sample_trino_cluster:443", client_tags=client_tags) as conn:
        curr = await conn.cursor()
        await curr.execute("SOME FAKE QUERY")

    _, passed_client_tags = mock_client.ClientSession.call_args
    assert passed_client_tags["client_tags"] == client_tags


@pytest.mark.asyncio
@patch("aiotrino.dbapi.aiotrino.client")
async def test_role_is_set_when_specified(mock_client):
    mock_client.TrinoQuery.return_value.execute.return_value = mock_execute()
    roles = {"system": "finance"}
    async with connect("sample_trino_cluster:443", roles=roles) as conn:
        curr = await conn.cursor()
        await curr.execute("SOME FAKE QUERY")

    _, passed_role = mock_client.ClientSession.call_args
    assert passed_role["roles"] == roles


@pytest.mark.asyncio
async def test_hostname_parsing():
    https_server_with_port = Connection("https://mytrinoserver.domain:9999")
    assert https_server_with_port.host == "mytrinoserver.domain"
    assert https_server_with_port.port == 9999
    assert https_server_with_port.http_scheme == constants.HTTPS

    https_server_without_port = Connection("https://mytrinoserver.domain")
    assert https_server_without_port.host == "mytrinoserver.domain"
    assert https_server_without_port.port == 8080
    assert https_server_without_port.http_scheme == constants.HTTPS

    http_server_with_port = Connection("http://mytrinoserver.domain:9999")
    assert http_server_with_port.host == "mytrinoserver.domain"
    assert http_server_with_port.port == 9999
    assert http_server_with_port.http_scheme == constants.HTTP

    http_server_without_port = Connection("http://mytrinoserver.domain")
    assert http_server_without_port.host == "mytrinoserver.domain"
    assert http_server_without_port.port == 8080
    assert http_server_without_port.http_scheme == constants.HTTP

    http_server_with_path = Connection("http://mytrinoserver.domain/some_path")
    assert http_server_with_path.host == "mytrinoserver.domain/some_path"
    assert http_server_with_path.port == 8080
    assert http_server_with_path.http_scheme == constants.HTTP

    only_hostname = Connection("mytrinoserver.domain")
    assert only_hostname.host == "mytrinoserver.domain"
    assert only_hostname.port == 8080
    assert only_hostname.http_scheme == constants.HTTP

    only_hostname_with_path = Connection("mytrinoserver.domain/some_path")
    assert only_hostname_with_path.host == "mytrinoserver.domain/some_path"
    assert only_hostname_with_path.port == 8080
    assert only_hostname_with_path.http_scheme == constants.HTTP


@pytest.mark.asyncio
async def test_description_is_none_when_cursor_is_not_executed():
    connection = Connection("sample_trino_cluster:443")
    async with await connection.cursor() as cursor:
        assert await cursor.get_description() is None
