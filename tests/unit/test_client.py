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
import asyncio
import time
import urllib
from typing import Dict, Optional
from unittest import mock
from urllib.parse import urlparse
from zoneinfo import ZoneInfoNotFoundError

import aiohttp
import aiohttp.client_exceptions
import pytest
from mocket.plugins.httpretty import async_httprettified, httpretty
from tzlocal import get_localzone_name  # type: ignore
from yarl import URL

import aiotrino
from aiotrino import __version__, constants
from aiotrino.client import (
    ClientSession,
    TrinoQuery,
    TrinoRequest,
    TrinoResult,
    _DelayExponential,
    _retry_with,
    _RetryWithExponentialBackoff,
)


def create_response() -> aiohttp.ClientResponse:
    client_response = TrinoRequest.http.ClientResponse(
        'POST',
        URL('aiotrino://user@localhost:8080/?source=aiotrino-sqlalchemy'),
        writer=None,
        continue100=None,
        timer=None,
        request_info=None,
        traces=[],
        loop=asyncio.get_running_loop(),
        session=None,
    )
    class MockReader:
        async def read(self):
            return None

    client_response.content = MockReader()

    return client_response


async def async_json(value):
    return value


@pytest.mark.asyncio
@mock.patch("aiotrino.client.TrinoRequest.http")
async def test_trino_initial_request(mock_requests, sample_post_response_data):
    mock_requests.ClientResponse.return_value.json.return_value = async_json(sample_post_response_data)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = create_response()
    http_resp.status = 200
    status = await req.process(http_resp)

    assert status.next_uri == sample_post_response_data["nextUri"]
    assert status.id == sample_post_response_data["id"]


@pytest.mark.asyncio
async def test_request_headers(mock_get_and_post):
    get, post = mock_get_and_post

    catalog = "test_catalog"
    schema = "test_schema"
    user = "test_user"
    authorization_user = "test_authorization_user"
    source = "test_source"
    timezone = "Europe/Brussels"
    accept_encoding_header = "accept-encoding"
    accept_encoding_value = "identity,deflate,gzip"
    client_info_header = constants.HEADER_CLIENT_INFO
    client_info_value = "some_client_info"
    encoding = "json+zstd"

    with pytest.deprecated_call():
        req = TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(
                user=user,
                authorization_user=authorization_user,
                source=source,
                catalog=catalog,
                schema=schema,
                timezone=timezone,
                encoding=encoding,
                headers={
                    accept_encoding_header: accept_encoding_value,
                    client_info_header: client_info_value,
                },
                roles={
                    "hive": "ALL",
                    "system": "analyst",
                    "catalog1": "NONE",
                    # ensure backwards compatibility
                    "catalog2": "ROLE{catalog2_role}",
                }
            ),
            http_scheme="http",
        )

    def assert_headers(headers):
        assert headers[constants.HEADER_CATALOG] == catalog
        assert headers[constants.HEADER_SCHEMA] == schema
        assert headers[constants.HEADER_SOURCE] == source
        assert headers[constants.HEADER_USER] == user
        assert headers[constants.HEADER_AUTHORIZATION_USER] == authorization_user
        assert headers[constants.HEADER_SESSION] == ""
        assert headers[constants.HEADER_TIMEZONE] == timezone
        assert headers[constants.HEADER_CLIENT_CAPABILITIES] == "PARAMETRIC_DATETIME"
        assert headers[accept_encoding_header] == accept_encoding_value
        assert headers[client_info_header] == client_info_value
        assert headers[constants.HEADER_ROLE] == (
            "hive=ALL,"
            "system=" + urllib.parse.quote("ROLE{analyst}") + ","
            "catalog1=NONE,"
            "catalog2=" + urllib.parse.quote("ROLE{catalog2_role}")
        )
        assert headers["User-Agent"] == f"{constants.CLIENT_NAME}/{__version__}"
        assert headers[constants.HEADER_ENCODING] == encoding
        assert constants.HEADER_TRANSACTION not in headers
        assert len(headers.keys()) == 13

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers(post_kwargs["headers"])

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers(get_kwargs["headers"])


@pytest.mark.asyncio
async def test_request_session_properties_headers(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            properties={
                "a": "1",
                "b": "2",
                "c": "more=v1,v2"
            }
        )
    )

    def assert_headers(headers):
        assert headers[constants.HEADER_SESSION] == "a=1,b=2,c=more%3Dv1%2Cv2"

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers(post_kwargs["headers"])

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers(get_kwargs["headers"])


@pytest.mark.asyncio
async def test_additional_request_post_headers(mock_get_and_post):
    """
    Tests that the `TrinoRequest.post` function can take addtional headers
    and that it combines them with the existing ones to perform the request.
    """
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    sql = 'select 1'
    additional_headers = {
        'X-Trino-Fake-1': 'one',
        'X-Trino-Fake-2': 'two',
    }

    combined_headers = req.http_headers
    combined_headers.update(additional_headers)

    await req.post(sql, additional_headers)

    # Validate that the post call was performed including the addtional headers
    _, post_kwargs = post.call_args
    assert post_kwargs['headers'] == combined_headers


async def test_request_invalid_http_headers():
    with pytest.raises(ValueError) as value_error:
        TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(
                user="test",
                headers={constants.HEADER_USER: "invalid_header"},
            ),
        )
    assert str(value_error.value).startswith("cannot override reserved HTTP header")


@pytest.mark.asyncio
async def test_request_client_tags_headers(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            client_tags=["tag1", "tag2"]
        ),
    )

    def assert_headers(headers):
        assert headers[constants.HEADER_CLIENT_TAGS] == "tag1,tag2"

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers(post_kwargs["headers"])

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers(get_kwargs["headers"])


@pytest.mark.asyncio
async def test_request_client_tags_headers_no_client_tags(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
        )
    )

    def assert_headers(headers):
        assert constants.HEADER_CLIENT_TAGS not in headers

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers(post_kwargs["headers"])

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers(get_kwargs["headers"])


@pytest.mark.asyncio
async def test_enabling_https_automatically_when_using_port_443(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
    )

    await req.post("SELECT 1")
    post_args, _ = post.call_args
    parsed_url = urlparse(post_args[0])

    assert parsed_url.scheme == constants.HTTPS


@pytest.mark.asyncio
async def test_https_scheme(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
    )

    await req.post("SELECT 1")
    post_args, _ = post.call_args
    parsed_url = urlparse(post_args[0])

    assert parsed_url.scheme == constants.HTTPS
    assert parsed_url.port == constants.DEFAULT_TLS_PORT


@pytest.mark.asyncio
async def test_http_scheme_with_port(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTP,
    )

    await req.post("SELECT 1")
    post_args, _ = post.call_args
    parsed_url = urlparse(post_args[0])

    assert parsed_url.scheme == constants.HTTP
    assert parsed_url.port == constants.DEFAULT_TLS_PORT


@async_httprettified
@pytest.mark.asyncio
async def test_request_timeout():
    timeout = 0.1
    http_scheme = "http"
    host = "coordinator"
    port = 8080
    url = http_scheme + "://" + host + ":" + str(port) + constants.URL_STATEMENT_PATH


    class LongCallResponse(bytes):
        @property
        def data(self):
            time.sleep(timeout * 2)
            return b"delayed success"

    for method in [httpretty.POST, httpretty.GET]:
        httpretty.register_uri(method, url, responses=[LongCallResponse()])

    # timeout without retry
    req = TrinoRequest(
        host=host,
        port=port,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=http_scheme,
        max_attempts=1,
        request_timeout=timeout,
    )

    with pytest.raises((
        asyncio.TimeoutError,
        aiohttp.client_exceptions.ServerTimeoutError,
        aiohttp.client_exceptions.SocketTimeoutError,
        aiohttp.client_exceptions.ConnectionTimeoutError,
    )):
        await req.get(url)

    with pytest.raises((
        asyncio.TimeoutError,
        aiohttp.client_exceptions.ServerTimeoutError,
        aiohttp.client_exceptions.SocketTimeoutError,
        aiohttp.client_exceptions.ConnectionTimeoutError,
    )):
        await req.post("select 1")


@pytest.mark.asyncio
@mock.patch("aiotrino.client.TrinoRequest.http")
async def test_trino_fetch_request(mock_requests, sample_get_response_data):
    mock_requests.ClientResponse.return_value.json.return_value = async_json(sample_get_response_data)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = create_response()
    http_resp.status = 200
    status = await req.process(http_resp)

    assert status.next_uri == sample_get_response_data["nextUri"]
    assert status.id == sample_get_response_data["id"]
    assert status.rows == sample_get_response_data["data"]

@pytest.mark.asyncio
@mock.patch("aiotrino.client.TrinoRequest.http")
async def test_trino_fetch_request_data_none(mock_requests, sample_get_response_data_none):
    mock_requests.ClientResponse.return_value.json.return_value = async_json(sample_get_response_data_none)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = create_response()
    http_resp.status = 200
    status = await req.process(http_resp)

    assert status.next_uri == sample_get_response_data_none["nextUri"]
    assert status.id == sample_get_response_data_none["id"]
    assert status.rows == []

@pytest.mark.asyncio
@mock.patch("aiotrino.client.TrinoRequest.http")
async def test_trino_fetch_error(mock_requests, sample_get_error_response_data):
    mock_requests.ClientResponse.return_value.json.return_value = async_json(sample_get_error_response_data)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = create_response()
    http_resp.status = 200
    with pytest.raises(aiotrino.exceptions.TrinoUserError) as exception_info:
        await req.process(http_resp)
    error = exception_info.value
    assert error.error_code == 1
    assert error.error_name == "SYNTAX_ERROR"
    assert error.error_type == "USER_ERROR"
    assert error.error_exception == "io.trino.spi.TrinoException"
    assert "stack" in error.failure_info
    assert len(error.failure_info["stack"]) == 36
    assert "suppressed" in error.failure_info
    assert (
        error.message
        == "line 1:15: Schema must be specified when session schema is not set"
    )
    assert error.error_location == (1, 15)
    assert error.query_id == "20210817_140827_00000_arvdv"

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code, error_type, error_message",
    [
        (503, aiotrino.exceptions.Http503Error, "service unavailable"),
        (504, aiotrino.exceptions.Http504Error, "gateway timeout"),
        (404, aiotrino.exceptions.HttpError, "error 404"),
    ],
)
async def test_trino_connection_error(monkeypatch, error_code, error_type, error_message):
    monkeypatch.setattr(TrinoRequest.http.ClientResponse, "json", lambda x: {})

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = create_response()
    http_resp.status = error_code
    with pytest.raises(error_type) as error:
        await req.process(http_resp)
    assert error_message in str(error)


@pytest.mark.asyncio
async def test_extra_credential(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
            extra_credential=[("a.username", "foo"), ("b.password", "bar")],
        ),
    )

    await req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "a.username=foo, b.password=bar"


@pytest.mark.asyncio
async def test_extra_credential_key_with_illegal_chars():
    with pytest.raises(ValueError) as e_info:
        TrinoRequest(
            host="coordinator",
            port=constants.DEFAULT_TLS_PORT,
            client_session=ClientSession(
                user="test",
                extra_credential=[("a=b", "")],
            ),
        )

    assert str(e_info.value) == "whitespace or '=' are disallowed in extra credential 'a=b'"


@pytest.mark.asyncio
async def test_extra_credential_key_non_ascii():
    with pytest.raises(ValueError) as e_info:
        TrinoRequest(
            host="coordinator",
            port=constants.DEFAULT_TLS_PORT,
            client_session=ClientSession(
                user="test",
                extra_credential=[("的", "")],
            ),
        )

    assert str(e_info.value) == "only ASCII characters are allowed in extra credential '的'"


@pytest.mark.asyncio
async def test_extra_credential_value_encoding(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
            extra_credential=[("foo", "bar 的")],
        ),
    )

    await req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=bar+%E7%9A%84"


@pytest.mark.asyncio
async def test_extra_credential_value_object(mock_get_and_post):
    _, post = mock_get_and_post

    class TestCredential:
        value = "initial"

        def __str__(self):
            return self.value

    credential = TestCredential()

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
            extra_credential=[("foo", credential)]
        )
    )

    await req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=initial"

    # Make a second request, assert that credential has changed
    credential.value = "changed"
    await req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=changed"


class RetryRecorder:
    def __init__(self, error=None, result=None):
        self.__name__ = "RetryRecorder"
        self._retry_count = 0
        self._error = error
        self._result = result

    async def __call__(self, *args, **kwargs):
        self._retry_count += 1
        if self._error is not None:
            raise self._error
        if self._result is not None:
            return self._result

    @property
    def retry_count(self):
        return self._retry_count

@pytest.mark.asyncio
@pytest.mark.parametrize("status_code, attempts", [
    (502, 3),
    (503, 3),
    (504, 3),
])
async def test_5XX_error_retry(status_code, attempts, monkeypatch):
    http_resp = create_response()
    http_resp.status = status_code

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.ClientSession, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.ClientSession, "get", get_retry)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        max_attempts=attempts
    )

    await req.post("URL")
    assert post_retry.retry_count == attempts

    await req.get("URL")
    assert post_retry.retry_count == attempts


@pytest.mark.asyncio
async def test_429_error_retry(monkeypatch):
    http_resp = create_response()
    http_resp.status = 429
    http_resp._headers = {"Retry-After": 1}

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.ClientSession, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.ClientSession, "get", get_retry)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        max_attempts=3
    )

    await req.post("URL")
    assert post_retry.retry_count == 3

    await req.get("URL")
    assert post_retry.retry_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [
    501
])
async def test_error_no_retry(status_code, monkeypatch):
    http_resp = create_response()
    http_resp.status = status_code

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.ClientSession, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.ClientSession, "get", get_retry)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        max_attempts=3,
    )

    await req.post("URL")
    assert post_retry.retry_count == 1

    await req.get("URL")
    assert post_retry.retry_count == 1


class FakeGatewayResponse:
    def __init__(self, http_response, redirect_count=1):
        self.__name__ = "FakeGatewayResponse"
        self.http_response = http_response
        self.redirect_count = redirect_count
        self.count = 0

    def __call__(self, *args, **kwargs):
        self.count += 1
        if self.count == self.redirect_count:
            return self.http_response
        http_response = create_response()
        http_response.status = 301
        http_response.headers["Location"] = "http://1.2.3.4:8080/new-path/"
        assert http_response.is_redirect
        return http_response


@pytest.mark.asyncio
async def test_trino_query_response_headers(sample_get_response_data):
    """
    Validates that the `TrinoQuery.execute` function can take addtional headers
    that are pass the the provided request instance post function call and it
    returns a `TrinoResult` instance.
    """

    class MockResponse(mock.Mock):
        # Fake response class
        @property
        def headers(self):
            return {
                'X-Trino-Fake-1': 'one',
                'X-Trino-Fake-2': 'two',
            }

        async def json(self, *args, **kwargs):
            return sample_get_response_data

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    sql = 'execute my_stament using 1, 2, 3'
    additional_headers = {
        constants.HEADER_PREPARED_STATEMENT: 'my_statement=added_prepare_statement_header',
        constants.HEADER_CLIENT_CAPABILITIES: 'PARAMETRIC_DATETIME'
    }

    # Patch the post function to avoid making the requests, as well as to
    # validate that the function was called with the right arguments.
    with mock.patch.object(req, 'post', return_value=MockResponse()) as mock_post:
        query = TrinoQuery(
            request=req,
            query=sql
        )
        result = await query.execute(additional_http_headers=additional_headers)

        # Validate the the post function was called with the right argguments
        mock_post.assert_called_once_with(sql, additional_headers)

        # Validate the result is an instance of TrinoResult
        assert isinstance(result, TrinoResult)


def test_delay_exponential_without_jitter():
    max_delay = 1200.0
    get_delay = _DelayExponential(base=5, jitter=False, max_delay=max_delay)
    results = [
        10.0,
        20.0,
        40.0,
        80.0,
        160.0,
        320.0,
        640.0,
        max_delay,  # rather than 1280.0
        max_delay,  # rather than 2560.0
    ]
    for i, result in enumerate(results, start=1):
        assert get_delay(i) == result


def test_delay_exponential_with_jitter():
    max_delay = 120.0
    get_delay = _DelayExponential(base=10, jitter=False, max_delay=max_delay)
    for i in range(10):
        assert get_delay(i) <= max_delay


class SomeException(Exception):
    pass


async def test_retry_with():
    max_attempts = 3
    with_retry = _retry_with(
        handle_retry=_RetryWithExponentialBackoff(),
        handled_exceptions=[SomeException],
        conditions={},
        max_attempts=max_attempts,
    )

    class FailerUntil:
        def __init__(self, until=1):
            self.attempt = 0
            self._until = until

        async def __call__(self):
            self.attempt += 1
            if self.attempt > self._until:
                return
            raise SomeException(self.attempt)

    await with_retry(FailerUntil(2).__call__)()
    with pytest.raises(SomeException):
        await with_retry(FailerUntil(3).__call__)()


def assert_headers_with_roles(headers: Dict[str, str], roles: Optional[str]):
    if roles is None:
        assert constants.HEADER_ROLE not in headers
    else:
        assert headers[constants.HEADER_ROLE] == roles
    assert headers[constants.HEADER_USER] == "test_user"


@pytest.mark.asyncio
async def test_request_headers_role_hive_all(mock_get_and_post):
    get, post = mock_get_and_post
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            roles={"hive": "ALL"}
        ),
    )

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_with_roles(post_kwargs["headers"], "hive=ALL")

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_with_roles(post_kwargs["headers"], "hive=ALL")


@pytest.mark.asyncio
async def test_request_headers_role_admin(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            roles={"system": "admin"}
        ),
    )
    roles = "system=" + urllib.parse.quote("ROLE{admin}")

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_with_roles(post_kwargs["headers"], roles)

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_with_roles(post_kwargs["headers"], roles)


@pytest.mark.asyncio
async def test_request_headers_role_empty(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            roles=None,
        ),
    )

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_with_roles(post_kwargs["headers"], None)

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_with_roles(post_kwargs["headers"], None)


def assert_headers_timezone(headers: Dict[str, str], timezone: str):
    assert headers[constants.HEADER_TIMEZONE] == timezone


@pytest.mark.asyncio
async def test_request_headers_with_timezone(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            timezone="Europe/Brussels"
        ),
    )

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_timezone(post_kwargs["headers"], "Europe/Brussels")

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_timezone(post_kwargs["headers"], "Europe/Brussels")


@pytest.mark.asyncio
async def test_request_headers_without_timezone(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
        ),
    )
    localzone = get_localzone_name()

    await req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_timezone(post_kwargs["headers"], localzone)

    await req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_timezone(post_kwargs["headers"], localzone)


def test_request_with_invalid_timezone(mock_get_and_post):
    with pytest.raises(ZoneInfoNotFoundError) as zinfo_error:
        TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(
                user="test_user",
                timezone="INVALID_TIMEZONE"
            ),
        )
    assert str(zinfo_error.value).startswith("'No time zone found with key")
