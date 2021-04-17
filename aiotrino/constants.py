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

from typing import Any, Optional, Text  # NOQA: mypy types

DEFAULT_PORT = 8080
DEFAULT_SOURCE = "trino-python-client"
DEFAULT_CATALOG = None  # type: Optional[Text]
DEFAULT_SCHEMA = None  # type: Optional[Text]
DEFAULT_AUTH = None  # type: Optional[Any]
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_REQUEST_TIMEOUT = 30.0  # type: float

HTTP = "http"
HTTPS = "https"

URL_STATEMENT_PATH = "/v1/statement"


class TrinoHeaders:
    CATALOG = "X-Trino-Catalog"
    SCHEMA = "X-Trino-Schema"
    SOURCE = "X-Trino-Source"
    USER = "X-Trino-User"
    CLIENT_INFO = "X-Trino-Client-Info"

    SESSION = "X-Trino-Session"
    SET_SESSION = "X-Trino-Set-Session"
    CLEAR_SESSION = "X-Trino-Clear-Session"

    STARTED_TRANSACTION = "X-Trino-Started-Transaction-Id"
    TRANSACTION = "X-Trino-Transaction-Id"

    PREPARED_STATEMENT = 'X-Trino-Prepared-Statement'
    ADDED_PREPARE = 'X-Trino-Added-Prepare'
    DEALLOCATED_PREPARE = 'X-Trino-Deallocated-Prepare'


class PrestoHeaders:
    CATALOG = "X-Presto-Catalog"
    SCHEMA = "X-Presto-Schema"
    SOURCE = "X-Presto-Source"
    USER = "X-Presto-User"
    CLIENT_INFO = "X-Presto-Client-Info"

    SESSION = "X-Presto-Session"
    SET_SESSION = "X-Presto-Set-Session"
    CLEAR_SESSION = "X-Presto-Clear-Session"

    STARTED_TRANSACTION = "X-Presto-Started-Transaction-Id"
    TRANSACTION = "X-Presto-Transaction-Id"

    PREPARED_STATEMENT = 'X-Presto-Prepared-Statement'
    ADDED_PREPARE = 'X-Presto-Added-Prepare'
    DEALLOCATED_PREPARE = 'X-Presto-Deallocated-Prepare'


HEADERS = TrinoHeaders
