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

import abc
from typing import Any

import aiohttp


class Authentication(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set_http_session(self, http_session: aiohttp.ClientSession) -> aiohttp.ClientSession:
        pass

    def get_exceptions(self) -> tuple[Any, ...]:
        return ()


class BasicAuthentication(Authentication):
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    def set_http_session(self, http_session: aiohttp.ClientSession) -> aiohttp.ClientSession:
        http_session._default_auth = aiohttp.BasicAuth(self._username, self._password)
        return http_session

    def get_exceptions(self):
        return ()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BasicAuthentication):
            return False
        return self._username == other._username and self._password == other._password


class JWTAuthentication(Authentication):
    def __init__(self, token: str):
        self.token = token

    def set_http_session(self, http_session: aiohttp.ClientSession) -> aiohttp.ClientSession:
        http_session.headers["Authorization"] = "Bearer " + self.token
        return http_session

    def get_exceptions(self) -> tuple[Any, ...]:
        return ()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JWTAuthentication):
            return False
        return self.token == other.token
