#!/usr/bin/env python

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

import os
from codecs import open
from typing import Any

from setuptools import find_packages, setup

about: dict[str, Any] = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "aiotrino", "_version.py"), "r", "utf-8") as f:
    exec(f.read(), about)

with open(os.path.join(here, "README.md"), "r", "utf-8") as f:
    readme = f.read()

sqlalchemy_require = ["sqlalchemy >= 1.4"]

all_require = sqlalchemy_require

tests_require = all_require + [
    # httpretty >= 1.1 duplicates requests in `httpretty.latest_requests`
    # https://github.com/gabrielfalcao/HTTPretty/issues/425
    "httpretty < 1.1",
    # httppretty compatible but with asyncio support
    "mocket[speedups]",
    "pytest",
    "pytest-aiohttp",
    "pytest-asyncio",
    "pytest-runner",
    "aioresponses",
    "mock",
    "pytz",
    "pre-commit",
    "ruff",
    "isort",
    "testcontainers",
    "boto3"
]

setup(
    name=about["__title__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    version=about["__version__"],
    url=about["__url__"],
    packages=find_packages(include=["aiotrino", "aiotrino.*"]),
    package_data={"": ["LICENSE", "README.md"]},
    description=about["__description__"],
    long_description=readme,
    long_description_content_type="text/markdown",
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database :: Front-Ends",
    ],
    python_requires=">=3.9",
    install_requires=[
        "aioitertools",
        "lz4",
        "python-dateutil",
        "pytz",
        "aiohttp",
        "tzdata",
        "tzlocal",
        "zstandard",
    ],
    extras_require={
        "all": all_require,
        "sqlalchemy": sqlalchemy_require,
        "tests": tests_require,
    },
    entry_points={
        "sqlalchemy.dialects": [
            "aiotrino = aiotrino.sqlalchemy.dialect:AIOTrinoDialect",
        ]
    },
)
