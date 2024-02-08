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

import ast
import re
import textwrap

from setuptools import setup

_version_re = re.compile(r"__version__\s+=\s+(.*)")


with open("aiotrino/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )

with open('README.md') as f:
    README = f.read()

kerberos_require = ["requests_kerberos"]

all_require = [kerberos_require]

tests_require = all_require + ["pytest", "pytest-aiohttp", "pytest-asyncio", "pytest-runner", "aioresponses", "click", "mock", "pytz"]

setup(
    name="aiotrino",
    author="Michiel Van Der Lee, Trino Team",
    author_email="jmt.vanderlee@gmail.com",
    version=version,
    url="https://github.com/mvanderlee/aiotrino/tree/main",
    packages=["aiotrino"],
    package_data={"": ["LICENSE", "README.md"]},
    description="ASyncIO Client for the Trino distributed SQL Engine",
    long_description=README,
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
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database :: Front-Ends",
    ],
    install_requires=["aiohttp"],
    extras_require={
        "all": all_require,
        "kerberos": kerberos_require,
        "tests": tests_require,
    },
)
