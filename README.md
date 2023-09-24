[![Build Status](https://github.com/mvanderlee/aiotrino/workflows/ci/badge.svg)](https://github.com/mvanderlee/aiotrino/actions?query=workflow%3Aci+event%3Apush+branch%3Apy3-async)
[![Trino Slack](https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358)](https://trino.io/slack.html)
[![Trino: The Definitive Guide book download](https://img.shields.io/badge/Trino%3A%20The%20Definitive%20Guide-download-brightgreen)](https://www.starburst.io/info/oreilly-trino-guide/)

# Introduction

This package provides a asyncio client interface to query [Trino](https://trino.io/)
a distributed SQL engine. It supports Python 3.7, 3.8, 3.9, 3.10, 3.11.
# Installation

```
$ pip install aiotrino
```

# Quick Start

Use the DBAPI interface to query Trino:

```python
import aiotrino
conn = aiotrino.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
)
cur = await conn.cursor()
await cur.execute('SELECT * FROM system.runtime.nodes')
rows = await cur.fetchall()
await conn.close()
```
Or with context manager 
```python
import aiotrino
async with aiotrino.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
) as conn:
    cur = await conn.cursor()
    await cur.execute('SELECT * FROM system.runtime.nodes')
    rows = await cur.fetchall()
```

This will query the `system.runtime.nodes` system tables that shows the nodes
in the Trino cluster.

The DBAPI implementation in `aiotrino.dbapi` provides methods to retrieve fewer
rows for example `Cursorfetchone()` or `Cursor.fetchmany()`. By default
`Cursor.fetchmany()` fetches one row. Please set
`trino.dbapi.Cursor.arraysize` accordingly.

For backwards compatibility with PrestoSQL, override the headers at the start of your application
```python
import aiotrino
aiotrino.constants.HEADERS = aiotrino.constants.PrestoHeaders
```

# Basic Authentication
The `BasicAuthentication` class can be used to connect to a LDAP-configured Trino
cluster:
```python
import aiotrino
conn = aiotrino.dbapi.connect(
    host='coordinator url',
    port=8443,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    http_scheme='https',
    auth=aiotrino.auth.BasicAuthentication("principal id", "password"),
)
cur = await conn.cursor()
await cur.execute('SELECT * FROM system.runtime.nodes')
rows = await cur.fetchall()
await conn.close()
```

# JWT Token Authentication
The `JWTAuthentication` class can be used to connect to a configured Trino cluster:
```python
import aiotrino
conn = aiotrino.dbapi.connect(
    host='coordinator url',
    port=8443,
    catalog='the-catalog',
    schema='the-schema',
    http_scheme='https',
    auth=aiotrino.auth.JWTAuthentication(token="jwt-token"),
)
cur = await conn.cursor()
await cur.execute('SELECT * FROM system.runtime.nodes')
rows = await cur.fetchall()
await conn.close()
```

# Transactions
The client runs by default in *autocommit* mode. To enable transactions, set
*isolation_level* to a value different than `IsolationLevel.AUTOCOMMIT`:

```python
import aiotrino
from aiotrino import transaction
async with aiotrino.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    isolation_level=transaction.IsolationLevel.REPEATABLE_READ,
) as conn:
  cur = await conn.cursor()
  await cur.execute('INSERT INTO sometable VALUES (1, 2, 3)')
  await cur.fetchone()
  await cur.execute('INSERT INTO sometable VALUES (4, 5, 6)')
  await cur.fetchone()
```

The transaction is created when the first SQL statement is executed.
`trino.dbapi.Connection.commit()` will be automatically called when the code
exits the *with* context and the queries succeed, otherwise
`trino.dbapi.Connection.rollback()' will be called.

# Development

## Getting Started With Development

Start by forking the repository and then modify the code in your fork.

Clone the repository and go inside the code directory. Then you can get the
version with `./setup.py --version`.

We recommend that you use `virtualenv` for development:

```
$ virtualenv .venv
$ . .venv/bin/activate
# TODO add requirements.txt: pip install -r requirements.txt
$ pip install .
```

For development purpose, pip can reference the code you are modifying in a
*virtualenv*:

```
$ pip install -e .[tests]
```

That way, you do not need to run `pip install` again to make your changes
applied to the *virtualenv*.

When the code is ready, submit a Pull Request.

## Code Style

- For Python code, adhere to PEP 8.
- Prefer code that is readable over one that is "clever".
- When writing a Git commit message, follow these [guidelines](https://chris.beams.io/posts/git-commit/).

## Running Tests

There is a helper scripts, `run`, that provides commands to run tests.
Type `./run tests` to run both unit and integration tests.

`aiotrino` uses [pytest](https://pytest.org/) for its tests. To run
only unit tests, type:

```
$ pytest tests
```

Then you can pass options like `--pdb` or anything supported by `pytest --help`.

To run the tests with different versions of Python in managed *virtualenvs*,
use `tox` (see the configuration in `tox.ini`):

```
$ tox
```

To run integration tests:

```
$ pytest integration_tests
```

They pull a Docker image and then run a container with a Trino server:
- the image is named `trinodb/trino:${TRINO_VERSION}`
- the container is named `aiotrino-python-client-tests-{uuid4()[:7]}`


### Test setup

Supported OS Ubuntu 22.04

1. Install [pyenv](https://github.com/pyenv/pyenv#automatic-installer)

    ```shell
    curl https://pyenv.run | bash
    ```

2. Install required python versions

    ```shell
    # Install the latest of all supported versions
    pyenv install 3.7, 3.8, 3.9, 3.10, 3.11
    ```

3. Set the installed versions as default for the shell. This allows `tox` to find them.

    List installed versions and update the following command as needed.
    ```shell
    pyenv versions
    ```

    ```shell
    pyenv shell 3.11.3 3.10.11 3.9.16 3.8.16 3.7.16
    ```

4. Install `tox`

    ```shell
    pip install tox
    ```

5. Run `tox`

    ```shell
    tox
    ```

## Releasing

- [Set up your development environment](#Getting-Started-With-Development).
- Change version in `aiotrino/__init__.py`.
- Commit and create an annotated tag (`git tag -m '' current_version`)
- Run the following:
  ```bash
  . .venv/bin/activate &&
  pip install twine wheel &&
  rm -rf dist/ &&
  ./setup.py sdist bdist_wheel &&
  twine upload dist/* &&
  open https://pypi.org/project/aiotrino/ &&
  echo "Released!"
  ```
- Push the branch and the tag (`git push upstream master current_version`)
- Send release announcement.

# Need Help?

Feel free to create an issue as it make your request visible to other users and contributors.

If an interactive discussion would be better or if you just want to hangout and chat about
the Trino Python client, you can join us on the *#python-client* channel on
[Trino Slack](https://trino.io/slack.html).
