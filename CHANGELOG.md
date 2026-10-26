# Changelog

All notable changes to aiotrino-patched since it was forked from
[mvanderlee/aiotrino](https://github.com/mvanderlee/aiotrino).

## [0.6.0] - 2026-10-26

### Features

- **sqlalchemy:** Support SQLAlchemy 2.1 alongside 2.0

### Miscellaneous

- **make:** Fail on unknown targets
- Drop agent tooling entries from .gitignore
- Test against SQLAlchemy 2.0 and 2.1

## [0.5.1] - 2026-10-01

### Miscellaneous

- **release:** Generate CHANGELOG.md with git-cliff

## [0.5.0] - 2026-10-01

### Features

- **client:** Catch up with trino-python-client 0.340.0

### Miscellaneous

- Bump dependencies

## [0.4.0] - 2026-09-14

### Features

- **client:** Catch up with trino-python-client 0.338.0
- **client:** Lazily load spooled result set segments ([#6](https://github.com/gledi-ai/aiotrino/pull/6))
- **client:** Catch up with trino-python-client 0.339.0
- **client:** Retry empty HTTP 200 statement responses

### Bug Fixes

- **tests:** Pass stream_writer to ClientResponse for aiohttp 3.14 version bump
- **client:** Surface empty 200 with JSON content type as TrinoConnectionError

### Miscellaneous

- Dependency upgrades
- **dev:** Add Makefile and wire test/coverage reporting in CI
- Bump actions/upload-artifact v4 -> v7 for Node 24 runtime
- **deps:** Upgrade uv.lock to latest compatible versions
- **coverage:** Hide fully covered files from terminal report
- **make:** Add test/cov alias for cov target

## [0.3.5] - 2026-05-15

### Features

- **sqlalchemy:** Warn and drop unsupported PK/FK/UNIQUE constraints
- **client:** Tolerate missing lz4 / zstandard at runtime

### Bug Fixes

- **client:** Use default TLS port (443) when scheme is https
- **client:** Infer http_scheme from port when not given explicitly
- **client:** Send original user via X-Trino-Original-User on impersonation
- **sqlalchemy:** Forward kwargs through compile_ignore_nulls

### Performance

- **client:** Defer tzlocal import until needed

### Miscellaneous

- Upgrade action to non-deprecated version
- **typing:** Mark package as PEP-561 compatible

## [0.3.4] - 2026-05-15

### Features

- Add release action

### Miscellaneous

- Clarify release process with hatch-vcs
- **dependencies:** Update dependencies
- Trigger release pipeline on tag push
- Update to supported action versions
- Skip ci workflow on tag pushes

## [0.3.3] - 2026-05-14

### Miscellaneous

- Manage versions from git tags

## [0.3.2] - 2026-05-14

### Miscellaneous

- Some aesthetic project config changes
- Autoformat

## [0.3.1] - 2026-02-01

### Bug Fixes

- Fix/sqlalchemy upgrade soft close ([#2](https://github.com/gledi-ai/aiotrino/pull/2))

### Miscellaneous

- Chore/modernize ([#1](https://github.com/gledi-ai/aiotrino/pull/1))
- **release:** Prepare for release ([#3](https://github.com/gledi-ai/aiotrino/pull/3))

