import json
import re
from urllib.parse import quote, quote_plus, urlparse

from sqlalchemy import exc


def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: f"%{ord(m.group(0)):X}", text)


def _assert_valid_host(host: str) -> None:
    # Parse with a leading "//" so a bare hostname is treated as the authority rather than a path.
    # urlparse(host).scheme catches an embedded scheme.
    try:
        parsed = urlparse("//" + host)
        invalid = bool(
            urlparse(host).scheme or parsed.port is not None or parsed.path or parsed.username or parsed.password
        )
    except ValueError:
        # A malformed port or IPv6 literal raises here.
        invalid = True
    if invalid:
        raise exc.ArgumentError(
            f"host must be a hostname only, without a scheme, port, path, or credentials. Got {host!r}. "
            "For example, use 'example.com' instead of 'https://example.com' and pass the port via the "
            "'port' argument."
        )


def _url(
    host: str,
    port: int | None = 8080,
    user: str | None = None,
    password: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    source: str | None = "aiotrino-sqlalchemy",
    session_properties: dict[str, str] | None = None,
    http_headers: dict[str, str | int] | None = None,
    extra_credential: list[tuple[str, str]] | None = None,
    client_tags: list[str] | None = None,
    legacy_primitive_types: bool | None = None,
    legacy_prepared_statements: bool | None = None,
    access_token: str | None = None,
    cert: str | None = None,
    key: str | None = None,
    verify: bool | None = None,
    roles: dict[str, str] | None = None,
    allow_insecure_auth: bool | None = None,
) -> str:
    """
    Composes a SQLAlchemy connection string from the given database connection
    parameters.
    Parameters containing special characters (e.g., '@', '%') need to be encoded to be parsed correctly.
    """

    trino_url = "aiotrino://"

    if user is not None:
        trino_url += _rfc_1738_quote(user)

    if password is not None:
        if user is None:
            raise exc.ArgumentError("user must be specified when specifying a password.")
        trino_url += f":{_rfc_1738_quote(password)}"

    if user is not None:
        trino_url += "@"

    if not host:
        raise exc.ArgumentError("host must be specified.")

    _assert_valid_host(host)

    trino_url += host

    if not port:
        raise exc.ArgumentError("port must be specified.")

    trino_url += f":{port}/"

    if catalog is not None:
        trino_url += quote(catalog, safe="")

    if schema is not None:
        if catalog is None:
            raise exc.ArgumentError("catalog must be specified when specifying a default schema.")
        trino_url += f"/{quote(schema, safe='')}"

    assert source
    trino_url += f"?source={quote_plus(source)}"

    if session_properties is not None:
        trino_url += f"&session_properties={quote_plus(json.dumps(session_properties))}"

    if http_headers is not None:
        trino_url += f"&http_headers={quote_plus(json.dumps(http_headers))}"

    if extra_credential is not None:
        # repr is used here as json.dumps converts tuples into arrays
        trino_url += f"&extra_credential={quote_plus(json.dumps(extra_credential))}"

    if client_tags is not None:
        trino_url += f"&client_tags={quote_plus(json.dumps(client_tags))}"

    if legacy_primitive_types is not None:
        trino_url += f"&legacy_primitive_types={json.dumps(legacy_primitive_types)}"

    if legacy_prepared_statements is not None:
        trino_url += f"&legacy_prepared_statements={json.dumps(legacy_prepared_statements)}"

    if access_token is not None:
        trino_url += f"&access_token={quote_plus(access_token)}"

    if cert is not None:
        trino_url += f"&cert={quote_plus(cert)}"

    if key is not None:
        trino_url += f"&key={quote_plus(key)}"

    if verify is not None:
        trino_url += f"&verify={json.dumps(verify)}"

    if roles is not None:
        trino_url += f"&roles={quote_plus(json.dumps(roles))}"

    if allow_insecure_auth is not None:
        trino_url += f"&allow_insecure_auth={json.dumps(allow_insecure_auth)}"

    return trino_url
