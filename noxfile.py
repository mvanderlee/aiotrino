import os

from nox import Session, options, parametrize  # pyright: ignore[reportMissingImports]
from nox_uv import session  # pyright: ignore[reportMissingImports]


options.default_venv_backend = "uv"

SQLALCHEMY_VERSIONS = {
    "2.0": "sqlalchemy[asyncio]>=2.0,<2.1",
    "2.1": "sqlalchemy[asyncio]>=2.1,<2.2",
}


@session(python=["3.12", "3.13", "3.14"], uv_extras=["all"], uv_groups=["dev"])
@parametrize("sqlalchemy", list(SQLALCHEMY_VERSIONS))
def test(s: Session, sqlalchemy: str) -> None:
    s.install(SQLALCHEMY_VERSIONS[sqlalchemy])
    py = s.python
    suffix = f"{py}-sqlalchemy{sqlalchemy}"
    s.run(
        "python",
        "-m",
        "pytest",
        "--cov=aiotrino",
        "--cov-report=term-missing",
        f"--cov-report=xml:coverage-{suffix}.xml",
        f"--junitxml=junit-{suffix}.xml",
    )
    # Publish the coverage table to the GitHub Actions run summary when in CI.
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        heading = f"## Coverage (Python {py}, SQLAlchemy {sqlalchemy})"
        s.run("bash", "-c", f'{{ echo "{heading}"; coverage report --format=markdown; }} >> "{summary}"')


@session(uv_groups=["dev"])
def lint(s: Session) -> None:
    s.run("ruff", "check", "aiotrino")


@session(uv_groups=["dev"])
def format(s: Session) -> None:
    s.run("ruff", "format", "--check", "aiotrino")
