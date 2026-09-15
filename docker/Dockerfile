# fetch-series, packaged for Nextflow.
#
# Deliberately minimal: this image resolves accessions and emits links.tsv. It
# does not download anything and does not need the SRA toolkit, STAR, samtools
# or any of the heavy tooling the reprocessing image carries. The incumbent
# bash module needed curl, jq and awk; this needs Python and two HTTP libraries.
#
# Build:
#   docker build -t fetch-series:0.1.0 -f docker/Dockerfile .
# Pin by digest in the Nextflow config, never by tag: a tag that moves under a
# running pipeline makes two samples in one batch resolve by different code.

FROM python:3.13-slim-bookworm AS build

# uv resolves and installs from the committed lockfile, so the image is built
# from exactly the versions the test suite ran against.
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/uv

WORKDIR /src
# The environment is built at the path it will be *used* from. A venv created at
# /src/.venv and copied to /opt/venv has console scripts whose shebang still
# names /src/.venv/bin/python, so every entry point fails with
# "exec: no such file or directory" -- and only at run time.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

# Dependencies first, so a source change does not re-resolve the environment.
COPY pyproject.toml uv.lock README.md LICENCE ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY src ./src
# --no-editable, or uv installs a .pth pointing at /src/src, which does not exist
# in the runtime stage: the venv imports cleanly here and raises
# ModuleNotFoundError there, at run time rather than at build time.
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-editable


FROM python:3.13-slim-bookworm

# ca-certificates is not optional: every archive this talks to is HTTPS, and
# without it every request fails with a certificate error that reads like a
# network outage.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Both stages are the same base image, so the interpreter the venv symlinks to
# is at the same path here.
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Nextflow runs processes as the invoking user against a bind-mounted work
# directory, so the container must not assume it can write anywhere else.
WORKDIR /work

# NCBI's rate limit is 3 requests/second without a key and 10 with one. The key
# is a query parameter, so it must arrive as an environment variable and never
# on a command line, where it would be visible in the process table and in
# Nextflow's own .command.sh.
ENV NCBI_API_KEY=""

# Fails the build if the entry point is broken, and records the version the way
# Nextflow's versions.yml wants it. The `set -e` and the emptiness check are not
# belt and braces: `$(fetch --version)` inside a printf swallows a non-zero exit,
# so the first version of this line wrote an empty versions.txt from an image
# whose entry point did not run at all, and reported success.
RUN set -eu; \
    version="$(fetch --version)"; \
    test -n "$version"; \
    printf 'fetch-series: %s\n' "$version" > /versions.txt

ENTRYPOINT ["fetch"]
CMD ["--help"]
