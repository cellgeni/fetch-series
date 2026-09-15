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
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

# Dependencies first, so a source change does not re-resolve the environment.
COPY pyproject.toml uv.lock README.md LICENCE ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev


FROM python:3.13-slim-bookworm

# ca-certificates is not optional: every archive this talks to is HTTPS, and
# without it every request fails with a certificate error that reads like a
# network outage.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/.venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Nextflow runs processes as the invoking user against a bind-mounted work
# directory, so the container must not assume it can write anywhere else.
WORKDIR /work

# NCBI's rate limit is 3 requests/second without a key and 10 with one. The key
# is a query parameter, so it must arrive as an environment variable and never
# on a command line, where it would be visible in the process table and in
# Nextflow's own .command.sh.
ENV NCBI_API_KEY=""

RUN fetch --help > /dev/null && printf 'fetch-series: %s\n' "$(fetch --version 2>/dev/null || echo 0.1.0)" > /versions.txt

ENTRYPOINT ["fetch"]
CMD ["--help"]
