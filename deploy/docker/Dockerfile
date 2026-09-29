# syntax=docker/dockerfile:1.7
#
# Loomwork container image.
#
#   docker build -f deploy/docker/Dockerfile -t loomwork:0.1.0 .
#   docker build -f deploy/docker/Dockerfile --target demo -t loomwork-demo:0.1.0 .
#
# The `runtime` target contains Loomwork with the server, PostgreSQL, SQLite and
# encryption extras and runs `loomwork server` by default. Build your worker image FROM it
# and add your workflow code (see deploy/README.md). The `demo` target adds the example
# applications so the Compose stack has something to run.

ARG PYTHON_VERSION=3.12.7

# --------------------------------------------------------------------------- build
FROM python:${PYTHON_VERSION}-slim-bookworm AS build

COPY --from=ghcr.io/astral-sh/uv:0.9.27 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /src
# Dependencies first, so source changes do not invalidate the dependency layer.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv /opt/loomwork \
    && VIRTUAL_ENV=/opt/loomwork uv export --frozen --no-dev --no-emit-project \
        --extra server --extra postgres --extra sqlite --extra crypto \
        --format requirements-txt > /tmp/requirements.txt \
    && VIRTUAL_ENV=/opt/loomwork uv pip install --no-deps -r /tmp/requirements.txt

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    VIRTUAL_ENV=/opt/loomwork uv pip install --no-deps .

# --------------------------------------------------------------------------- runtime
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="loomwork" \
      org.opencontainers.image.description="Durable, event-sourced workflow engine" \
      org.opencontainers.image.source="https://github.com/integral0909/loomwork" \
      org.opencontainers.image.licenses="Apache-2.0"

# A fixed, unprivileged user; Kubernetes manifests rely on this uid. The secrets
# directory exists in the image so that a volume mounted there inherits its ownership.
RUN groupadd --system --gid 10001 loomwork \
    && useradd --system --uid 10001 --gid loomwork --home-dir /var/lib/loomwork \
        --shell /usr/sbin/nologin loomwork \
    && mkdir -p /var/lib/loomwork/secrets /etc/loomwork \
    && chown -R loomwork:loomwork /var/lib/loomwork \
    && chmod 0700 /var/lib/loomwork/secrets

COPY --from=build /opt/loomwork /opt/loomwork
ENV PATH=/opt/loomwork/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 10001:10001
WORKDIR /var/lib/loomwork
EXPOSE 8080

# The server's liveness endpoint; workers override this (they serve /healthz on their
# metrics port instead).
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import sys, urllib.request; sys.exit(urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).status != 200)"]

ENTRYPOINT ["loomwork"]
CMD ["server", "--config", "/etc/loomwork/server.toml"]

# --------------------------------------------------------------------------- demo
FROM runtime AS demo

COPY --chown=10001:10001 examples /opt/loomwork-demo/examples
ENV PYTHONPATH=/opt/loomwork-demo
