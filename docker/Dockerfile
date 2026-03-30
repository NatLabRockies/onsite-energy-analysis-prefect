FROM julia:1.11.9-trixie AS xpress-build
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /tmp/xpress

COPY onsite-xpress/xpress/install.sh ./install.sh
COPY onsite-xpress/xpress/xpauth.xpr ./xpauth.xpr
COPY onsite-xpress/xpress/xp9.7.0_linux_x86_64.tar.gz ./xp9.7.0_linux_x86_64.tar.gz

RUN set -eux; \
    bash ./install.sh -l static -a . -o -p -c mosel,cli,interfaces,insight-cli -x

FROM ghcr.io/astral-sh/uv:0.11.0 AS uv

FROM julia:1.11.9-trixie
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        libncurses6 \
        libsqlite3-0 \
        python3 \
        python3-dev; \
    rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /bin/
COPY --from=xpress-build /opt/xpressmp /opt/xpressmp
COPY onsite-xpress/xpress/certs/ /usr/local/share/ca-certificates/

RUN set -eux; \
    update-ca-certificates; \
    echo "/opt/xpressmp/lib" > /etc/ld.so.conf.d/xpress.conf; \
    ldconfig

ENV UV_LINK_MODE=copy \
    PYTHON=/usr/bin/python3 \
    PYCALL_JL_RUNTIME_PYTHON=/usr/bin/python3

WORKDIR /opt/onsite-energy-analysis-prefect

COPY pyproject.toml ./pyproject.toml
COPY uv.lock ./uv.lock
COPY .python-version ./.python-version

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

ENV PATH="/opt/onsite-energy-analysis-prefect/.venv/bin:/usr/local/julia/bin:${PATH}" \
    PYTHONPATH="/opt/onsite-energy-analysis-prefect" \
    ONSITE_PROJECT_ROOT="/onsite-energy-analysis"

COPY prefect.yaml ./prefect.yaml
COPY onsite_prefect ./onsite_prefect
COPY scripts ./scripts
COPY docker/entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]