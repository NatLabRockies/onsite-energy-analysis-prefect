FROM julia:1.11.9-trixie AS julia-base

FROM julia:1.11.9-trixie AS xpress-build
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /tmp/xpress

COPY onsite-xpress/xpress/install.sh ./install.sh
COPY onsite-xpress/xpress/xpauth.xpr ./xpauth.xpr
COPY onsite-xpress/xpress/xp9.7.0_linux_x86_64.tar.gz ./xp9.7.0_linux_x86_64.tar.gz

RUN set -eux; \
    bash ./install.sh -l static -a . -o -p -c mosel,cli,interfaces,insight-cli -x

FROM prefecthq/prefect:3-latest
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends bash ca-certificates libncurses6 libsqlite3-0; \
    rm -rf /var/lib/apt/lists/*

COPY --from=julia-base /usr/local/julia /usr/local/julia
COPY --from=xpress-build /opt/xpressmp /opt/xpressmp
COPY onsite-xpress/xpress/certs/ /usr/local/share/ca-certificates/

RUN set -eux; \
    update-ca-certificates; \
    echo "/opt/xpressmp/lib" > /etc/ld.so.conf.d/xpress.conf; \
    ldconfig

ENV PATH="/usr/local/julia/bin:${PATH}" \
    PYTHONPATH="/opt/onsite-energy-analysis-prefect" \
    ONSITE_PROJECT_ROOT="/onsite-energy-analysis"

WORKDIR /opt/onsite-energy-analysis-prefect

COPY pyproject.toml ./pyproject.toml
COPY prefect.yaml ./prefect.yaml
COPY onsite_prefect ./onsite_prefect
COPY scripts ./scripts
COPY docker/entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh /opt/onsite-energy-analysis-prefect/scripts/bootstrap_prefect.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
