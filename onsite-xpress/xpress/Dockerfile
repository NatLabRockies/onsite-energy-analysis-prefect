FROM julia:1.11.9-trixie AS xpress-build
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /tmp/xpress

# Mount installer inputs from build context so they aren't persisted in image layers
RUN --mount=type=bind,source=install.sh,target=/tmp/xpress/install.sh,readonly \
    --mount=type=bind,source=xpauth.xpr,target=/tmp/xpress/xpauth.xpr,readonly \
    --mount=type=bind,source=xp9.7.0_linux_x86_64.tar.gz,target=/tmp/xpress/xp9.7.0_linux_x86_64.tar.gz,readonly \
    set -eux; \
    bash ./install.sh -l static -a . -o -p -c mosel,cli,interfaces,insight-cli -x;

FROM julia:1.11.9-trixie
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates libncurses6 libsqlite3-0; \
    rm -rf /var/lib/apt/lists/*

COPY --from=xpress-build /opt/xpressmp /opt/xpressmp

COPY --from=xpress-build /root/.bashrc /root/.bashrc

COPY certs/ /usr/local/share/ca-certificates/

# Make runtime linker aware of Xpress libs (works even when .bashrc is not sourced)
RUN set -eux; \
    update-ca-certificates; \
    echo "/opt/xpressmp/lib" > /etc/ld.so.conf.d/xpress.conf; \
    ldconfig
