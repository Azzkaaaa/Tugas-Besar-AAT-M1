ARG PYTHON_VERSION=3.15.1
ARG GO_VERSION=1.27.3

FROM python:${PYTHON_VERSION}-trixie AS python
FROM golang:${GO_VERSION}-trixie AS golang

FROM buildpack-deps:trixie

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ---------------------------------------------------------------------------
# System packages
#   - Python: the runtime deps docker-library/python keeps for tkinter,
#     bluetooth sockets, uuid, decimal (libmpdec) and compression.zstd.
#   - Go: binutils-gold, which cgo needs on arm64 (docker-library/golang#570).
#   - .NET: the deps listed in Microsoft's runtime-deps + sdk images. Microsoft
#     names version-specific runtime libs (libicu74, libssl3t64); the -dev
#     packages pull in whichever version this Debian release ships.
#   gcc, g++, make, pkg-config, git and gnupg already come with buildpack-deps.
# ---------------------------------------------------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        libbluetooth-dev \
        libmpdec-dev \
        libzstd-dev \
        tk-dev \
        uuid-dev \
        \
        libatomic1 \
        libicu-dev \
        libssl-dev \
        tzdata \
        tzdata-legacy \
    ; \
    if [ "$(dpkg --print-architecture)" = 'arm64' ]; then \
        apt-get install -y --no-install-recommends binutils-gold; \
    fi; \
    apt-get dist-clean

RUN groupadd --gid 1000 dev \
 && useradd --uid 1000 --gid dev --shell /bin/bash --create-home dev

# ---------------------------------------------------------------------------
# Python: copied from the official image (everything it adds lives in /usr/local)
# ---------------------------------------------------------------------------
COPY --link --from=python /usr/local/ /usr/local/
RUN set -eux; \
    ldconfig; \
    python3 --version; \
    pip3 --version; \
    python3 -c 'import sys, ssl, sqlite3, ctypes, lzma, bz2, zlib, readline, uuid, _decimal, tkinter; sys.version_info >= (3, 14) and __import__("compression.zstd")'

# ---------------------------------------------------------------------------
# Go: copied from the official image, with the same environment it sets
# ---------------------------------------------------------------------------
ENV GOTOOLCHAIN=local
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
COPY --link --from=golang /usr/local/go /usr/local/go
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 1777 "$GOPATH" \
 && go version

# ---------------------------------------------------------------------------
# Node.js: nodejs/docker-node recipe (release-key GPG check of SHASUMS256)
# ---------------------------------------------------------------------------
ARG NODE_VERSION=24.22.1
RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
        amd64) ARCH='x64' ;; \
        arm64) ARCH='arm64' ;; \
        *) echo >&2 "unsupported architecture"; exit 1 ;; \
    esac; \
    GNUPGHOME="$(mktemp -d)"; export GNUPGHOME; \
    # release keys: https://github.com/nodejs/node#release-keys
    for key in \
        5BE8A3F6C8A5C01D106C0AD820B1A390B168D356 \
        DD792F5973C6DE52C432CBDAC77ABFA00DDBF2B7 \
        CC68F5A3106FF448322E48ED27F5E38D5B0A215F \
        890C08DB8579162FEE0DF9DB8BEAB4DFCF555EF4 \
        C82FA3AE1CBEDC6BE46B9360C43CEC45C17AB93C \
        108F52B48DB57BB0CC439B2997B01419BD92F80A \
        655F3B5C1FB3FA8D1A0CA6BDE4A7D232B936D2FD \
        A363A499291CBBC940DD62E41F10027AF002F8B0 \
    ; do \
        { gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys "$key" && gpg --batch --fingerprint "$key"; } || \
        { gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$key" && gpg --batch --fingerprint "$key"; } ; \
    done; \
    tarball="node-v${NODE_VERSION}-linux-${ARCH}.tar.xz"; \
    curl -fsSLo "/tmp/${tarball}" --compressed "https://nodejs.org/dist/v${NODE_VERSION}/${tarball}"; \
    curl -fsSLo /tmp/SHASUMS256.txt.asc --compressed "https://nodejs.org/dist/v${NODE_VERSION}/SHASUMS256.txt.asc"; \
    gpg --batch --decrypt --output /tmp/SHASUMS256.txt /tmp/SHASUMS256.txt.asc; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME"; \
    grep " ${tarball}\$" /tmp/SHASUMS256.txt | sed "s| ${tarball}\$| /tmp/${tarball}|" | sha256sum -c -; \
    tar -xJf "/tmp/${tarball}" -C /usr/local --strip-components=1 --no-same-owner; \
    ln -s /usr/local/bin/node /usr/local/bin/nodejs; \
    rm -rf /tmp/*; \
    node --version; \
    npm --version

# ---------------------------------------------------------------------------
# Rust: rust-lang/docker-rust recipe (pinned, checksummed rustup-init; rustup
# then verifies the toolchain it downloads against the release manifest)
# ---------------------------------------------------------------------------
ARG RUST_VERSION=1.99.1
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo
ENV PATH=$CARGO_HOME/bin:$PATH
RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
        amd64) \
            rustArch='x86_64-unknown-linux-gnu'; \
            rustupSha256='5be191e8c70a5007074254cb74d78dbfdd25cd5a61c27de7b8e5410122ec24f4'; \
            ;; \
        arm64) \
            rustArch='aarch64-unknown-linux-gnu'; \
            rustupSha256='a9c168dbea81a84671c6b7a5c1795cb0eb699020110d107ddfca427bd797e697'; \
            ;; \
        *) echo >&2 "unsupported architecture"; exit 1 ;; \
    esac; \
    curl -fsSLo /tmp/rustup-init "https://static.rust-lang.org/rustup/archive/1.29.2/${rustArch}/rustup-init"; \
    echo "${rustupSha256} */tmp/rustup-init" | sha256sum -c -; \
    chmod +x /tmp/rustup-init; \
    /tmp/rustup-init -y --no-modify-path --profile minimal \
        --default-toolchain "$RUST_VERSION" --default-host "$rustArch" \
        --component rustfmt --component clippy; \
    rm /tmp/rustup-init; \
    chmod -R a+w "$RUSTUP_HOME" "$CARGO_HOME"; \
    rustup --version; \
    cargo --version; \
    rustc --version

# ---------------------------------------------------------------------------
# .NET SDK: dotnet/dotnet-docker recipe (official tarball + its .sha512)
# ---------------------------------------------------------------------------
ARG DOTNET_SDK_VERSION=10.0.403
ENV DOTNET_ROOT=/usr/share/dotnet \
    DOTNET_RUNNING_IN_CONTAINER=true \
    DOTNET_GENERATE_ASPNET_CERTIFICATE=false \
    DOTNET_NOLOGO=true \
    DOTNET_USE_POLLING_FILE_WATCHER=true \
    NUGET_XMLDOC_MODE=skip \
    DOTNET_CLI_TELEMETRY_OPTOUT=1

RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
        amd64) ARCH='x64' ;; \
        arm64) ARCH='arm64' ;; \
        *) echo >&2 "unsupported architecture"; exit 1 ;; \
    esac; \
    tarball="dotnet-sdk-${DOTNET_SDK_VERSION}-linux-${ARCH}.tar.gz"; \
    cd /tmp; \
    curl -fsSL \
        --remote-name "https://builds.dotnet.microsoft.com/dotnet/Sdk/${DOTNET_SDK_VERSION}/${tarball}" \
        --remote-name "https://builds.dotnet.microsoft.com/dotnet/Sdk/${DOTNET_SDK_VERSION}/${tarball}.sha512"; \
    sha512sum -c "${tarball}.sha512"; \
    mkdir -p "$DOTNET_ROOT"; \
    tar -xzf "${tarball}" -C "$DOTNET_ROOT" --no-same-owner; \
    rm -rf /tmp/*; \
    ln -s "$DOTNET_ROOT/dotnet" /usr/bin/dotnet; \
    ln -s "$DOTNET_ROOT/dnx" /usr/bin/dnx; \
    dotnet help > /dev/null; \
    dotnet --version

WORKDIR /workspace
CMD ["bash"]
