FROM rust:1.82-slim-bookworm

# Install PostgreSQL build dependencies and cargo-pgrx
# For more information about cargo-pgrx, see https://github.com/pgcentralfoundation/pgrx
RUN apt-get update && apt-get install -y \
    libclang-dev \
    build-essential \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    libxml2-dev \
    libxslt-dev \
    libssl-dev \
    libxml2-utils \
    xsltproc \
    ccache \
    pkg-config

# Switch to the non-root user 
RUN useradd -m rustuser
USER rustuser
ENV USER=rustuser

# Install cargo-pgrx and initialize it
RUN cargo install cargo-pgrx && cargo pgrx init --jobs 4 --pg16 download

# setup dev tools
RUN rustup component add rustfmt clippy
