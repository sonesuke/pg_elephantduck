#
# This part is used to develop the pg_elephantduck extension.
#
FROM rust:1.82-slim-bookworm AS development

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
RUN cargo install cargo-pgrx@0.12.8 && cargo pgrx init --jobs 4 --pg16 download

# setup dev tools
RUN rustup component add rustfmt clippy


#
# This part is used to build the pg_elephantduck extension. 
#
FROM development AS builder

COPY . /home/rustuser/app
WORKDIR /home/rustuser/app

USER root
RUN chown -R rustuser:rustuser /home/rustuser/app
USER rustuser

ENV PATH=/home/rustuser/.pgrx/16.6/pgrx-install/bin:$PATH

RUN cargo pgrx install


#
# This part is used to deploy this extension to the PostgreSQL database with a configuration of streaming replication.
#
FROM postgres:16.6-bookworm AS database-base

COPY --from=builder /home/rustuser/.pgrx/16.6/pgrx-install/share/postgresql/extension/pg_elephantduck--0.0.0.sql /usr/share/postgresql/16/extension
COPY --from=builder /home/rustuser/.pgrx/16.6/pgrx-install/share/postgresql/extension/pg_elephantduck.control /usr/share/postgresql/16/extension
COPY --from=builder /home/rustuser/.pgrx/16.6/pgrx-install/lib/postgresql/pg_elephantduck.so /usr/lib/postgresql/16/lib

# Set credentials for PostgreSQL
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=mysecretpassword

# Set the archive directory
RUN mkdir /var/lib/postgresql/archive
RUN chown -R postgres:postgres /var/lib/postgresql/archive
