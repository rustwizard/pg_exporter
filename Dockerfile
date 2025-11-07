FROM rust:1.91-slim AS cargo-build

RUN apt-get update && apt-get -y install libssl-dev pkg-config ca-certificates

WORKDIR /usr/src/pg_exporter

COPY ./src ./src
COPY ./Cargo.toml ./Cargo.toml
COPY ./pg_exporter.yml ./pg_exporter.yml
RUN cargo build --release

FROM debian:12-slim

RUN apt-get update && apt-get -y install libssl-dev openssl ca-certificates

WORKDIR /home/pg_exporter/bin/

COPY --from=cargo-build /usr/src/pg_exporter/target/release/pg_exporter .
COPY --from=cargo-build /usr/src/pg_exporter/pg_exporter.yml .

EXPOSE 61488

ENTRYPOINT ["./pg_exporter"]