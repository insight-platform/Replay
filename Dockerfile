FROM rust:1.78 AS builder

WORKDIR /opt/replay
COPY . .
COPY ./replay/assets/test.json /opt/etc/config.json

RUN build/docker-deps.sh
RUN cargo build --release
RUN build/copy-deps.sh
RUN cargo clean

FROM debian:bookworm-slim AS runner

COPY --from=builder /opt /opt

WORKDIR /opt/replay

ENV LD_LIBRARY_PATH=/opt/libs
ENV DB_PATH=/opt/rocksdb
ENV RUST_LOG=info

EXPOSE 8080
EXPOSE 5555

ENTRYPOINT ["/opt/bin/replay"]
CMD ["/opt/etc/config.json"]
