FROM rust:1.85-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libfontconfig1-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --bin txgen-tempo --bin bench

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 libfontconfig1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/txgen-tempo /usr/local/bin/txgen-tempo
COPY --from=builder /app/target/release/bench /usr/local/bin/bench
ENTRYPOINT ["txgen-tempo"]
