FROM rust:1.95.0-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libfontconfig1-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --release --bin txgen-ethereum --bin txgen-tempo --bin txgen-tempo-property --bin bench

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 libfontconfig1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/txgen-tempo /usr/local/bin/txgen-tempo
COPY --from=builder /app/target/release/txgen-tempo-property /usr/local/bin/txgen-tempo-property
COPY --from=builder /app/target/release/txgen-ethereum /usr/local/bin/txgen-ethereum
COPY --from=builder /app/target/release/bench /usr/local/bin/bench
# Ship the example spec set under /specs so workflows / users can do
# `txgen-tempo generate --spec /specs/<name>.yaml` without mounting anything.
COPY --from=builder /app/examples/ /specs/
ENTRYPOINT ["txgen-tempo"]
