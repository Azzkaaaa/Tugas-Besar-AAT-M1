# Build context: repository root (contracts/proto and packages/bnpb-*-rust are needed).
#   docker build -f services/event-live-feed/Dockerfile .

# The codegen/ folders under packages/bnpb-*-rust are gitignored; generate them here.
FROM bufbuild/buf:1.73.0 AS proto
WORKDIR /workspace
COPY buf.yaml buf.lock ./
COPY contracts/proto contracts/proto
COPY packages/bnpb-common-rust packages/bnpb-common-rust
COPY packages/bnpb-event-rust packages/bnpb-event-rust
COPY packages/bnpb-feed-rust packages/bnpb-feed-rust
RUN for package in common event feed; do (cd packages/bnpb-$package-rust && buf generate) || exit 1; done

FROM rust:1.96-bookworm AS build
WORKDIR /workspace
COPY --from=proto /workspace/packages packages
# Cache dependencies separately from the service sources.
COPY services/event-live-feed/Cargo.toml services/event-live-feed/Cargo.lock services/event-live-feed/
WORKDIR /workspace/services/event-live-feed
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY services/event-live-feed/src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /workspace/services/event-live-feed/target/release/event-live-feed /event-live-feed
EXPOSE 9093
HEALTHCHECK --interval=5s --timeout=5s --retries=20 CMD ["/event-live-feed", "--healthcheck"]
ENTRYPOINT ["/event-live-feed"]
