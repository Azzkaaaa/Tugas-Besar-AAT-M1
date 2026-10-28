# Build context: repository root.
#   docker build -f services/token-exchange/Dockerfile .
FROM rust:1.96-alpine AS build
WORKDIR /src
# Cache dependencies separately from the service sources.
COPY services/token-exchange/Cargo.toml services/token-exchange/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY services/token-exchange/src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/target/release/token-exchange /token-exchange
EXPOSE 9000
HEALTHCHECK --interval=5s --timeout=5s --retries=20 CMD ["/token-exchange", "--healthcheck"]
ENTRYPOINT ["/token-exchange"]
