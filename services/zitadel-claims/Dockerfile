FROM rust:1.96-alpine AS build
WORKDIR /src
# Cache dependencies separately from the service sources.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/target/release/zitadel-claims /zitadel-claims
EXPOSE 3002
HEALTHCHECK --interval=5s --timeout=5s --retries=20 CMD ["/zitadel-claims", "--healthcheck"]
ENTRYPOINT ["/zitadel-claims"]
