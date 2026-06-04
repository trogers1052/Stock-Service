# Build healthcheck binary
FROM golang:1.25-alpine AS healthcheck-builder

WORKDIR /build
COPY cmd/healthcheck/main.go .
RUN go mod init healthcheck && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /healthcheck .

# Runtime stage
FROM gcr.io/distroless/base-debian12

WORKDIR /app

# Copy prebuilt ARM64 binary from CI
COPY dist/stocks-service /app/stocks-service

# Copy healthcheck binary
COPY --from=healthcheck-builder /healthcheck /app/healthcheck

# Copy migrations if needed at runtime
COPY db/migrations /app/db/migrations

EXPOSE 8081

USER nonroot:nonroot

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
    CMD ["/app/healthcheck"]

ENTRYPOINT ["/app/stocks-service"]
