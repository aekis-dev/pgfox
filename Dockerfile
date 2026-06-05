# Build stage
FROM golang:1.26-alpine AS builder

# Install build dependencies
RUN apk add --no-cache ca-certificates tzdata

# Set working directory
WORKDIR /pkg

# Copy the Go source code and module files
COPY pkg /pkg/
RUN go mod download

# Build the application with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o pgfox .

# Create final image
FROM alpine:3.22.2

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata postgresql-client

# Create non-root user
RUN addgroup -g 1000 pgfox && \
    adduser -D -u 1000 -G pgfox pgfox

# Create necessary directories. /var/lib/pgfox/data is where PgFox writes its
# CA and per-user certificates at runtime, so it MUST be owned by the pgfox
# user — otherwise the non-root process cannot generate certs. A fresh named
# volume mounted here inherits this ownership on first creation.
RUN mkdir -p /etc/pgfox /var/lib/pgfox/data /var/log/pgfox && \
    chown -R pgfox:pgfox /etc/pgfox /var/lib/pgfox /var/log/pgfox

# Set working directory
WORKDIR /var/lib/pgfox

# Copy binary from builder
COPY --from=builder /pkg/pgfox /usr/local/bin/pgfox

# Switch to non-root user
USER pgfox

# Expose ports: client listener and metrics
EXPOSE 5432 4502

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/pgfox"]

# Default command
CMD ["-config", "/etc/pgfox/config.yaml"]