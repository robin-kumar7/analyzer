# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the analyzer HTTP service.
# Build:  podman build -t vibe/analyzer -f analyzer/Containerfile ./analyzer
# Run:    podman run --rm -p 8081:8081 vibe/analyzer
#
# This file is named `Containerfile` (podman's native name). Podman and
# Docker both build it transparently — docker users can also run:
#   docker build -t vibe/analyzer -f analyzer/Containerfile ./analyzer

# ---------- build stage ----------
FROM golang:1.26-alpine AS build

ENV CGO_ENABLED=0 \
    GOFLAGS=-trimpath

WORKDIR /src

RUN apk add --no-cache ca-certificates git

# Cache module downloads in a separate layer
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy the rest of the source
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/analyzer \
        ./cmd/analyzer

# ---------- runtime stage ----------
FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates tzdata curl && \
    addgroup -S app && adduser -S -G app -h /app app

WORKDIR /app
USER app

COPY --from=build /out/analyzer /usr/local/bin/analyzer

EXPOSE 8081

# Default to `serve`; override with `docker run ... analyzer version`, etc.
ENTRYPOINT ["analyzer"]
CMD ["serve"]
