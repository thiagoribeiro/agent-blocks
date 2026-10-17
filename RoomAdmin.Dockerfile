# Nimlet image providing the `nimlet` admin binary, built from github.com/thiagoribeiro/nimlet.
ARG NIMLET_IMAGE=nimlet:latest
FROM ${NIMLET_IMAGE} AS nimlet
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /room-admin ./cmd/room-admin
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 brian
COPY --from=builder /room-admin /usr/local/bin/room-admin
COPY --from=nimlet /usr/local/bin/nimlet /usr/local/bin/nimlet
USER brian
ENTRYPOINT ["room-admin"]
