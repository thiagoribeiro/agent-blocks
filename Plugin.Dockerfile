FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /plugin-server ./cmd/plugin-server
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /plugin-server /usr/local/bin/plugin-server
EXPOSE 4010
ENTRYPOINT ["plugin-server"]
