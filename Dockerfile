FROM golang:1.26-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /agent-blocks ./cmd/router

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /agent-blocks /usr/local/bin/agent-blocks

EXPOSE 4000
ENTRYPOINT ["agent-blocks"]
