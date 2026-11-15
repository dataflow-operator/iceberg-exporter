FROM golang:1.25.9-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/iceberg-exporter ./cmd/iceberg-exporter

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/iceberg-exporter /usr/local/bin/iceberg-exporter
USER nobody
EXPOSE 8080
ENTRYPOINT ["iceberg-exporter"]
