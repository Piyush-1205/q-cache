
# Q-Cache

A lightweight, self-hostable, in-memory key-value cache
with native HTTP/3 over QUIC support.

## Vision

Build a high-performance caching engine designed to
investigate efficient data access for modern backend
and ephemeral serverless workloads.

## Core Features

- In-memory key-value storage
- Binary-safe values
- TTL-based expiration
- LRU eviction
- Configurable memory limits
- Concurrent request handling
- HTTP/1.1 support
- Native HTTP/3 over QUIC
- Authentication and observability
- Single-binary deployment

## Technology Stack

- Go
- HTTP/1.1
- HTTP/3
- QUIC
- Docker

## Project Status

Under active development.

## Current Scope

Initial release targets a single-node, non-persistent
in-memory caching engine.

Distributed clustering and replication are future goals.



## HTTP/1.1 and HTTP/3 Support

Q-Cache supports two HTTP transports that share the same API handler and
in-memory storage engine.

| Protocol | Transport | Address |
|---|---|---|
| HTTP/1.1 | TCP | 127.0.0.1:8080 |
| HTTP/3 | QUIC over UDP | 127.0.0.1:8443 |

### HTTP/3 Development Setup

HTTP/3 requires TLS 1.3. For local development, generate a self-signed
certificate using OpenSSL.

Run these commands from the project root:

```bash
mkdir -p dev-certs

openssl req -x509 -newkey rsa:2048 \
  -keyout dev-certs/server.key \
  -out dev-certs/server.crt \
  -sha256 \
  -days 365 \
  -nodes \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"