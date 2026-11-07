
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
