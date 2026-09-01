# Context Engine Architecture

**Documentation:** [README](../README.md) · [Getting Started](GETTING_STARTED.md) · [Configuration](CONFIGURATION.md) · [IDE Clients](IDE_CLIENTS.md) · [MCP API](MCP_API.md) · [ctx CLI](CTX_CLI.md) · [Memory Guide](MEMORY_GUIDE.md) · [Architecture](ARCHITECTURE.md) · [Multi-Repo](MULTI_REPO_COLLECTIONS.md) · [Observability](OBSERVABILITY.md) · [Kubernetes](../deploy/kubernetes/README.md) · [VS Code Extension](vscode-extension.md) · [Troubleshooting](TROUBLESHOOTING.md) · [Development](DEVELOPMENT.md)

---

**On this page:**
- [Overview](#overview)
- [Core Principles](#core-principles)
- [System Architecture](#system-architecture)
- [Relevance Feedback System](#5-relevance-feedback-optional)
- [Data Flow](#data-flow)
- [ReFRAG Pipeline](#refrag-pipeline)

---

## Overview

Production-ready MCP (Model Context Protocol) retrieval stack unifying code indexing, hybrid search, and optional LLM decoding. Enables teams to ship context-aware AI agents with semantic and lexical search capabilities and dual-transport compatibility.

## Core Principles

- **Research-Grade Retrieval**: ReFRAG-inspired micro-chunking and span budgeting
- **Dual-Transport Support**: SSE (legacy) and HTTP RMCP (modern) protocols
- **Performance-First**: Intelligent caching, connection pooling, and async I/O
- **Production-Ready**: Comprehensive health checks, monitoring, and operational tooling

## System Architecture

### Component Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client Apps   │◄──►│  MCP Servers    │◄──►│   Qdrant DB     │
│ (IDE, CLI, Web) │    │  (SSE + HTTP)   │    │  (Vector Store) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │  LLM Decoder    │
                       │  (llama.cpp)    │
                       │   (Optional)    │
                       └─────────────────┘
```

## Core Components

### 1. MCP Servers

#### Memory Server (`scripts/mcp_memory_server.py`)
- **Purpose**: Knowledge base storage and retrieval
- **Transport**: SSE (port 8000) + HTTP RMCP (port 8002)
- **Key Features**:
  - Structured memory storage with rich metadata
  - Hybrid search (dense + lexical)
  - Dual vector support for embedding and lexical hashes
  - Automatic collection management

#### Indexer Server (`scripts/mcp_indexer_server.py`)
- **Purpose**: Code search, indexing, and management
- **Transport**: SSE (port 8001) + HTTP RMCP (port 8003)
- **Key Features**:
  - Hybrid code search with multiple filtering options
  - ReFRAG-inspired micro-chunking (16-token windows)
  - Context-aware Q&A with local LLM integration
  - Workspace and collection management
  - Live indexing and pruning capabilities

### 2. Search Pipeline

#### Hybrid Search Engine (`scripts/hybrid_search.py`)
- **Multi-Vector Architecture**:
  - **Dense Vectors**: Semantic embeddings (BAAI/bge-base-en-v1.5)
  - **Lexical Vectors**: BM25-style hashing (4096 dimensions)
  - **Mini Vectors**: ReFRAG gating (64 dimensions, optional)

- **Retrieval Process**:
  1. **Query Expansion**: Generate multiple query variations
  2. **Parallel Search**: Dense + lexical search with RRF fusion
  3. **Optional Reranking**: Cross-encoder neural reranking
  4. **Result Assembly**: Format with citations and metadata

- **Advanced Features**:
  - Request deduplication
  - Intelligent caching (multi-policy: LRU, LFU, TTL, FIFO)
  - Connection pooling to Qdrant
  - Batch processing support

#### ReFRAG Implementation
- **Micro-chunking**: Token-level windows (16 tokens, 8 stride)
- **Span Budgeting**: Global token budget management
- **Gate-First Filtering**: Mini-vector pre-filtering for efficiency

### 3. Storage Layer

#### Qdrant Vector Database
- **Primary Storage**: Embeddings and metadata
- **Collection Management**: Automatic creation and configuration
- **Named Vectors**: Separate storage for different embedding types
- **Performance**: HNSW indexing for fast approximate nearest neighbor search

#### Unified Cache System (`scripts/cache_manager.py`)
- **Eviction Policies**: LRU, LFU, TTL, FIFO
- **Memory Management**: Configurable size limits and monitoring
- **Thread Safety**: Proper locking for concurrent access
- **Statistics Tracking**: Hit rates, memory usage, eviction counts

### 4. Supporting Infrastructure

#### Async Subprocess Manager (`scripts/async_subprocess_manager.py`)
- **Process Management**: Async subprocess execution with resource cleanup
- **Connection Pooling**: Reused HTTP connections
- **Timeout Handling**: Configurable timeouts with graceful degradation
- **Resource Tracking**: Active process monitoring and statistics

#### Deduplication System (`scripts/deduplication.py`)
- **Request Deduplication**: Prevent redundant processing
- **Cache Integration**: Works with unified cache system
- **Performance Impact**: Significant reduction in duplicate work

#### Semantic Expansion (`scripts/semantic_expansion.py`)
- **Query Enhancement**: LLM-assisted query variation generation
- **Local LLM Integration**: llama.cpp for offline expansion
- **Caching**: Expanded query results cached for reuse

#### Pseudo Descriptions
- **Index-time vocabulary bridge**: Optional LLM-generated pseudo descriptions
  and tags are stored with chunks and can be included in dense indexing text via
  `INDEX_DENSE_MODE=info+pseudo+tags`.
- **Lexical participation**: Pseudo/tags are appended to lexical text during
  indexing and can contribute to lexical scoring when `HYBRID_PSEUDO_BOOST` is
  enabled.
- **Not reranker-only**: These fields are carried through search results and are
  already part of retrieval when indexing/search env knobs enable them.

### 5. Relevance Feedback (Optional)

Per-collection feedback via `rate_search_results` MCP tool. Agents or users
rate search results (0=not used, 1=glanced, 2=directly used) and a background
trainer aggregates ratings into per-collection weight files.

Feedback uses stable target identity rather than exact line spans:
- `result_id` / `target_id`: stable symbol-or-file target used for recall and boosts
- `impression_id`: query/content/span-specific diagnostic ID

Identity and reindex behavior:
- Line shifts and body edits keep feedback when repo, kind, and symbol identity stay
  the same.
- File moves keep symbol feedback because the path is not part of symbol target
  identity.
- Exact-body symbol renames are reconciled during smart reindex using the
  symbol-level content hash.
- A removed symbol may transfer feedback to multiple extracted successors only
  when their token overlap collectively covers the old body above conservative
  thresholds. The inherited weight is divided between successors.
- Ambiguous renames/splits are not migrated. The old feedback remains recorded
  but does not attach itself to an uncertain target.

Recent result metadata used by hands-off ratings is persisted per collection in
the shared feedback volume. Search and rating calls therefore do not need to hit
the same server process.

Subsequent searches can rehydrate positively-rated targets that ordinary
retrieval missed, then apply a soft `relevance_boost`. Feedback adds candidates
and nudges rank; it does not force rated targets to win.

For adjacent-code discovery, feedback recall uses inverse graph lookups against
the materialized Qdrant graph collection: a rated callee symbol can produce
candidate caller paths via `callee_symbol -> caller_path`, which are then
hydrated from the main code collection.

Components:
- **Event Logger** (`scripts/rerank_tools/events.py`): NDJSON-based event files
- **Relevance Trainer** (`scripts/relevance_trainer.py`): Aggregates ratings,
  preserves rehydratable target metadata, writes per-collection weight files
  atomically
- **Feedback Recall + Boost** (`scripts/mcp_impl/search.py`): Reads weight files
  at search time, rehydrates a small number of rated targets and inverse-graph
  caller candidates from Qdrant, then applies `relevance_boost` to target IDs

Key env vars: `RELEVANCE_BOOST_FACTOR`, `RELEVANCE_RECALL_MAX`,
`RELEVANCE_GRAPH_RECALL_MAX`, `RERANKER_WEIGHTS_DIR`, `RERANK_EVENTS_DIR`

Note: This replaces a previous self-supervised ML pipeline (`rerank_recursive/`,
`learning_reranker_worker.py`) that tried to learn from ONNX reranker outputs.
That system was removed in favor of explicit human/agent ratings.

## Data Flow Architecture

### Search Request Flow
```
1. Client Query → MCP Server
2. Query Expansion (optional) → Multiple Query Variations
3. Parallel Execution → Dense Search + Lexical Search
4. RRF Fusion → Combined Results
5. Reranking (optional) → Enhanced Relevance
6. Result Formatting → Structured Response with Citations
7. Return to Client → MCP Protocol Response
```

### Indexing Flow
```
1. File Change Detection → File System Watcher
2. Content Processing → Tokenization + Chunking
3. Embedding Generation → Model Inference
4. Vector Creation → Dense + Lexical + Mini
5. Metadata Assembly → Path, symbols, language, etc.
6. Batch Upsert → Qdrant Storage
7. Cache Updates → Local Cache Refresh
```

## Configuration Architecture

### Environment-Based Configuration
- **Docker-Native**: All configuration via environment variables
- **Development Support**: Local .env file configuration
- **Production Ready**: External secret management integration

### Key Configuration Areas
- **Service Configuration**: Ports, hosts, transport protocols
- **Model Configuration**: Embedding models, reranker settings
- **Performance Tuning**: Cache sizes, batch sizes, timeouts
- **Feature Flags**: Experimental features, debug modes

## Transport Layer Architecture

### Dual-Transport Design
- **SSE (Server-Sent Events)**: Legacy client compatibility
- **HTTP RMCP**: Modern JSON-RPC over HTTP
- **Simultaneous Operation**: Both protocols can run together
- **Automatic Fallback**: Graceful degradation when transport fails

### MCP Protocol Implementation
- **FastMCP Framework**: Modern MCP server implementation
- **Tool Registry**: Automatic tool discovery and registration
- **Health Endpoints**: `/readyz` and `/tools` endpoints
- **Error Handling**: Structured error responses and logging

## Performance Architecture

### Caching Strategy
- **Multi-Level Caching**: Embedding cache, search cache, expansion cache
- **Intelligent Invalidation**: TTL-based and LRU eviction
- **Memory Management**: Configurable limits and monitoring
- **Performance Monitoring**: Hit rates, response times, memory usage

### Concurrency Model
- **Async I/O**: Non-blocking operations throughout
- **Connection Pooling**: Reused connections to external services
- **Batch Processing**: Efficient bulk operations
- **Resource Management**: Proper cleanup and resource limits

## Security Architecture

### Isolation and Safety
- **Container-Based**: Docker isolation for all services
- **Network Segmentation**: Internal service communication
- **Input Validation**: Comprehensive parameter validation
- **Resource Limits**: Configurable timeouts and memory limits

### Data Protection
- **No Hardcoded Secrets**: Environment-based configuration
- **API Key Management**: External secret manager integration
- **Audit Logging**: Structured logging for security events

## Operational Architecture

### Health Monitoring
- **Service Health**: `/readyz` endpoints for all services
- **Tool Availability**: Dynamic tool listing and status
- **Performance Metrics**: Response times, cache statistics
- **Error Tracking**: Structured error logging and alerting

### Deployment Patterns
- **Docker Compose**: Multi-service orchestration
- **Environment Parity**: Development ↔ Production consistency
- **Graceful Shutdown**: Proper resource cleanup on termination
- **Rolling Updates**: Zero-downtime deployment support

## Extensibility Architecture

### Plugin System
- **MCP Tool Extension**: Easy addition of new tools
- **Transport Flexibility**: Support for future MCP transports
- **Model Pluggability**: Support for different embedding models
- **Storage Abstraction**: Potential for alternative vector stores

### Configuration Extension
- **Environment-Driven**: Easy configuration via environment variables
- **Feature Flags**: Experimental feature toggling
- **A/B Testing**: Multiple configuration variants support

This architecture enables Context Engine to serve as a production-ready, scalable context layer for AI applications while maintaining the flexibility to evolve with changing requirements and technologies.
