[![CI](https://github.com/m1rl0k/Context-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/m1rl0k/Context-Engine/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@context-engine-bridge/context-engine-mcp-bridge.svg)](https://www.npmjs.com/package/@context-engine-bridge/context-engine-mcp-bridge)
[![VS Code Marketplace](https://img.shields.io/visual-studio-marketplace/i/context-engine.context-engine-uploader.svg?label=VS%20Code)](https://marketplace.visualstudio.com/items?itemName=context-engine.context-engine-uploader)
[![Join our Discord](https://img.shields.io/badge/Discord-Join%20Chat-5865F2?logo=discord&logoColor=white)](https://discord.gg/tCxvqmP4QT)

**Documentation:** [Getting Started](docs/GETTING_STARTED.md) · README · [Configuration](docs/CONFIGURATION.md) · [IDE Clients](docs/IDE_CLIENTS.md) · [MCP API](docs/MCP_API.md) · [ctx CLI](docs/CTX_CLI.md) · [Memory Guide](docs/MEMORY_GUIDE.md) · [Architecture](docs/ARCHITECTURE.md) · [Multi-Repo](docs/MULTI_REPO_COLLECTIONS.md) · [Observability](docs/OBSERVABILITY.md) · [Kubernetes](deploy/kubernetes/README.md) · [VS Code Extension](docs/vscode-extension.md) · [Troubleshooting](docs/TROUBLESHOOTING.md) · [Development](docs/DEVELOPMENT.md)

---

## Context-Engine

Open-source code search engine for AI coding agents — hybrid retrieval with cross-encoder reranking.

<p align="center">
  <img src="useage.png" alt="Context-Engine Usage" width="50%"/>
</p>

---

## Quick Start: Stack in 30 Seconds

### VS Code Extension (Easiest)
1. Install [Context Engine Uploader](https://marketplace.visualstudio.com/items?itemName=context-engine.context-engine-uploader)
2. Open any project → extension prompts to set up Context-Engine stack
3. Opened workspace is indexed
4. MCP configs can configure your agent/IDE

**That's it!** The extension handles everything:
- Clones Context-Engine to your chosen location (keeps it separate from your project)
- Starts the Docker stack automatically
- Sets up MCP bridge configuration
- Writes MCP configs for Claude Code, Windsurf, and Augment

**Claude Code users:** Install the skill plugin:
```
/plugin marketplace add m1rl0k/Context-Engine
/plugin install context-engine
```

### Manual Setup (Alternative)
```bash
git clone https://github.com/m1rl0k/Context-Engine.git && cd Context-Engine
make bootstrap  # One-shot: up → wait → index → warm → health
```

Or step-by-step:
```bash
docker compose up -d
HOST_INDEX_PATH=/path/to/your/project docker compose run --rm indexer
```

*See [Configuration](docs/CONFIGURATION.md) for environment variables and [IDE_CLIENTS.md](docs/IDE_CLIENTS.md) for MCP setup.*

---

## Why This Stack Works Better

| Problem | Context-Engine Solution |
|---------|------------------------|
| **Large file chunks** → returns entire files | **Precise spans**: Returns 5-50 line chunks, not whole files |
| **Lost context** → missing relevant code | **Hybrid search**: Semantic + lexical + cross-encoder reranking |
| **Cloud dependency** → vendor lock-in | **Local stack**: Docker Compose on your machine |
| **Tool limits** → only works in specific IDEs | **MCP native**: Works with any MCP-compatible tool |

---

## What You Get Out of the Box

- **ReFRAG-inspired micro-chunking**: Research-grade precision retrieval
- **Self-hosted stack**: No cloud dependency, no vendor lock-in
- **Universal compatibility**: Claude Code, Windsurf, Cursor, Cline, etc.
- **Auto-syncing**: Extension watches for changes and re-indexes automatically
- **Memory system**: Store team knowledge alongside your code
- **Optional LLM features**: Local decoder (llama.cpp), cloud integration (GLM, MiniMax)

### Works With Your Local Files
No complicated path setup - Context-Engine automatically handles the mapping between your local files and the search index.

### Enterprise-Ready Features
- **Built-in authentication** with session management (optional)
- **Unified MCP endpoint** that combines indexer and memory services
- **Automatic collection injection** for workspace-aware queries

**Alternative: Direct HTTP endpoints**
```json
{
  "mcpServers": {
    "qdrant-indexer": { "url": "http://localhost:8003/mcp" },
    "memory": { "url": "http://localhost:8002/mcp" }
  }
}
```

*Using other IDEs? See [docs/IDE_CLIENTS.md](docs/IDE_CLIENTS.md) for complete MCP configuration examples.*

---

## Supported Clients

| Client | Transport |
|--------|-----------|
| Claude Code | SSE / RMCP |
| Cursor | SSE / RMCP |
| Windsurf | SSE / RMCP |
| Cline | SSE / RMCP |
| Roo | SSE / RMCP |
| Augment | SSE |
| Codex | RMCP |
| Copilot | RMCP |
| AmpCode | RMCP |
| Kiro | RMCP |
| Antigravity | RMCP |
| Zed | SSE (via mcp-remote) |

---

## Endpoints

| Service | URL |
|---------|-----|
| Indexer MCP (SSE) | `http://localhost:8001/sse` |
| Indexer MCP (RMCP) | `http://localhost:8003/mcp` |
| Memory MCP (SSE) | `http://localhost:8000/sse` |
| Memory MCP (RMCP) | `http://localhost:8002/mcp` |
| Qdrant | `http://localhost:6333` |
| Upload Service | `http://localhost:8004` |

---

## VS Code Extension

[Context Engine Uploader](https://marketplace.visualstudio.com/items?itemName=context-engine.context-engine-uploader) provides:

- **One-click upload** — Sync workspace to Context-Engine
- **Auto-sync** — Watch for changes and re-index automatically
- **Prompt+ button** — Enhance prompts with code context before sending
- **MCP auto-config** — Writes Claude/Windsurf MCP configs

See [docs/vscode-extension.md](docs/vscode-extension.md) for full documentation.

---

## MCP Tools

**Search** (Indexer MCP):
- `repo_search` — Code search with filters and optional profiles
- `context_search` — Blend code + memory results
- `context_answer` — LLM-generated answers with citations

**Memory** (Memory MCP):
- `store` — Save knowledge with metadata
- `find` — Retrieve stored memories

**Indexing**:
- `qdrant_index_root` — Index the workspace
- `qdrant_status` — Check collection health
- `qdrant_prune` — Remove stale entries

See [docs/MCP_API.md](docs/MCP_API.md) for complete API reference.

---

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/GETTING_STARTED.md) | VS Code + dev-remote walkthrough |
| [IDE Clients](docs/IDE_CLIENTS.md) | Config examples for all supported clients |
| [Configuration](docs/CONFIGURATION.md) | Environment variables reference |
| [MCP API](docs/MCP_API.md) | Full tool documentation |
| [Architecture](docs/ARCHITECTURE.md) | System design |
| [Multi-Repo](docs/MULTI_REPO_COLLECTIONS.md) | Multiple repositories in one collection |
| [Kubernetes](deploy/kubernetes/README.md) | Production deployment |

---

## How It Works

```mermaid
flowchart LR
  subgraph Your Machine
    A[IDE / AI Tool]
    V[VS Code Extension]
  end
  subgraph Docker
    U[Upload Service]
    I[Indexer MCP]
    M[Memory MCP]
    Q[(Qdrant)]
    L[[LLM Decoder]]
  end
  V -->|sync| U
  U --> I
  A -->|MCP| I
  A -->|MCP| M
  I --> Q
  M --> Q
  I -.-> L
```

---

## Language Support

Python, TypeScript/JavaScript, Go, Java, Rust, C#, PHP, Shell, Terraform, YAML, PowerShell

---

## Benchmarks

### CoSQA (Dense Retrieval, No Rerank)

| Method | MRR | R@1 | R@5 | R@10 | NDCG@10 |
|--------|-----|-----|-----|------|---------|
| **Context-Engine (Jina-Code)** | **0.276** | 0.146 | 0.448 | 0.658 | 0.365 |
| Context-Engine (BGE-base) | 0.253 | 0.150 | 0.374 | 0.550 | 0.322 |
| CodeT5+ embedding | 0.266 | - | - | - | - |
| BM25 (Lucene) | 0.167 | - | - | - | - |
| BoW | 0.065 | - | - | - | - |

*Corpus: 20,604 code snippets | 500 queries | Pure dense retrieval, no reranking*
*Jina-Code: jinaai/jina-embeddings-v2-base-code (code-specific, 8k context)*

---

## License

MIT

