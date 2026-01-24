#!/usr/bin/env python3
"""
Qdrant client and query logic extracted from hybrid_search.py.

This module provides:
- Connection pooling setup and client management
- Thread executor for parallel queries
- Point coercion utilities
- Collection caching and management
- Query functions (lex_query, sparse_lex_query, dense_query)
- Lexical vector functions (lex_hash_vector, lex_sparse_vector)
"""

# Note: __all__ is defined at the end of this file for clarity

import os
import logging
import threading
import re
import time
from typing import List, Dict, Any, Tuple, Optional, Callable, TypeVar
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Core Qdrant imports (optional in some runtimes)
try:
    from qdrant_client import QdrantClient, models
except ImportError:  # pragma: no cover
    QdrantClient = None  # type: ignore
    models = None  # type: ignore

try:
    from qdrant_client.http.exceptions import ResponseHandlingException
except ImportError:  # pragma: no cover
    ResponseHandlingException = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import httpx
except ImportError:
    httpx = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import httpcore
except ImportError:
    httpcore = None  # type: ignore

logger = logging.getLogger("hybrid_qdrant")


def _is_timeout_exception(exc: Exception) -> bool:
    """Detect whether an exception is a Qdrant/http timeout."""

    if ResponseHandlingException and isinstance(exc, ResponseHandlingException):
        cause = exc.__cause__ or exc.__context__
        if cause is not None and cause is not exc:
            return _is_timeout_exception(cause)
        return "timeout" in str(exc).lower()

    timeout_types = []
    if httpx is not None:
        timeout_types.append(getattr(httpx, "TimeoutException", None))
        timeout_types.append(getattr(httpx, "ReadTimeout", None))
    if httpcore is not None:
        timeout_types.append(getattr(httpcore, "TimeoutException", None))
        timeout_types.append(getattr(httpcore, "ReadTimeout", None))

    for t in timeout_types:
        if t and isinstance(exc, t):
            return True

    return isinstance(exc, TimeoutError)


def _log_qdrant_timeout(kind: str, collection: Optional[str], detail: Exception) -> None:
    coll = collection or "(unknown)"
    logger.warning(
        "Qdrant %s query timed out for collection %s; returning partial results", kind, coll
    )


def _handle_timeout(kind: str, collection: Optional[str], exc: Exception) -> bool:
    if _is_timeout_exception(exc):
        _log_qdrant_timeout(kind, collection, exc)
        return True
    return False


# ---------------------------------------------------------------------------
# Helper functions for safe type conversion
# ---------------------------------------------------------------------------

def _safe_int(val: Any, default: int) -> int:
    try:
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return default
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val: Any, default: float) -> float:
    try:
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Configuration constants (from environment)
# ---------------------------------------------------------------------------

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
API_KEY = os.environ.get("QDRANT_API_KEY")

# Lexical vector configuration
# Imported from ingest config to ensure Single Source of Truth
from scripts.ingest.config import (
    LEX_VECTOR_NAME,
    LEX_VECTOR_DIM,
    LEX_SPARSE_NAME,
    LEX_SPARSE_MODE,
    # Multi-granular vectors
    MULTI_GRANULAR_VECTORS,
    ENTITY_DENSE_NAME,
    ENTITY_DENSE_DIM,
    RELATION_DENSE_NAME,
    RELATION_DENSE_DIM,
)

EF_SEARCH = _safe_int(os.environ.get("QDRANT_EF_SEARCH", "128"), 128)
_MAX_QDRANT_CONCURRENCY = max(1, _safe_int(os.environ.get("QDRANT_MAX_CONCURRENCY", "6"), 6))
_SEMAPHORE_LOG_THRESHOLD = float(os.environ.get("QDRANT_SEMAPHORE_LOG_THRESHOLD", "0.5") or 0.5)
_QDRANT_REQUEST_SEMAPHORE = threading.BoundedSemaphore(_MAX_QDRANT_CONCURRENCY)
T = TypeVar("T")

# Quantization search params (for faster search with quantized collections)
QDRANT_QUANTIZATION = os.environ.get("QDRANT_QUANTIZATION", "none").strip().lower()
QDRANT_QUANTIZATION_RESCORE = os.environ.get("QDRANT_QUANTIZATION_RESCORE", "1").strip().lower() in ("1", "true", "yes", "on")
QDRANT_QUANTIZATION_OVERSAMPLING = float(os.environ.get("QDRANT_QUANTIZATION_OVERSAMPLING", "2.0") or 2.0)


def _get_search_params(ef: int) -> models.SearchParams:
    """Build SearchParams with optional quantization settings."""
    if QDRANT_QUANTIZATION in {"scalar", "binary"}:
        return models.SearchParams(
            hnsw_ef=ef,
            quantization=models.QuantizationSearchParams(
                rescore=QDRANT_QUANTIZATION_RESCORE,
                oversampling=QDRANT_QUANTIZATION_OVERSAMPLING,
            )
        )
    return models.SearchParams(hnsw_ef=ef)


def _with_qdrant_slot(kind: str, fn: Callable[[], T]) -> T:
    """Serialize Qdrant calls to avoid overload while preserving concurrency."""
    wait_start = time.perf_counter()
    _QDRANT_REQUEST_SEMAPHORE.acquire()
    waited = time.perf_counter() - wait_start
    if waited >= _SEMAPHORE_LOG_THRESHOLD:
        logger.debug(
            "Qdrant %s query waited %.3fs for slot (max=%s)",
            kind,
            waited,
            _MAX_QDRANT_CONCURRENCY,
        )
    try:
        return fn()
    finally:
        _QDRANT_REQUEST_SEMAPHORE.release()


# ---------------------------------------------------------------------------
# Connection pooling setup
# ---------------------------------------------------------------------------

try:
    from scripts.qdrant_client_manager import get_qdrant_client, return_qdrant_client, pooled_qdrant_client
    _POOL_AVAILABLE = True
except ImportError:
    _POOL_AVAILABLE = False

    def get_qdrant_client(url=None, api_key=None, force_new=False, use_pool=True):
        """Fallback client creation when pooling is unavailable."""
        if QdrantClient is None:
            raise ImportError(
                "qdrant_client is not installed. Install with: pip install qdrant-client"
            )
        return QdrantClient(
            url=url or os.environ.get("QDRANT_URL", "http://localhost:6333"),
            api_key=api_key or os.environ.get("QDRANT_API_KEY")
        )

    def return_qdrant_client(client):
        """No-op when pooling is unavailable."""
        pass

    class pooled_qdrant_client:
        """Fallback context manager when pooling is unavailable."""
        def __init__(self, url=None, api_key=None):
            self.url = url
            self.api_key = api_key
            self.client = None

        def __enter__(self):
            self.client = get_qdrant_client(self.url, self.api_key)
            return self.client

        def __exit__(self, exc_type, exc_val, exc_tb):
            return_qdrant_client(self.client)


# ---------------------------------------------------------------------------
# Thread executor for parallel queries
# ---------------------------------------------------------------------------

_QUERY_EXECUTOR: ThreadPoolExecutor | None = None
_EXECUTOR_LOCK = threading.Lock()


def _get_query_executor(max_workers: int = 4) -> ThreadPoolExecutor:
    """Get or create a shared ThreadPoolExecutor for parallel queries."""
    global _QUERY_EXECUTOR
    if _QUERY_EXECUTOR is None:
        with _EXECUTOR_LOCK:
            if _QUERY_EXECUTOR is None:
                _QUERY_EXECUTOR = ThreadPoolExecutor(max_workers=max_workers)
    return _QUERY_EXECUTOR


# ---------------------------------------------------------------------------
# Point coercion
# ---------------------------------------------------------------------------

def _coerce_points(result: Any) -> List[Any]:
    """Normalize Qdrant responses to a list of points."""
    if result is None:
        return []
    if isinstance(result, list):
        return result
    try:
        return list(result)
    except TypeError:
        return [result]


# ---------------------------------------------------------------------------
# Legacy search fallback
# ---------------------------------------------------------------------------

def _legacy_vector_search(
    client,
    collection: str,
    vec_name: str,
    vector: List[float],
    per_query: int,
    flt,
) -> List[Any]:
    """Fallback to legacy client.search when query_points is unavailable."""
    try:
        result = client.search(
            collection_name=collection,
            query_vector={"name": vec_name, "vector": vector},
            limit=per_query,
            with_payload=True,
            query_filter=flt,
        )
        return _coerce_points(getattr(result, "points", result))
    except Exception as exc:
        if _handle_timeout("legacy", collection, exc):
            return []
        return []


# ---------------------------------------------------------------------------
# Collection caching
# ---------------------------------------------------------------------------

_ENSURED_COLLECTIONS: set[str] = set()
_COLLECTION_VECTOR_NAMES: Dict[str, set[str]] = {}


def _get_client_endpoint(client) -> str:
    """Extract endpoint identifier from Qdrant client for cache scoping."""
    try:
        if hasattr(client, '_client') and hasattr(client._client, '_host'):
            return f"{client._client._host}:{getattr(client._client, '_port', 6333)}"
        if hasattr(client, 'rest_uri'):
            return client.rest_uri
        return os.environ.get("QDRANT_URL", "localhost:6333")
    except Exception:
        return os.environ.get("QDRANT_URL", "localhost:6333")


def _collection_cache_key(client, collection: str) -> str:
    return f"{_get_client_endpoint(client)}:{collection}"


def _cache_collection_vectors(client, collection: str) -> set[str] | None:
    """Cache available vector names (dense + sparse) for a collection."""
    cache_key = _collection_cache_key(client, collection)
    if cache_key in _COLLECTION_VECTOR_NAMES:
        return _COLLECTION_VECTOR_NAMES[cache_key]
    try:
        info = client.get_collection(collection)
    except Exception:
        return None
    try:
        vnames: set[str] = set()
        vcfg = info.config.params.vectors
        if isinstance(vcfg, dict):
            vnames.update(vcfg.keys())
        elif hasattr(vcfg, "size"):
            vnames.add("")  # Default (unnamed) vector
        scfg = info.config.params.sparse_vectors
        if isinstance(scfg, dict):
            vnames.update(scfg.keys())
        _COLLECTION_VECTOR_NAMES[cache_key] = vnames
        return vnames
    except Exception:
        return None


def _vector_available(client, collection: str, vector_name: str | None) -> bool:
    if not vector_name:
        return True
    vnames = _cache_collection_vectors(client, collection)
    if vnames is None:
        return True
    return vector_name in vnames


def _ensure_collection(client, collection: str, dim: int, vec_name: str):
    """Cached wrapper for ensure_collection - only calls once per (endpoint, collection, vec_name) pair.

    IMPORTANT: This is called during SEARCH operations. We must NOT delete/recreate collections
    that already exist with data. The ensure_collection in ingest_code can trigger recreation
    when PATTERN_VECTORS=1 or LEX_SPARSE_MODE=1 if the collection lacks those vectors.

    For search, we only need to verify the collection exists - not modify its schema.
    """
    endpoint = _get_client_endpoint(client)
    cache_key = f"{endpoint}:{collection}:{vec_name}:{dim}"
    if cache_key in _ENSURED_COLLECTIONS:
        return

    # For SEARCH operations, just verify collection exists - don't try to modify schema
    # Schema modifications can trigger deletion of existing data!
    vnames = _cache_collection_vectors(client, collection)
    if vnames is not None:
        _ENSURED_COLLECTIONS.add(cache_key)
        return

    # Collection doesn't exist - only then call ensure_collection to create it
    try:
        from scripts.ingest_code import ensure_collection as _ensure_collection_raw
        _ensure_collection_raw(client, collection, dim, vec_name)
    except ImportError:
        pass

    try:
        _cache_collection_vectors(client, collection)
    except Exception as e:
        logger.debug(f"Suppressed exception: {e}")
    _ENSURED_COLLECTIONS.add(cache_key)


def clear_ensured_collections():
    """Clear the collection cache (useful for testing)."""
    global _ENSURED_COLLECTIONS, _COLLECTION_VECTOR_NAMES
    _ENSURED_COLLECTIONS = set()
    _COLLECTION_VECTOR_NAMES = {}


# ---------------------------------------------------------------------------
# Collection name resolution
# ---------------------------------------------------------------------------

def _collection(collection_name: str | None = None) -> str:
    """Determine collection name with priority: CLI arg > env > workspace state > default."""
    if collection_name and collection_name.strip():
        return collection_name.strip()

    env_coll = os.environ.get("COLLECTION_NAME", "").strip()
    if env_coll:
        return env_coll

    try:
        import json
        workspace_root = Path(os.environ.get("WORKSPACE_PATH") or os.environ.get("WATCH_ROOT") or "/work")
        state_file = workspace_root / ".codebase" / "state.json"
        if state_file.exists():
            with open(state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
            if isinstance(state, dict):
                coll = state.get("qdrant_collection")
                if isinstance(coll, str) and coll.strip():
                    return coll.strip()
    except Exception as e:
        logger.debug(f"Suppressed exception: {e}")

    return "codebase"


# ---------------------------------------------------------------------------
# Filter sanitization
# ---------------------------------------------------------------------------

_FILTER_CACHE: Dict[int, Any] = {}
_FILTER_CACHE_LOCK = threading.Lock()
_FILTER_CACHE_MAX = 256


def _sanitize_filter_obj(flt):
    """Sanitize Qdrant filter objects so we never send an empty filter {}.
    
    Qdrant returns 400 if filter has no conditions; return None in that case.
    Uses caching for repeated filter patterns to avoid redundant validation.
    """
    if flt is None:
        return None

    cache_key = id(flt)
    with _FILTER_CACHE_LOCK:
        if cache_key in _FILTER_CACHE:
            return _FILTER_CACHE[cache_key]

    try:
        must = getattr(flt, "must", None)
        should = getattr(flt, "should", None)
        must_not = getattr(flt, "must_not", None)
        if must is None and should is None and must_not is None:
            if isinstance(flt, dict):
                m = [c for c in (flt.get("must") or []) if c is not None]
                s = [c for c in (flt.get("should") or []) if c is not None]
                mn = [c for c in (flt.get("must_not") or []) if c is not None]
                result = None if (not m and not s and not mn) else flt
            else:
                result = None
        else:
            m = [c for c in (must or []) if c is not None]
            s = [c for c in (should or []) if c is not None]
            mn = [c for c in (must_not or []) if c is not None]
            result = None if (not m and not s and not mn) else flt
    except Exception:
        result = None

    with _FILTER_CACHE_LOCK:
        if len(_FILTER_CACHE) < _FILTER_CACHE_MAX:
            _FILTER_CACHE[cache_key] = result

    return result


# ---------------------------------------------------------------------------
# Lexical vector functions
# ---------------------------------------------------------------------------

_STOP = {
    "the", "a", "an", "of", "in", "on", "for", "and", "or", "to",
    "with", "by", "is", "are", "be", "this", "that",
}


def _split_ident_lex(s: str) -> List[str]:
    """Split identifier into tokens (snake_case and camelCase aware)."""
    parts = re.split(r"[^A-Za-z0-9]+", s)
    out: List[str] = []
    for p in parts:
        if not p:
            continue
        segs = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+", p)
        out.extend([x for x in segs if x])
    return [x.lower() for x in out if x and x.lower() not in _STOP]


def lex_hash_vector(phrases: List[str], dim: int | None = None) -> List[float]:
    """Generate dense lexical hash vector for query phrases."""
    if dim is None:
        dim = LEX_VECTOR_DIM
    try:
        from scripts.utils import lex_hash_vector_queries as _lex_hash_vector_queries
        return _lex_hash_vector_queries(phrases, dim)
    except ImportError:
        return _fallback_lex_hash_vector(phrases, dim)


def _fallback_lex_hash_vector(phrases: List[str], dim: int) -> List[float]:
    """Fallback implementation when utils is unavailable."""
    import hashlib
    vec = [0.0] * dim
    for phrase in phrases:
        for tok in _split_ident_lex(phrase):
            h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
            idx = h % dim
            vec[idx] += 1.0
    norm = sum(v * v for v in vec) ** 0.5
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


def lex_sparse_vector(phrases: List[str]) -> Dict[str, Any]:
    """Generate sparse vector for query phrases (lossless exact matching)."""
    try:
        from scripts.utils import lex_sparse_vector_queries as _lex_sparse_vector_queries
        return _lex_sparse_vector_queries(phrases)
    except ImportError:
        return _fallback_lex_sparse_vector(phrases)


def _fallback_lex_sparse_vector(phrases: List[str]) -> Dict[str, Any]:
    """Fallback implementation when utils is unavailable."""
    import hashlib
    indices = []
    values = []
    seen = set()
    for phrase in phrases:
        for tok in _split_ident_lex(phrase):
            h = int(hashlib.md5(tok.encode()).hexdigest(), 16) % (2**31)
            if h not in seen:
                indices.append(h)
                values.append(1.0)
                seen.add(h)
    return {"indices": indices, "values": values}


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

def lex_query(
    client,
    v: List[float],
    flt,
    per_query: int,
    collection_name: str | None = None
) -> List[Any]:
    """Query using dense lexical hash vector."""
    ef = max(EF_SEARCH, 32 + 4 * int(per_query))
    flt = _sanitize_filter_obj(flt)
    collection = _collection(collection_name)
    if not _vector_available(client, collection, LEX_VECTOR_NAME):
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            logger.debug(f"Skipping lex query: {LEX_VECTOR_NAME} not in collection vectors")
        return []

    try:
        qp = client.query_points(
            collection_name=collection,
            query=v,
            using=LEX_VECTOR_NAME,
            query_filter=flt,
            search_params=_get_search_params(ef),
            limit=per_query,
            with_payload=True,
        )
        return _coerce_points(getattr(qp, "points", qp))
    except TypeError:
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            logger.debug("QP_FILTER_KWARG_SWITCH", extra={"using": LEX_VECTOR_NAME})
        qp = client.query_points(
            collection_name=collection,
            query=v,
            using=LEX_VECTOR_NAME,
            filter=flt,
            search_params=_get_search_params(ef),
            limit=per_query,
            with_payload=True,
        )
        return _coerce_points(getattr(qp, "points", qp))
    except AttributeError:
        return _legacy_vector_search(client, collection, LEX_VECTOR_NAME, v, per_query, flt)
    except Exception as e:
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            try:
                logger.debug("QP_FILTER_DROP", extra={"using": LEX_VECTOR_NAME, "reason": str(e)[:200]})
            except Exception as e:
                logger.debug(f"Suppressed exception: {e}")
        try:
            qp = _with_qdrant_slot(
                "lex",
                lambda: client.query_points(
                    collection_name=collection,
                    query=v,
                    using=LEX_VECTOR_NAME,
                    query_filter=None,
                    search_params=_get_search_params(ef),
                    limit=per_query,
                    with_payload=True,
                ),
            )
            return _coerce_points(getattr(qp, "points", qp))
        except TypeError:
            qp = _with_qdrant_slot(
                "lex",
                lambda: client.query_points(
                    collection_name=collection,
                    query=v,
                    using=LEX_VECTOR_NAME,
                    filter=None,
                    search_params=_get_search_params(ef),
                    limit=per_query,
                    with_payload=True,
                ),
            )
            return _coerce_points(getattr(qp, "points", qp))
        except Exception as e2:
            if os.environ.get("DEBUG_HYBRID_SEARCH"):
                try:
                    logger.debug("QP_FILTER_DROP_FAILED", extra={"using": LEX_VECTOR_NAME, "reason": str(e2)[:200]})
                except Exception as e:
                    logger.debug(f"Suppressed exception: {e}")
        return _legacy_vector_search(client, collection, LEX_VECTOR_NAME, v, per_query, flt)


def sparse_lex_query(
    client,
    sparse_vec: Dict[str, Any],
    flt,
    per_query: int,
    collection_name: str | None = None
) -> List[Any]:
    """Query using sparse lexical vector for lossless exact matching."""
    flt = _sanitize_filter_obj(flt)
    collection = _collection(collection_name)
    
    # Check if sparse vector exists in collection
    vnames = _cache_collection_vectors(client, collection)
    if vnames is not None and LEX_SPARSE_NAME not in vnames:
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            logger.debug(f"Skipping sparse lex query: {LEX_SPARSE_NAME} not in {vnames}")
        return []

    if not sparse_vec.get("indices"):
        return []

    try:
        qp = _with_qdrant_slot(
            "sparse",
            lambda: client.query_points(
                collection_name=collection,
                query=models.SparseVector(
                    indices=sparse_vec["indices"],
                    values=sparse_vec["values"],
                ),
                using=LEX_SPARSE_NAME,
                query_filter=flt,
                limit=per_query,
                with_payload=True,
            ),
        )
        return _coerce_points(getattr(qp, "points", qp))
    except TypeError:
        try:
            qp = _with_qdrant_slot(
                "sparse",
                lambda: client.query_points(
                    collection_name=collection,
                    query=models.SparseVector(
                        indices=sparse_vec["indices"],
                        values=sparse_vec["values"],
                    ),
                    using=LEX_SPARSE_NAME,
                    filter=flt,
                    limit=per_query,
                    with_payload=True,
                ),
            )
            return _coerce_points(getattr(qp, "points", qp))
        except Exception:
            return []
    except Exception as e:
        if _handle_timeout("sparse", collection, e):
            return []
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            logger.debug("SPARSE_LEX_QUERY_ERROR", extra={"error": str(e)[:200]})
        return []


def dense_query(
    client,
    vec_name: str,
    v: List[float],
    flt,
    per_query: int,
    collection_name: str | None = None,
    query_text: str | None = None
) -> List[Any]:
    """Query using dense embedding vector."""
    # Default EF: scale with per_query for adequate recall
    ef = max(EF_SEARCH, 32 + 4 * int(per_query))

    # Apply dynamic EF optimization if query text provided
    if query_text:
        try:
            from scripts.query_optimizer import optimize_query
            result = optimize_query(query_text)
            # Only override EF when adaptive optimization is enabled
            if result.get("adaptive_enabled", False):
                ef = result["recommended_ef"]
                if os.environ.get("DEBUG_HYBRID_SEARCH"):
                    logger.debug(f"Dynamic EF: {ef} (complexity={result['complexity']}, type={result['query_type']})")
        except ImportError:
            pass
        except Exception as e:
            if os.environ.get("DEBUG_HYBRID_SEARCH"):
                logger.debug(f"Query optimizer failed, using default EF: {e}")

    flt = _sanitize_filter_obj(flt)
    collection = _collection(collection_name)
    if not _vector_available(client, collection, vec_name):
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            logger.debug(f"Skipping dense query: {vec_name} not in collection vectors")
        return []

    try:
        qp = _with_qdrant_slot(
            "dense",
            lambda: client.query_points(
                collection_name=collection,
                query=v,
                using=vec_name,
                query_filter=flt,
                search_params=_get_search_params(ef),
                limit=per_query,
                with_payload=True,
            ),
        )
        return _coerce_points(getattr(qp, "points", qp))
    except TypeError:
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            logger.debug("QP_FILTER_KWARG_SWITCH", extra={"using": vec_name})
        qp = _with_qdrant_slot(
            "dense",
            lambda: client.query_points(
                collection_name=collection,
                query=v,
                using=vec_name,
                filter=flt,
                search_params=_get_search_params(ef),
                limit=per_query,
                with_payload=True,
            ),
        )
        return _coerce_points(getattr(qp, "points", qp))
    except Exception as e:
        if _handle_timeout("dense", collection, e):
            return []
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            try:
                logger.debug("QP_FILTER_DROP", extra={"using": vec_name, "reason": str(e)[:200]})
            except Exception as e:
                logger.debug(f"Suppressed exception: {e}")
        if not collection:
            return _legacy_vector_search(client, _collection(), vec_name, v, per_query, flt)
        try:
            qp = _with_qdrant_slot(
                "dense",
                lambda: client.query_points(
                    collection_name=collection,
                    query=v,
                    using=vec_name,
                    query_filter=None,
                    search_params=_get_search_params(ef),
                    limit=per_query,
                    with_payload=True,
                ),
            )
            return _coerce_points(getattr(qp, "points", qp))
        except TypeError:
            try:
                qp = _with_qdrant_slot(
                    "dense",
                    lambda: client.query_points(
                        collection_name=collection,
                        query=v,
                        using=vec_name,
                        filter=None,
                        search_params=_get_search_params(ef),
                        limit=per_query,
                        with_payload=True,
                    ),
                )
                return _coerce_points(getattr(qp, "points", qp))
            except Exception as e2:
                if _handle_timeout("dense", collection, e2):
                    return []
                if os.environ.get("DEBUG_HYBRID_SEARCH"):
                    try:
                        logger.debug("QP_FILTER_DROP_FAILED", extra={"using": vec_name, "reason": str(e2)[:200]})
                    except Exception as e:
                        logger.debug(f"Suppressed exception: {e}")
        return _legacy_vector_search(client, collection, vec_name, v, per_query, flt)


# ---------------------------------------------------------------------------
# Multi-Granular Query (Two-Stage Prefetch)
# ---------------------------------------------------------------------------

def multi_granular_query(
    client,
    vec_name: str,
    dense_vec: List[float],
    entity_vec: List[float] | None,
    relation_vec: List[float] | None,
    flt,
    per_query: int,
    collection_name: str | None = None,
    *,
    prefetch_limit: int = 100,
    query_text: str | None = None,
) -> Tuple[List[Any], Dict[str, List[Any]]]:
    """Two-stage search with multi-granular vectors using Qdrant prefetch.

    Uses entity_dense and relation_dense vectors for coarse prefetch,
    then reranks with primary dense vector.

    Args:
        client: QdrantClient instance
        vec_name: Primary dense vector name (e.g., "dense")
        dense_vec: Primary dense embedding
        entity_vec: Entity signature embedding (optional, for prefetch)
        relation_vec: Relation pattern embedding (optional, for prefetch)
        flt: Qdrant filter object
        per_query: Number of results to return
        collection_name: Target collection
        prefetch_limit: How many candidates to fetch in prefetch stage
        query_text: Optional query text for optimizer integration

    Returns:
        Tuple of (final_results, stage_results_dict)
        stage_results_dict contains {"entity": [...], "relation": [...]} for fusion
    """
    if not MULTI_GRANULAR_VECTORS:
        # Fall back to standard dense query
        results = dense_query(client, vec_name, dense_vec, flt, per_query, collection_name, query_text)
        return results, {}

    collection = _collection(collection_name)
    if collection is None:
        return [], {}

    # Build prefetch queries for entity and relation vectors
    prefetch_queries = []
    stage_results: Dict[str, List[Any]] = {"entity": [], "relation": []}

    # Entity prefetch: coarse filter by symbol signatures
    if entity_vec is not None:
        try:
            prefetch_queries.append(
                models.Prefetch(
                    query=entity_vec,
                    using=ENTITY_DENSE_NAME,
                    limit=prefetch_limit,
                    filter=flt,
                )
            )
        except Exception as e:
            logger.debug(f"Suppressed exception: {e}")

    # Relation prefetch: coarse filter by call patterns
    if relation_vec is not None:
        try:
            prefetch_queries.append(
                models.Prefetch(
                    query=relation_vec,
                    using=RELATION_DENSE_NAME,
                    limit=prefetch_limit,
                    filter=flt,
                )
            )
        except Exception as e:
            logger.debug(f"Suppressed exception: {e}")

    # If no prefetch vectors, fall back to standard dense query
    if not prefetch_queries:
        results = dense_query(client, vec_name, dense_vec, flt, per_query, collection_name, query_text)
        return results, {}

    # Two-stage query: prefetch with entity/relation, then rerank with dense
    # NOTE: query_points uses 'query_filter' not 'filter' (Qdrant 1.7+ API)
    try:
        ef = max(EF_SEARCH, 32 + 4 * per_query)
        search_params = _get_search_params(ef)

        qp = client.query_points(
            collection_name=collection,
            prefetch=prefetch_queries,
            query=dense_vec,
            using=vec_name,
            query_filter=flt,
            limit=per_query,
            search_params=search_params,
            with_payload=True,
        )
        final_results = _coerce_points(getattr(qp, "points", qp))

        # Run separate entity/relation queries to capture stage scores for fusion
        # NOTE: query_points uses 'query_filter' not 'filter' (Qdrant 1.7+ API)
        if entity_vec is not None:
            try:
                entity_qp = client.query_points(
                    collection_name=collection,
                    query=entity_vec,
                    using=ENTITY_DENSE_NAME,
                    query_filter=flt,
                    limit=per_query * 2,  # Get more for fusion
                    search_params=search_params,
                    with_payload=True,
                )
                stage_results["entity"] = _coerce_points(getattr(entity_qp, "points", entity_qp))
            except Exception as e:
                if os.environ.get("DEBUG_HYBRID_SEARCH"):
                    logger.debug(f"Entity query failed: {e}")

        if relation_vec is not None:
            try:
                relation_qp = client.query_points(
                    collection_name=collection,
                    query=relation_vec,
                    using=RELATION_DENSE_NAME,
                    query_filter=flt,
                    limit=per_query * 2,
                    search_params=search_params,
                    with_payload=True,
                )
                stage_results["relation"] = _coerce_points(getattr(relation_qp, "points", relation_qp))
            except Exception as e:
                if os.environ.get("DEBUG_HYBRID_SEARCH"):
                    logger.debug(f"Relation query failed: {e}")

        return final_results, stage_results

    except TypeError:
        # Fallback if prefetch not supported (older Qdrant version)
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            logger.debug("PREFETCH_NOT_SUPPORTED: Falling back to standard dense query")
        results = dense_query(client, vec_name, dense_vec, flt, per_query, collection_name, query_text)
        return results, {}

    except Exception as e:
        if os.environ.get("DEBUG_HYBRID_SEARCH"):
            logger.debug(f"MULTI_GRANULAR_QUERY_FAILED: {e}")
        # Fall back to standard dense query
        results = dense_query(client, vec_name, dense_vec, flt, per_query, collection_name, query_text)
        return results, {}


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------

__all__ = [
    # Pool availability flag
    "_POOL_AVAILABLE",
    # Connection pooling
    "get_qdrant_client",
    "return_qdrant_client",
    "pooled_qdrant_client",
    # Thread executor
    "_QUERY_EXECUTOR",
    "_EXECUTOR_LOCK",
    "_get_query_executor",
    # Point coercion
    "_coerce_points",
    # Legacy search
    "_legacy_vector_search",
    # Collection caching
    "_ENSURED_COLLECTIONS",
    "_get_client_endpoint",
    "_ensure_collection",
    "clear_ensured_collections",
    # Collection name resolution
    "_collection",
    # Filter sanitization
    "_sanitize_filter_obj",
    # Lexical vector functions
    "_split_ident_lex",
    "lex_hash_vector",
    "lex_sparse_vector",
    # Query functions
    "lex_query",
    "sparse_lex_query",
    "dense_query",
    "multi_granular_query",
    # Multi-granular config
    "MULTI_GRANULAR_VECTORS",
    "ENTITY_DENSE_NAME",
    "RELATION_DENSE_NAME",
    # Constants
    "QDRANT_URL",
    "API_KEY",
    "LEX_VECTOR_NAME",
    "LEX_VECTOR_DIM",
    "LEX_SPARSE_NAME",
    "LEX_SPARSE_MODE",
    "EF_SEARCH",
]
