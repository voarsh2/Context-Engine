"""
rerank_tools - Reranker utilities.

This package contains tools for reranker evaluation and event logging.
The recursive reranker and learning systems have been removed in favor
of a simpler relevance feedback loop.

Modules:
- events: Training event logging for relevance feedback
- eval: Offline evaluation utilities (MRR/Recall/latency)
- local: Local cross-encoder reranker
"""
from __future__ import annotations

# Event logging (used by the relevance feedback system)
from .events import (
    log_training_event,
    list_event_files,
    read_events,
    cleanup_old_events,
    RERANK_EVENTS_ENABLED,
    RERANK_EVENTS_SAMPLE_RATE,
    RERANK_EVENTS_RETENTION_DAYS,
)

__all__ = [
    "log_training_event",
    "list_event_files",
    "read_events",
    "cleanup_old_events",
    "RERANK_EVENTS_ENABLED",
    "RERANK_EVENTS_SAMPLE_RATE",
    "RERANK_EVENTS_RETENTION_DAYS",
]
