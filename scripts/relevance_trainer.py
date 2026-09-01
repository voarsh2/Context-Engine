#!/usr/bin/env python3
"""
Background Relevance Trainer.

Consumes relevance feedback events (produced by rate_search_results MCP tool)
and computes per-collection ranking adjustments. Writes weights atomically so
the search path can apply them without restarting.

Uses result-level relevance aggregation (upgradeable to logistic regression
when score features are added to events).

Features:
- Reads feedback from NDJSON event log files (one per collection)
- Aggregates relevance scores per result_id
- Writes per-collection weight files atomically (write to .tmp, rename)
- Can run as a daemon or one-shot

Usage:
    # Run continuously (daemon mode)
    python -m scripts.relevance_trainer --daemon

    # Process pending events once and exit
    python -m scripts.relevance_trainer --once

    # Process specific collection
    python -m scripts.relevance_trainer --collection my-repo --once
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Event log configuration (shared with rerank_tools/events.py)
def _get_events_dir() -> Path:
    return Path(os.environ.get("RERANK_EVENTS_DIR", "/tmp/rerank_events"))


def _get_weights_dir() -> Path:
    return Path(os.environ.get("RERANKER_WEIGHTS_DIR", "/tmp/rerank_weights"))


def _get_poll_interval() -> int:
    return int(os.environ.get("RELEVANCE_TRAINER_POLL_INTERVAL", "30"))


def _get_min_events() -> int:
    return int(os.environ.get("RELEVANCE_TRAINER_MIN_EVENTS", "10"))


def list_collections_from_events() -> List[str]:
    """Discover collections from event files on disk."""
    events_dir = _get_events_dir()
    if not events_dir.exists():
        return []
    collections = set()
    for f in events_dir.glob("events_*_*.ndjson"):
        name = f.stem
        parts = name.split("_", 1)
        if len(parts) >= 2:
            coll_hour = parts[1].rsplit("_", 1)
            if len(coll_hour) >= 2 and coll_hour[1].isdigit():
                collections.add(coll_hour[0])
    return sorted(collections)


def read_feedback_events(collection: str) -> List[Dict[str, Any]]:
    """Read all relevance feedback events for a collection."""
    events_dir = _get_events_dir()
    if not events_dir.exists():
        return []
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in collection)
    pattern = f"events_{safe_name}_*.ndjson"
    events = []
    for fp in sorted(events_dir.glob(pattern)):
        try:
            with open(fp, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        if event.get("type") == "relevance_feedback":
                            events.append(event)
                    except json.JSONDecodeError:
                        continue
        except Exception:
            continue
    return events


def _rating_target(rating: Dict[str, Any]) -> Dict[str, Any]:
    """Extract rehydratable target metadata from a feedback rating."""
    keep = (
        "target_id",
        "impression_id",
        "path",
        "host_path",
        "container_path",
        "symbol",
        "kind",
        "repo",
        "file_hash",
        "symbol_content_hash",
    )
    target = {}
    for key in keep:
        val = rating.get(key)
        if val is not None and str(val).strip():
            target[key] = str(val).strip()
    return target


def aggregate_ratings(events: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Aggregate relevance scores per stable feedback target.

    Returns: {result_id: {total_relevance, count, avg_relevance, target}}
    """
    results: Dict[str, List[int]] = {}
    targets: Dict[str, Dict[str, Any]] = {}
    for event in events:
        for rating in event.get("ratings", []):
            rid = rating.get("result_id", "").strip()
            rel = rating.get("relevance")
            if not rid or rel is None:
                continue
            try:
                rel = int(rel)
            except (ValueError, TypeError):
                continue
            if rel not in (0, 1, 2):
                continue
            if rid not in results:
                results[rid] = []
            results[rid].append(rel)
            target = _rating_target(rating)
            if target:
                targets[rid] = target

    aggregated: Dict[str, Dict[str, Any]] = {}
    for rid, scores in results.items():
        total = sum(scores)
        count = len(scores)
        aggregated[rid] = {
            "total_relevance": total,
            "count": count,
            "avg_relevance": round(total / count, 3),
        }
        if rid in targets:
            aggregated[rid]["target"] = targets[rid]
    return aggregated


def load_weights(collection: str) -> Dict[str, Any]:
    """Load existing weights for a collection."""
    weights_dir = _get_weights_dir()
    weights_dir.mkdir(parents=True, exist_ok=True)
    weights_file = weights_dir / f"{collection}_relevance.json"
    if weights_file.exists():
        try:
            with open(weights_file, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_weights(collection: str, weights: Dict[str, Any]):
    """Atomically save weights for a collection (write .tmp, rename)."""
    weights_dir = _get_weights_dir()
    weights_dir.mkdir(parents=True, exist_ok=True)
    weights_file = weights_dir / f"{collection}_relevance.json"
    tmp_file = weights_dir / f"{collection}_relevance.json.tmp"
    try:
        with open(tmp_file, "w") as f:
            json.dump(weights, f)
        tmp_file.rename(weights_file)
    except Exception:
        if tmp_file.exists():
            tmp_file.unlink(missing_ok=True)
        raise


def process_collection(collection: str) -> Dict[str, Any]:
    """Read events, aggregate ratings, merge with existing weights, save."""
    events = read_feedback_events(collection)
    if len(events) < _get_min_events():
        return {
            "collection": collection,
            "events": len(events),
            "skipped": True,
            "reason": f"fewer than {_get_min_events()} events",
        }

    new_weights = aggregate_ratings(events)
    existing = load_weights(collection)
    existing_results = existing.get("results") if isinstance(existing.get("results"), dict) else existing
    if not isinstance(existing_results, dict):
        existing_results = {}
    merged = {**existing_results, **new_weights}
    save_weights(collection, {"updated_at": time.time(), "results": merged})

    return {
        "collection": collection,
        "events": len(events),
        "new_entries": len(new_weights),
        "total_entries": len(merged),
    }


def run_daemon(collections: Optional[List[str]] = None, poll_interval: int = 30):
    """Run continuously, polling for new events."""
    import logging
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="[%(asctime)s] %(levelname)s %(message)s",
    )
    logger = logging.getLogger("relevance_trainer")

    logger.info("Relevance trainer daemon started (poll=%ds)", poll_interval)

    while True:
        targets = collections or list_collections_from_events()
        for coll in targets:
            try:
                result = process_collection(coll)
                if not result.get("skipped"):
                    logger.info(
                        "[%s] %d events → %d new / %d total entries",
                        coll, result["events"], result["new_entries"], result["total_entries"],
                    )
            except Exception:
                logger.exception("[%s] Failed to process collection", coll)
        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="Relevance feedback trainer")
    parser.add_argument("--daemon", action="store_true", help="Run continuously")
    parser.add_argument("--once", action="store_true", help="Process pending events and exit")
    parser.add_argument("--collection", type=str, help="Process specific collection")
    parser.add_argument("--poll-interval", type=int, default=_get_poll_interval(),
                        help="Polling interval in seconds (daemon mode)")
    args = parser.parse_args()

    if args.daemon:
        collections = [args.collection] if args.collection else None
        run_daemon(collections=collections, poll_interval=args.poll_interval)
    elif args.once:
        targets = [args.collection] if args.collection else list_collections_from_events()
        for coll in targets:
            result = process_collection(coll)
            if result.get("skipped"):
                print(f"[{coll}] Skipped: {result.get('reason')}")
            else:
                print(f"[{coll}] {result['events']} events → {result['total_entries']} entries")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
