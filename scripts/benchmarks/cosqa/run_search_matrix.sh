#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT_DIR}"

PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "${PYTHON_BIN}" ]; then
  if command -v python3.11 >/dev/null 2>&1; then
    PYTHON_BIN="python3.11"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "No Python interpreter found (looked for python3.11/python3/python)." >&2
    exit 127
  fi
fi
RUN_TAG="${RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"
PROFILE="${PROFILE:-full}"      # smoke | quick | full
RUN_SET="${RUN_SET:-full}"       # pr | knobs | nightly | full
OUT_DIR="${OUT_DIR:-bench_results/cosqa/${RUN_TAG}}"
LOG_DIR="${LOG_DIR:-${OUT_DIR}}"
SPLIT="${SPLIT:-test}"
COLLECTION="${COLLECTION:-cosqa-search-${RUN_TAG}}"
LIMIT="${LIMIT:-10}"
RECREATE_INDEX="${RECREATE_INDEX:-1}"
ENFORCE_HYBRID_GATE="${ENFORCE_HYBRID_GATE:-0}"
HYBRID_MIN_DELTA="${HYBRID_MIN_DELTA:--0.020}"

case "${PROFILE}" in
  smoke)
    : "${CORPUS_LIMIT:=150}"
    : "${QUERY_LIMIT:=30}"
    ;;
  quick)
    : "${CORPUS_LIMIT:=500}"
    : "${QUERY_LIMIT:=100}"
    ;;
  full)
    : "${CORPUS_LIMIT:=0}"
    : "${QUERY_LIMIT:=0}"
    ;;
  *)
    echo "Unknown PROFILE='${PROFILE}'. Use smoke|quick|full" >&2
    exit 2
    ;;
esac

mkdir -p "${OUT_DIR}" "${LOG_DIR}"

BASE_ENV=(
  "LOG_LEVEL=${LOG_LEVEL:-INFO}"
  "DEBUG_HYBRID_SEARCH=${DEBUG_HYBRID_SEARCH:-0}"
  "QDRANT_URL=${QDRANT_URL:-http://localhost:6333}"
  "HYBRID_IN_PROCESS=${HYBRID_IN_PROCESS:-1}"
  "RERANK_IN_PROCESS=${RERANK_IN_PROCESS:-1}"
  "LEX_VECTOR_DIM=${LEX_VECTOR_DIM:-4096}"
  "COSQA_QUERY_CONCURRENCY=${COSQA_QUERY_CONCURRENCY:-8}"
  "LLM_EXPAND_MAX=0"
  "REFRAG_DECODER=0"
  "RERANK_LEARNING=0"
  "RERANK_EVENTS_ENABLED=0"
)

run_index_once() {
  local log="${LOG_DIR}/cosqa_index.log"
  local args=(
    "-m" "scripts.benchmarks.cosqa.runner"
    "--split" "${SPLIT}"
    "--collection" "${COLLECTION}"
    "--limit" "${LIMIT}"
    "--index-only"
  )

  if [ "${CORPUS_LIMIT}" -gt 0 ]; then
    args+=("--corpus-limit" "${CORPUS_LIMIT}")
  fi
  if [ "${QUERY_LIMIT}" -gt 0 ]; then
    args+=("--query-limit" "${QUERY_LIMIT}")
  fi
  if [ "${RECREATE_INDEX}" = "1" ]; then
    args+=("--recreate")
  fi

  echo "[index] collection=${COLLECTION} corpus_limit=${CORPUS_LIMIT} query_limit=${QUERY_LIMIT}" | tee "${log}"
  (
    export "${BASE_ENV[@]}"
    "${PYTHON_BIN}" "${args[@]}"
  ) >> "${log}" 2>&1
}

preflight_python_deps() {
  "${PYTHON_BIN}" - <<'PY'
import importlib.util

required = ["qdrant_client", "datasets"]
missing = [m for m in required if importlib.util.find_spec(m) is None]
if missing:
    raise SystemExit(
        "Missing Python deps for CoSQA benchmark: "
        + ", ".join(missing)
        + ". Install them before running."
    )
PY
}

verify_collection_ready() {
  "${PYTHON_BIN}" - "${COLLECTION}" <<'PY'
import os
import sys
from qdrant_client import QdrantClient

collection = sys.argv[1]
url = os.environ.get("QDRANT_URL", "http://localhost:6333")
client = QdrantClient(url=url, timeout=60)
info = client.get_collection(collection)
points = int(info.points_count or 0)
if points <= 0:
    raise RuntimeError(f"Collection '{collection}' has no points after indexing")
print(f"[verify] collection={collection} points={points}")
PY
}

run_case() {
  local label="$1"
  local mode="$2"
  local rerank="$3"
  local expand="$4"
  local lex_mode="$5"
  shift 5

  local output="${OUT_DIR}/cosqa_${label}.json"
  local log="${LOG_DIR}/cosqa_${label}.log"

  local args=(
    "-m" "scripts.benchmarks.cosqa.runner"
    "--split" "${SPLIT}"
    "--collection" "${COLLECTION}"
    "--limit" "${LIMIT}"
    "--skip-index"
    "--mode" "${mode}"
    "--output" "${output}"
  )

  if [ "${CORPUS_LIMIT}" -gt 0 ]; then
    args+=("--corpus-limit" "${CORPUS_LIMIT}")
  fi
  if [ "${QUERY_LIMIT}" -gt 0 ]; then
    args+=("--query-limit" "${QUERY_LIMIT}")
  fi
  if [ "${rerank}" = "0" ]; then
    args+=("--no-rerank")
  fi
  if [ "${expand}" = "0" ]; then
    args+=("--no-expand")
  fi

  local case_env=("HYBRID_LEXICAL_TEXT_MODE=${lex_mode}")
  for kv in "$@"; do
    case_env+=("${kv}")
  done

  echo "[run] ${label} mode=${mode} rerank=${rerank} expand=${expand} lex_mode=${lex_mode}" | tee "${log}"
  (
    export "${BASE_ENV[@]}"
    export "${case_env[@]}"
    "${PYTHON_BIN}" "${args[@]}"
  ) >> "${log}" 2>&1

  echo "[ok] ${output}"
}

CASES=()
case "${RUN_SET}" in
  pr)
    CASES=(
      "dense_norerank|dense|0|0|raw"
      "hybrid_rerank_lexrrf|hybrid|1|0|rrf"
      "hybrid_rerank_expand_lexrrf|hybrid|1|1|rrf"
    )
    ;;
  knobs)
    CASES=(
      "dense_norerank|dense|0|0|raw"
      "dense_rerank|dense|1|0|raw"
      "hybrid_norerank_lexraw|hybrid|0|0|raw"
      "hybrid_norerank_lexrrf|hybrid|0|0|rrf"
      "hybrid_rerank_lexraw|hybrid|1|0|raw"
      "hybrid_rerank_lexrrf|hybrid|1|0|rrf"
      "hybrid_rerank_expand_lexrrf|hybrid|1|1|rrf"
      "lexical_norerank|lexical|0|0|raw"
    )
    ;;
  nightly)
    CASES=(
      "dense_norerank|dense|0|0|raw"
      "dense_rerank|dense|1|0|raw"
      "hybrid_norerank_lexraw|hybrid|0|0|raw"
      "hybrid_norerank_lexrrf|hybrid|0|0|rrf"
      "hybrid_rerank_lexraw|hybrid|1|0|raw"
      "hybrid_rerank_lexrrf|hybrid|1|0|rrf"
      "hybrid_rerank_expand_lexrrf|hybrid|1|1|rrf"
      "lexical_norerank|lexical|0|0|raw"
    )
    ;;
  full)
    CASES=(
      "dense_norerank|dense|0|0|raw"
      "dense_rerank|dense|1|0|raw"
      "hybrid_norerank_lexraw|hybrid|0|0|raw"
      "hybrid_norerank_lexrrf|hybrid|0|0|rrf"
      "hybrid_rerank_lexraw|hybrid|1|0|raw"
      "hybrid_rerank_lexrrf|hybrid|1|0|rrf"
      "hybrid_rerank_expand_lexrrf|hybrid|1|1|rrf"
      "lexical_norerank|lexical|0|0|raw"
    )
    ;;
  *)
    echo "Unknown RUN_SET='${RUN_SET}'. Use pr|knobs|nightly|full" >&2
    exit 2
    ;;
esac

echo "[config] run_tag=${RUN_TAG} profile=${PROFILE} run_set=${RUN_SET} out_dir=${OUT_DIR}"
preflight_python_deps
run_index_once
verify_collection_ready

for spec in "${CASES[@]}"; do
  IFS='|' read -r label mode rerank expand lex_mode <<< "${spec}"
  run_case "${label}" "${mode}" "${rerank}" "${expand}" "${lex_mode}"
done

"${PYTHON_BIN}" - "${OUT_DIR}" "${ENFORCE_HYBRID_GATE}" "${HYBRID_MIN_DELTA}" <<'PY'
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
enforce_gate = str(sys.argv[2]).strip() in {"1", "true", "yes"}
min_delta = float(sys.argv[3])

rows = []
for path in sorted(out_dir.glob("cosqa_*.json")):
    if path.name.startswith("cosqa_index") or path.name.endswith("_meta.json") or path.name.startswith("summary"):
        continue
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or "metrics" not in data or "config" not in data:
        continue
    metrics = data.get("metrics") or {}
    config = data.get("config") or {}
    env = (config.get("env") or {}) if isinstance(config, dict) else {}
    rows.append({
        "label": path.stem.replace("cosqa_", ""),
        "mode": config.get("mode", ""),
        "rerank": bool(config.get("rerank_enabled", False)),
        "expand": env.get("HYBRID_EXPAND", ""),
        "lex_mode": env.get("HYBRID_LEXICAL_TEXT_MODE", ""),
        "mrr": float(metrics.get("mrr", 0.0) or 0.0),
        "recall_10": float(metrics.get("recall@10", 0.0) or 0.0),
        "ndcg_10": float(metrics.get("ndcg@10", 0.0) or 0.0),
        "lat_ms": float((data.get("latency") or {}).get("avg_ms", 0.0) or 0.0),
        "file": path.name,
    })

if not rows:
    print("No CoSQA result JSON files found.", file=sys.stderr)
    sys.exit(3)

rows.sort(key=lambda r: (-r["mrr"], -r["recall_10"]))

summary = {
    "ranked": rows,
    "best": rows[0],
}
(out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

lines = [
    "# CoSQA Search Matrix Summary",
    "",
    "| Rank | Label | Mode | Rerank | Expand | LexMode | MRR | R@10 | NDCG@10 | Avg Lat (ms) |",
    "|---:|---|---|---:|---:|---|---:|---:|---:|---:|",
]
for i, r in enumerate(rows, start=1):
    lines.append(
        f"| {i} | {r['label']} | {r['mode']} | {int(r['rerank'])} | {r['expand']} | {r['lex_mode']} | "
        f"{r['mrr']:.4f} | {r['recall_10']:.4f} | {r['ndcg_10']:.4f} | {r['lat_ms']:.2f} |"
    )

best_dense = max((r for r in rows if r["mode"] == "dense"), key=lambda r: r["mrr"], default=None)
best_hybrid = max((r for r in rows if r["mode"] == "hybrid"), key=lambda r: r["mrr"], default=None)
if best_dense and best_hybrid:
    delta = best_hybrid["mrr"] - best_dense["mrr"]
    lines.append("")
    lines.append(
        f"Best hybrid ({best_hybrid['label']}) vs best dense ({best_dense['label']}): "
        f"delta MRR = {delta:+.4f}"
    )
    if enforce_gate and delta < min_delta:
        lines.append(
            f"Gate failed: hybrid delta {delta:+.4f} is below required minimum {min_delta:+.4f}"
        )
        (out_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        print("\n".join(lines))
        sys.exit(4)

(out_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines))
PY

echo "[done] results=${OUT_DIR}"
echo "[done] summary=${OUT_DIR}/summary.md"
