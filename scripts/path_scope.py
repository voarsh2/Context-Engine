#!/usr/bin/env python3
"""
Shared helpers for user-facing path scoping (`under`) across search tools.

`under` is treated as a recursive subtree scope from the user's workspace
perspective (for example: "space" matches ".../space/**").
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Any, Mapping, Optional, Set

_MULTI_SLASH_RE = re.compile(r"/+")


def _normalize_path_token(value: Any) -> str:
    s = str(value or "").strip().replace("\\", "/")
    if not s:
        return ""
    s = _MULTI_SLASH_RE.sub("/", s)
    # Normalize common "file://" style inputs.
    if s.startswith("file://"):
        s = s[7:]
    return s.strip("/")


def _normalize_repo_hint(repo_hint: Any) -> str:
    r = _normalize_path_token(repo_hint)
    if not r:
        return ""
    return r.split("/")[-1]


def _repo_root_hint() -> str:
    """Best-effort repository root (directory containing scripts/)."""
    try:
        return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    except Exception:
        return ""


def _maybe_expand_from_cwd(token: str) -> str:
    """Recover under values that were relativized from the current subdirectory."""
    s = str(token or "").strip().strip("/")
    if not s or "/" in s:
        return s
    try:
        root = _repo_root_hint()
        if not root:
            return s
        cwd = os.path.abspath(os.getcwd())
        if not (cwd == root or cwd.startswith(root + os.sep)):
            return s
        rel_cwd = os.path.relpath(cwd, root).replace("\\", "/").strip("/")
        if not rel_cwd:
            return s
        rebased = f"{rel_cwd}/{s}"
        rebased_path = os.path.join(root, *rebased.split("/"))
        top_level_path = os.path.join(root, s)
        if os.path.exists(rebased_path) and not os.path.exists(top_level_path):
            return rebased
    except Exception:
        pass
    return s


@lru_cache(maxsize=256)
def _unique_segment_path(root: str, segment: str) -> str:
    """Return unique repo-relative directory path for a segment, else empty."""
    if not root or not segment:
        return ""
    top = os.path.join(root, segment)
    if os.path.exists(top):
        return ""
    matches: list[str] = []
    skip = {
        ".git",
        ".codebase",
        "__pycache__",
        ".venv",
        "node_modules",
    }
    try:
        for dirpath, dirnames, _filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in skip and not d.startswith(".")]
            if segment in dirnames:
                rel = os.path.relpath(os.path.join(dirpath, segment), root).replace("\\", "/")
                matches.append(rel.strip("/"))
                if len(matches) > 1:
                    return ""
    except Exception:
        return ""
    return matches[0] if len(matches) == 1 else ""


def _maybe_expand_unique_segment(token: str) -> str:
    """Resolve single-segment under values to a unique subtree when possible."""
    s = str(token or "").strip().strip("/")
    if not s or "/" in s:
        return s
    root = _repo_root_hint()
    if not root:
        return s
    found = _unique_segment_path(root, s)
    return found or s


def normalize_under(under: Optional[str]) -> Optional[str]:
    """Normalize user-provided `under` into a comparable path token."""
    s = _normalize_path_token(under)
    if not s or s in {".", "work"}:
        return None
    # Accept absolute-style workspace prefixes while preserving user-facing scope.
    if s.startswith("work/"):
        s = s[len("work/") :]
    s = _maybe_expand_from_cwd(s)
    s = _maybe_expand_unique_segment(s)
    if not s or s in {".", "work"}:
        return None
    return s


def _path_forms(path: Any, repo_hint: Any = None) -> Set[str]:
    """Generate comparable path forms from a path-like value."""
    p = _normalize_path_token(path)
    if not p:
        return set()

    forms: Set[str] = {p}

    repo = _normalize_repo_hint(repo_hint)

    if p.startswith("work/"):
        rest = p[len("work/") :]
        if rest:
            forms.add(rest)
            if "/" in rest and repo:
                head, tail = rest.split("/", 1)
                if head.casefold() == repo.casefold() and tail:
                    forms.add(tail)

    if repo:
        repo_cf = repo.casefold()
        repo_prefix_cf = repo_cf + "/"
        marker = "/" + repo + "/"
        marker_cf = "/" + repo_cf + "/"
        for f in list(forms):
            f_cf = f.casefold()
            if f_cf.startswith(repo_prefix_cf):
                forms.add(f[len(repo) + 1 :])
            idx = f_cf.find(marker_cf)
            if idx >= 0:
                tail = f[idx + len(marker) :]
                if tail:
                    forms.add(tail)

    return {x for x in forms if x}


def metadata_path_forms(metadata: Mapping[str, Any]) -> Set[str]:
    """Collect path forms from a metadata payload."""
    repo_hint = metadata.get("repo")
    forms: Set[str] = set()
    for key in (
        "repo_rel_path",
        "path",
        "container_path",
        "host_path",
        "path_prefix",
        "file_path",
        "rel_path",
        "client_path",
    ):
        v = metadata.get(key)
        if v:
            forms.update(_path_forms(v, repo_hint=repo_hint))
    return forms


def metadata_matches_under(metadata: Mapping[str, Any], under: Optional[str]) -> bool:
    """Return True when metadata falls under the requested subtree scope."""
    norm_under = normalize_under(under)
    if not norm_under:
        return True

    repo_hint = metadata.get("repo")
    under_forms = _path_forms(norm_under, repo_hint=repo_hint)
    under_forms.add(norm_under)
    if not norm_under.startswith("work/"):
        under_forms.add("work/" + norm_under)

    under_forms_l = {u.casefold() for u in under_forms if u}
    if not under_forms_l:
        return True

    has_repo_hint = bool(str(repo_hint or "").strip())

    for cand in metadata_path_forms(metadata):
        cand_forms = {cand}
        # Compatibility fallback for points that only store /work/<repo>/... paths
        # but do not carry metadata.repo (older/benchmark/custom payloads).
        if not has_repo_hint:
            c0 = cand.strip("/")
            if c0.startswith("work/"):
                rest = c0[len("work/") :]
                if "/" in rest:
                    _head, tail = rest.split("/", 1)
                    if tail:
                        cand_forms.add(tail)

        for cf in cand_forms:
            c = cf.casefold()
            for u in under_forms_l:
                if c == u or c.startswith(u + "/"):
                    return True
    return False


def path_matches_under(path: Any, under: Optional[str], repo_hint: Any = None) -> bool:
    """Path-only convenience wrapper for `under` subtree matching."""
    md = {"path": path}
    if repo_hint:
        md["repo"] = repo_hint
    return metadata_matches_under(md, under)
