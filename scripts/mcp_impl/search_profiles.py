"""Shared repo_search profile definitions.

Profiles are intentionally small path constraints, not alternate search
algorithms. They let callers express common scopes without exposing separate
MCP tools for every preset.
"""

from __future__ import annotations

from typing import Iterable

TEST_GLOBS = [
    "tests/**",
    "test/**",
    "**/*test*.*",
    "**/*_test.*",
    "**/Test*/**",
]

CONFIG_GLOBS = [
    "**/*.yml",
    "**/*.yaml",
    "**/*.json",
    "**/*.toml",
    "**/*.ini",
    "**/*.env",
    "**/*.config",
    "**/*.conf",
    "**/*.properties",
    "**/*.csproj",
    "**/*.props",
    "**/*.targets",
    "**/*.xml",
    "**/appsettings*.json",
]

CODE_GLOBS = [
    "**/*.py",
    "**/*.js",
    "**/*.ts",
    "**/*.tsx",
    "**/*.jsx",
    "**/*.mjs",
    "**/*.cjs",
    "**/*.go",
    "**/*.java",
    "**/*.cs",
    "**/*.rb",
    "**/*.php",
    "**/*.rs",
    "**/*.c",
    "**/*.h",
    "**/*.cpp",
    "**/*.hpp",
]

PROFILE_GLOBS = {
    "test": TEST_GLOBS,
    "tests": TEST_GLOBS,
    "config": CONFIG_GLOBS,
    "configs": CONFIG_GLOBS,
    "code": CODE_GLOBS,
}


def normalize_profile(profile: object) -> str:
    return str(profile or "").strip().lower().replace("-", "_")


def globs_for_profile(profile: object) -> list[str]:
    return list(PROFILE_GLOBS.get(normalize_profile(profile), []))


def append_profile_globs(path_globs: Iterable[str], profile: object) -> list[str]:
    merged = [str(g).strip() for g in path_globs if str(g).strip()]
    seen = set(merged)
    for glob in globs_for_profile(profile):
        if glob not in seen:
            merged.append(glob)
            seen.add(glob)
    return merged
