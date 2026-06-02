"""Code indexing subsystem package.

Submodules are intentionally loaded on demand. Importing lightweight helpers
such as ``scripts.ingest.config`` should not initialize Qdrant, tree-sitter, or
the indexing pipeline.
"""

__all__ = [
    "config",
    "tree_sitter",
    "vectors",
    "exclusions",
    "chunking",
    "symbols",
    "pseudo",
    "metadata",
    "qdrant",
    "pipeline",
    "cli",
]
