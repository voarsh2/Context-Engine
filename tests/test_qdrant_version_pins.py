from pathlib import Path
from importlib.metadata import version
import inspect

from qdrant_client import QdrantClient


ROOT = Path(__file__).resolve().parents[1]
QDRANT_CLIENT_PIN = "qdrant-client==1.15.1"
QDRANT_SERVER_IMAGE = "qdrant/qdrant:v1.15.4"


def test_qdrant_client_is_exactly_pinned():
    requirements = (ROOT / "requirements.txt").read_text()

    assert QDRANT_CLIENT_PIN in requirements
    assert "qdrant-client>=" not in requirements


def test_qdrant_server_images_are_exactly_pinned():
    files = [
        ROOT / ".github/workflows/ci.yml",
        ROOT / "docker-compose.yml",
        ROOT / "docker-compose-bindmount-checkout.yml",
        ROOT / "deploy/kubernetes/qdrant.yaml",
        ROOT / "tests/conftest.py",
    ]

    for path in files:
        text = path.read_text()
        assert QDRANT_SERVER_IMAGE in text, str(path)
        assert "qdrant/qdrant:latest" not in text, str(path)


def test_installed_qdrant_client_matches_supported_api():
    assert version("qdrant-client") == "1.15.1"
    assert hasattr(QdrantClient, "search")
    assert hasattr(QdrantClient, "query_points")
    assert "query_filter" in inspect.signature(QdrantClient.query_points).parameters
