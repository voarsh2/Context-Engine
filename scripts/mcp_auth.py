import os
from typing import Any, Dict, Optional

from scripts.logger import ValidationError
from scripts.auth_backend import (
    AUTH_ENABLED as AUTH_ENABLED_AUTH,
    ACL_ALLOW_ALL as ACL_ALLOW_ALL_AUTH,
    validate_session as _auth_validate_session,
    has_collection_access as _has_collection_access,
)


ACL_ENFORCE = (
    str(os.environ.get("CTXCE_MCP_ACL_ENFORCE", "0")).strip().lower()
    in {"1", "true", "yes", "on"}
)


def require_auth_session(session: Optional[str]) -> Optional[Dict[str, Any]]:
    if not AUTH_ENABLED_AUTH:
        return None
    sid = (session or "").strip()
    if not sid:
        raise ValidationError("Missing session for authorized operation")
    info = _auth_validate_session(sid)
    if not info:
        raise ValidationError("Invalid or expired session")
    return info


def require_collection_access(user_id: Optional[str], collection: str, perm: str) -> None:
    if not ACL_ENFORCE or not AUTH_ENABLED_AUTH:
        return
    if ACL_ALLOW_ALL_AUTH:
        return
    uid = (user_id or "").strip()
    if not uid:
        raise ValidationError("Not authorized: missing user id")
    if not _has_collection_access(uid, collection, perm):
        raise ValidationError(
            f"Forbidden: {perm} access to collection '{collection}' denied"
        )
