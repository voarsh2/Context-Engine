"""Small HTTP client for calling MCP tools.

This is intentionally transport glue, not an intent router. Agent/tool selection
belongs to the MCP client using the exposed tools directly.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, Tuple
from urllib import request


def _post_raw(url: str, payload: Dict[str, Any], headers: Dict[str, str], timeout: float = 60.0) -> Tuple[Dict[str, str], bytes]:
    req = request.Request(url, method="POST")
    for k, v in headers.items():
        req.add_header(k, v)
    data = json.dumps(payload).encode("utf-8")
    with request.urlopen(req, data=data, timeout=timeout) as resp:
        body = resp.read()
        hdrs = {k.lower(): v for k, v in resp.headers.items()}
    return hdrs, body


def _post_raw_retry(url: str, payload: Dict[str, Any], headers: Dict[str, str], timeout: float = 60.0, retries: int = 2, backoff: float = 0.5) -> Tuple[Dict[str, str], bytes]:
    last_exc: Exception | None = None
    for i in range(max(0, retries) + 1):
        try:
            return _post_raw(url, payload, headers, timeout=timeout)
        except Exception as e:
            last_exc = e
            if i < retries:
                try:
                    time.sleep(backoff * (2 ** i))
                except Exception:
                    pass
            else:
                raise last_exc
    raise last_exc or RuntimeError("MCP HTTP request failed")


def _parse_stream_or_json(body: bytes) -> Dict[str, Any]:
    txt = body.decode("utf-8", errors="ignore")
    if "data:" in txt and ("event:" in txt or txt.strip().startswith("data:")):
        last = None
        for line in txt.splitlines():
            if line.startswith("data:"):
                last = line[len("data:"):].strip()
        if last:
            try:
                return json.loads(last)
            except Exception:
                pass
    return json.loads(txt)


def _filter_args(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v not in (None, "")}


def _mcp_handshake(base_url: str, timeout: float = 30.0) -> Dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    init_payload = {
        "jsonrpc": "2.0",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "context-engine-http-client", "version": "0.1.0"},
        },
        "id": 1,
    }
    hdrs, body = _post_raw_retry(base_url, init_payload, headers, timeout=timeout)
    sid = hdrs.get("mcp-session-id") or hdrs.get("Mcp-Session-Id")
    if not sid:
        try:
            j = _parse_stream_or_json(body)
            sid = j.get("sessionId")
        except Exception:
            sid = None
    if sid:
        headers["Mcp-Session-Id"] = sid
    try:
        _post_raw_retry(base_url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, headers, timeout=timeout)
    except Exception:
        pass
    return headers


def _extract_iserror_text(resp: Dict[str, Any]) -> str | None:
    try:
        r = resp.get("result") or {}
        if isinstance(r, dict) and r.get("isError"):
            content = r.get("content")
            if isinstance(content, list) and content and isinstance(content[0], dict):
                if content[0].get("type") == "text":
                    return content[0].get("text")
    except Exception:
        pass
    return None


def call_tool_http(base_url: str, tool_name: str, args: Dict[str, Any], timeout: float = 120.0) -> Dict[str, Any]:
    """Call an MCP tool over streamable HTTP."""
    headers = _mcp_handshake(base_url, timeout=min(timeout, 30.0))

    def _do_call(arguments: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "id": "mcp-http-client-1",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        _, body = _post_raw_retry(base_url, payload, headers, timeout=timeout)
        return _parse_stream_or_json(body)

    args1 = _filter_args(args or {})
    resp = _do_call({"arguments": args1} if tool_name.endswith("_compat") else args1)

    def _get_structured_error(r: Dict[str, Any]) -> str | None:
        try:
            rr = r.get("result") or {}
            sc = rr.get("structuredContent") or {}
            rs = sc.get("result") or {}
            err = rs.get("error")
            if isinstance(err, str):
                return err
        except Exception:
            pass
        return None

    msg = _extract_iserror_text(resp)
    serr = _get_structured_error(resp)
    if msg:
        low = msg.lower()
        if ("kwargs" in low) and ("field required" in low or "missing" in low):
            return _do_call({"kwargs": args1})
        if ("arguments" in low) and ("field required" in low or "missing" in low):
            return _do_call({"arguments": args1})
    if (serr and serr.strip().lower() == "query required") and ("query" in args1 or "queries" in args1):
        resp4 = _do_call({"kwargs": args1})
        serr2 = _get_structured_error(resp4)
        if not (serr2 and serr2.strip().lower() == "query required"):
            return resp4
        resp5 = _do_call({"arguments": {"kwargs": args1}})
        serr3 = _get_structured_error(resp5)
        if not (serr3 and serr3.strip().lower() == "query required"):
            return resp5
        return _do_call({"arguments": args1})
    return resp
