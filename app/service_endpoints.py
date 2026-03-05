from __future__ import annotations

import socket
from urllib.parse import urlparse


def _parse_uri(raw: str) -> tuple[str, str, bool]:
    normalized = raw.strip()
    if not normalized:
        return "", "", False
    if "://" not in normalized:
        normalized = f"http://{normalized}"
    parsed = urlparse(normalized)
    netloc = parsed.netloc or parsed.path
    host = parsed.hostname or ""
    secure = parsed.scheme == "https"
    return normalized, host, secure


def _host_is_resolvable(host: str) -> bool:
    try:
        socket.getaddrinfo(host, None)
        return True
    except OSError:
        return False


def _uri_is_reachable(uri: str) -> bool:
    parsed = urlparse(uri if "://" in uri else f"http://{uri}")
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def resolve_service_uri(primary_uri: str | None, local_uri: str | None) -> str:
    primary = (primary_uri or "").strip()
    normalized, host, _ = _parse_uri(primary)
    if not normalized:
        return primary
    if not host.endswith(".svc.cluster.local") or _host_is_resolvable(host):
        return primary
    local = (local_uri or "").strip()
    if local and _uri_is_reachable(local):
        return local
    return primary
