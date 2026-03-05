import logging
import socket
from urllib.parse import quote
from urllib.parse import urlparse

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


def _parse_endpoint(raw: str) -> str:
    normalized = raw.strip()
    if not normalized:
        return ""
    if "://" not in normalized:
        normalized = f"http://{normalized}"
    parsed = urlparse(normalized)
    endpoint = parsed.netloc or parsed.path
    return endpoint


def _host_is_resolvable(host: str) -> bool:
    try:
        socket.getaddrinfo(host, None)
        return True
    except OSError:
        return False


def _endpoint_is_reachable(endpoint: str) -> bool:
    host, _, port_raw = endpoint.partition(":")
    port = int(port_raw or 80)
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _resolve_lakefs_endpoint(cfg) -> str:
    endpoint = (cfg.lakefs_endpoint or "").rstrip("/")
    parsed = _parse_endpoint(endpoint)
    host = parsed.split(":", 1)[0] if parsed else ""
    if not parsed or not host.endswith(".svc.cluster.local") or _host_is_resolvable(host):
        return endpoint
    local = (cfg.lakefs_local_endpoint or "").rstrip("/")
    local_parsed = _parse_endpoint(local)
    if local_parsed and _endpoint_is_reachable(local_parsed):
        return local
    return endpoint


class LakeFSClient:
    def __init__(self) -> None:
        cfg = get_settings()
        self.enabled = cfg.lakefs_enabled
        self.endpoint = _resolve_lakefs_endpoint(cfg)
        self.repo = cfg.lakefs_repo or ""
        self.access_key = cfg.lakefs_access_key or ""
        self.secret_key = cfg.lakefs_secret_key or ""
        self.source_branch = cfg.lakefs_source_branch
        self.bookgen_branch_prefix = cfg.lakefs_bookgen_branch_prefix
        self.timeout = 30.0
        if not self.enabled:
            return
        missing = []
        if not self.endpoint:
            missing.append("LAKEFS_ENDPOINT")
        if not self.repo:
            missing.append("LAKEFS_REPO")
        if not self.access_key:
            missing.append("LAKEFS_ACCESS_KEY")
        if not self.secret_key:
            missing.append("LAKEFS_SECRET_KEY")
        if missing:
            raise RuntimeError(f"LakeFS enabled but missing required config: {', '.join(missing)}")

    def _url(self, path: str) -> str:
        return f"{self.endpoint}{path}"

    def _request(self, method: str, path: str, *, json: dict | None = None) -> httpx.Response:
        response = httpx.request(
            method,
            self._url(path),
            json=json,
            auth=(self.access_key, self.secret_key),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response

    def _authed(self) -> tuple[str, str]:
        return (self.access_key, self.secret_key)

    def ensure_branch(self, branch: str, source: str | None = None) -> None:
        if not self.enabled:
            return
        repo = quote(self.repo, safe="")
        branch_q = quote(branch, safe="")
        get_path = f"/api/v1/repositories/{repo}/branches/{branch_q}"
        response = httpx.get(
            self._url(get_path),
            auth=(self.access_key, self.secret_key),
            timeout=self.timeout,
        )
        if response.status_code == 200:
            return
        if response.status_code != 404:
            response.raise_for_status()
        source_branch = source or self.source_branch
        create_path = f"/api/v1/repositories/{repo}/branches"
        self._request(
            "POST",
            create_path,
            json={"name": branch, "source": source_branch},
        )
        logger.info("lakefs_branch_created repo=%s branch=%s source=%s", self.repo, branch, source_branch)

    def commit(self, *, branch: str, message: str, metadata: dict | None = None) -> str:
        if not self.enabled:
            return ""
        repo = quote(self.repo, safe="")
        branch_q = quote(branch, safe="")
        path = f"/api/v1/repositories/{repo}/branches/{branch_q}/commits"
        payload: dict[str, object] = {"message": message}
        if metadata:
            payload["metadata"] = metadata
        response = self._request("POST", path, json=payload)
        body = response.json()
        commit_id = str(body.get("id", ""))
        if not commit_id:
            raise RuntimeError("LakeFS commit response missing commit id")
        return commit_id

    def upload_object(self, *, branch: str, path: str, content: bytes, content_type: str = "application/octet-stream") -> None:
        if not self.enabled:
            return
        repo = quote(self.repo, safe="")
        branch_q = quote(branch, safe="")
        # lakeFS v1.78 stages object content with POST on branches endpoint.
        url = self._url(f"/api/v1/repositories/{repo}/branches/{branch_q}/objects")
        response = httpx.post(
            url,
            auth=self._authed(),
            params={"path": path},
            headers={"Content-Type": content_type},
            content=content,
            timeout=self.timeout,
        )
        response.raise_for_status()

    def download_object(self, *, branch: str, path: str) -> bytes:
        if not self.enabled:
            raise RuntimeError("LakeFS is disabled")
        repo = quote(self.repo, safe="")
        branch_q = quote(branch, safe="")
        url = self._url(f"/api/v1/repositories/{repo}/refs/{branch_q}/objects")
        response = httpx.get(
            url,
            auth=self._authed(),
            params={"path": path},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.content

    def object_exists(self, *, branch: str, path: str) -> bool:
        if not self.enabled:
            return False
        repo = quote(self.repo, safe="")
        branch_q = quote(branch, safe="")
        url = self._url(f"/api/v1/repositories/{repo}/refs/{branch_q}/objects/stat")
        response = httpx.get(
            url,
            auth=self._authed(),
            params={"path": path},
            timeout=self.timeout,
        )
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return False
