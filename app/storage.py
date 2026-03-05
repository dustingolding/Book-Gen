import json
import socket
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error
import yaml

from app.config import get_settings


def _parse_minio_endpoint(raw: str) -> tuple[str, bool]:
    normalized = raw.strip()
    if "://" not in normalized:
        normalized = f"http://{normalized}"
    parsed = urlparse(normalized)
    endpoint = parsed.netloc or parsed.path
    if not endpoint:
        raise RuntimeError(f"Invalid MINIO endpoint: {raw}")
    secure = parsed.scheme == "https"
    return endpoint, secure


def _host_is_resolvable(host: str) -> bool:
    try:
        socket.getaddrinfo(host, None)
        return True
    except OSError:
        return False


def _endpoint_is_reachable(endpoint: str, secure: bool) -> bool:
    host, _, port_raw = endpoint.partition(":")
    port = int(port_raw or (443 if secure else 80))
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _resolve_minio_connection() -> tuple[str, bool]:
    cfg = get_settings()
    endpoint, secure = _parse_minio_endpoint(cfg.minio_endpoint)
    host = endpoint.split(":", 1)[0]
    if not host.endswith(".svc.cluster.local") or _host_is_resolvable(host):
        return endpoint, secure or bool(cfg.minio_secure)

    local_raw = str(cfg.minio_local_endpoint or "").strip()
    if not local_raw:
        return endpoint, secure or bool(cfg.minio_secure)
    local_endpoint, local_secure = _parse_minio_endpoint(local_raw)
    if _endpoint_is_reachable(local_endpoint, local_secure):
        return local_endpoint, local_secure
    return endpoint, secure or bool(cfg.minio_secure)


class ObjectStore:
    def __init__(self) -> None:
        cfg = get_settings()
        endpoint, secure = _resolve_minio_connection()
        self.bucket = cfg.s3_bucket
        self.client = Minio(
            endpoint,
            access_key=cfg.minio_access_key,
            secret_key=cfg.minio_secret_key,
            secure=secure,
        )
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def put_json(self, key: str, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        stream = BytesIO(raw)
        self.client.put_object(
            self.bucket,
            key,
            data=stream,
            length=len(raw),
            content_type="application/json",
        )

    def get_json(self, key: str) -> dict[str, Any]:
        response = self.client.get_object(self.bucket, key)
        data = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return json.loads(data)

    def put_yaml(self, key: str, payload: dict[str, Any]) -> None:
        raw = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).encode("utf-8")
        stream = BytesIO(raw)
        self.client.put_object(
            self.bucket,
            key,
            data=stream,
            length=len(raw),
            content_type="application/yaml",
        )

    def get_yaml(self, key: str) -> dict[str, Any]:
        response = self.client.get_object(self.bucket, key)
        data = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return yaml.safe_load(data) or {}

    def exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except S3Error as exc:
            if getattr(exc, "code", "") in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
                return False
            raise

    def get_text(self, key: str) -> str:
        response = self.client.get_object(self.bucket, key)
        data = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return data

    def put_text(self, key: str, text: str, content_type: str = "text/markdown") -> None:
        raw = text.encode("utf-8")
        stream = BytesIO(raw)
        self.client.put_object(
            self.bucket,
            key,
            data=stream,
            length=len(raw),
            content_type=content_type,
        )
