from __future__ import annotations

from types import SimpleNamespace

from app import storage


def test_resolve_minio_connection_uses_local_endpoint_when_cluster_host_unresolvable_and_port_forward_reachable(monkeypatch):
    monkeypatch.setattr(
        storage,
        "get_settings",
        lambda: SimpleNamespace(
            minio_endpoint="http://minio.sideline-wire-dailycast.svc.cluster.local:9000",
            minio_local_endpoint="http://127.0.0.1:19000",
            minio_secure=False,
        ),
    )
    monkeypatch.setattr(storage, "_host_is_resolvable", lambda host: False)
    monkeypatch.setattr(storage, "_endpoint_is_reachable", lambda endpoint, secure: endpoint == "127.0.0.1:19000" and not secure)

    endpoint, secure = storage._resolve_minio_connection()

    assert endpoint == "127.0.0.1:19000"
    assert secure is False


def test_resolve_minio_connection_keeps_cluster_endpoint_when_resolvable(monkeypatch):
    monkeypatch.setattr(
        storage,
        "get_settings",
        lambda: SimpleNamespace(
            minio_endpoint="http://minio.sideline-wire-dailycast.svc.cluster.local:9000",
            minio_local_endpoint="http://127.0.0.1:19000",
            minio_secure=False,
        ),
    )
    monkeypatch.setattr(storage, "_host_is_resolvable", lambda host: True)

    endpoint, secure = storage._resolve_minio_connection()

    assert endpoint == "minio.sideline-wire-dailycast.svc.cluster.local:9000"
    assert secure is False
