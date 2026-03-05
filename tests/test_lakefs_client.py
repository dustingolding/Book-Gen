from __future__ import annotations

from types import SimpleNamespace

from app.clients import lakefs


def test_resolve_lakefs_endpoint_uses_local_when_cluster_host_unresolvable_and_local_reachable(monkeypatch):
    cfg = SimpleNamespace(
        lakefs_endpoint="http://lakefs.sideline-wire-dailycast.svc.cluster.local:8000",
        lakefs_local_endpoint="http://127.0.0.1:18000",
    )
    monkeypatch.setattr(lakefs, "_host_is_resolvable", lambda host: False)
    monkeypatch.setattr(lakefs, "_endpoint_is_reachable", lambda endpoint: endpoint == "127.0.0.1:18000")

    resolved = lakefs._resolve_lakefs_endpoint(cfg)

    assert resolved == "http://127.0.0.1:18000"


def test_resolve_lakefs_endpoint_keeps_cluster_when_resolvable(monkeypatch):
    cfg = SimpleNamespace(
        lakefs_endpoint="http://lakefs.sideline-wire-dailycast.svc.cluster.local:8000",
        lakefs_local_endpoint="http://127.0.0.1:18000",
    )
    monkeypatch.setattr(lakefs, "_host_is_resolvable", lambda host: True)

    resolved = lakefs._resolve_lakefs_endpoint(cfg)

    assert resolved == "http://lakefs.sideline-wire-dailycast.svc.cluster.local:8000"
