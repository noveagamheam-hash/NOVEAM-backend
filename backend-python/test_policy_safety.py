"""Synthetic-account tests. Database tests require a disposable local PostgreSQL DB."""
import os
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
import pytest
from fastapi.testclient import TestClient

import policy_safety as safety

ROOT = Path(__file__).parent
CATALOG = ROOT / "policy_catalog.v1.6.staging.json"
MIGRATION = ROOT / "migrations/016_policy_safety.sql"


def test_catalog_cannot_publish_draft_outside_staging():
    document, digest = safety.read_catalog(CATALOG, staging=True)
    assert len(document["sections"]) == 60
    assert digest == safety.read_catalog(CATALOG, staging=True)[1]
    with pytest.raises(ValueError, match="Only approved"):
        safety.read_catalog(CATALOG, staging=False)


@pytest.fixture
def backend(monkeypatch, tmp_path):
    dsn = os.getenv("NOVEAM_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set NOVEAM_TEST_DATABASE_URL to a disposable local PostgreSQL database")
    options = conninfo_to_dict(dsn)
    if options.get("host") not in ("127.0.0.1", "::1", "localhost"):
        pytest.fail("Integration tests require a local PostgreSQL database")
    import main
    schema = "policy_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            test_dsn = make_conninfo(dsn, options=f"-c search_path={schema}")
            monkeypatch.setattr(main, "DATABASE_URL", test_dsn)
            monkeypatch.setattr(main, "UPLOAD_DIR", tmp_path)
            monkeypatch.setattr(safety, "ENABLED", False)
            main.initialize_database()
            with psycopg.connect(test_dsn, autocommit=True, row_factory=dict_row) as conn:
                conn.execute(MIGRATION.read_text(encoding="utf-8"))
                conn.execute(MIGRATION.read_text(encoding="utf-8"))
            monkeypatch.setattr(safety, "STAGING", True)
            with main.db() as conn:
                safety.publish_catalog(conn, CATALOG, staging=True)
                safety.publish_catalog(conn, CATALOG, staging=True)
            # Direct test import supports running with the feature off by default.
            if "/api/v1/policies/current" not in main.app.openapi()["paths"]:
                safety.install_policy_safety(main.app, main.db, main.authenticate_session)
            monkeypatch.setattr(safety, "ENABLED", True)
            monkeypatch.setattr(safety, "OBSERVE", True)
            monkeypatch.setattr(safety, "ENFORCE", True)
            yield main, TestClient(main.app)
        finally:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def account(client):
    suffix = uuid4().hex[:12]
    payload = {"username": "user" + suffix, "display_name": "Test User",
               "email": suffix + "@example.test", "phone": str(int(suffix, 16)),
               "password": "Synthetic-password-984!", "role": "Patient"}
    response = client.post("/api/auth/register", json=payload)
    assert response.status_code == 201, response.text
    data = response.json()
    return data["user"]["id"], {"Authorization": "Bearer " + data["token"]}, data["policy"], payload


def accept(client, headers, state):
    return client.post("/api/v1/policies/accept", headers=headers,
                       json={"version": state["version"], "digest": state["digest"], "accepted": True})


def test_rules_gate_preserves_existing_draft_and_completed_state(backend, monkeypatch):
    main, client = backend
    assert client.get("/api/v1/policies/current").status_code == 200
    assert client.get("/api/v1/policies/status").status_code == 401
    uid, headers, state, credentials = account(client)
    assert state["next"] == "/rules.html"
    assert client.get("/api/posts", headers=headers).status_code == 428
    assert client.get("/api/onboarding/me", headers=headers).status_code == 428
    assert client.post("/api/v1/policies/accept", headers=headers,
                       json={"version": "1.6", "digest": state["digest"], "accepted": "yes"}).status_code == 422
    assert accept(client, headers, {**state, "digest": "0" * 64}).status_code == 409
    with main.db() as conn:
        conn.execute("INSERT INTO user_onboarding(user_id,step,revision,bio) VALUES(%s,2,8,'draft')", (uid,))
    first = accept(client, headers, state)
    assert first.status_code == 200, first.text
    assert first.json()["next"] == "/visitor/onboarding.html"
    assert accept(client, headers, state).json()["accepted_at"] == first.json()["accepted_at"]
    draft = client.get("/api/onboarding/me", headers=headers).json()
    assert (draft["step"], draft["revision"], draft["bio"]) == (2, 8, "draft")
    assert client.get("/api/posts", headers=headers).status_code == 200
    monkeypatch.setattr(safety, "ENABLED", False)
    existing = client.post("/api/auth/register", json={**credentials, "username": "existing",
                           "email": "existing@example.test", "phone": "2025550100"}).json()
    with main.db() as conn:
        conn.execute("INSERT INTO user_onboarding(user_id,step,completed_at) VALUES(%s,3,now())",
                     (existing["user"]["id"],))
    monkeypatch.setattr(safety, "ENABLED", True)
    login = client.post("/api/auth/login", json={"identifier": "existing",
                        "password": credentials["password"]}).json()
    assert login["policy"]["next"] == "/rules.html"
    old_headers = {"Authorization": "Bearer " + existing["token"]}
    assert accept(client, old_headers, login["policy"]).json()["next"] == "/feed.html"
    assert client.post("/api/auth/logout", headers=old_headers).status_code == 200
    assert client.get("/api/posts", headers=old_headers).status_code == 401
    with main.db() as conn:
        count = conn.execute("SELECT count(*) AS n FROM safety_audit_events WHERE actor_id=%s AND event='rules.accepted'",
                             (uid,)).fetchone()["n"]
    assert count == 1


def test_observation_report_independent_appeal_and_restriction_reversal(backend):
    main, client = backend
    subject, sh, state, _ = account(client)
    reporter, rh, _, _ = account(client)
    moderator, mh, _, _ = account(client)
    reviewer, vh, _, _ = account(client)
    auditor, ah, _, _ = account(client)
    assert accept(client, sh, state).status_code == 200
    assert client.get("/api/v1/admin/moderation/cases", headers=rh).status_code == 403
    with main.db() as conn:
        for uid, role in [(moderator, "moderator"), (moderator, "appeal_reviewer"),
                          (reviewer, "appeal_reviewer"), (auditor, "auditor")]:
            conn.execute("INSERT INTO safety_staff(user_id,role) VALUES(%s,%s)", (uid, role))

    for visibility in ("PRIVATE", "PUBLIC"):
        post = client.post("/api/posts", headers=sh,
                           json={"text": "I will kill you", "visibility": visibility})
        assert post.status_code == 200 or post.status_code == 201, post.text
    cases_path = "/api/v1/admin/moderation/cases"
    actions_path = "/api/v1/admin/moderation/actions"
    queue = client.get(cases_path, headers=mh).json()
    assert [(row["source"], row["urgent"]) for row in queue] == [("NRS-A01", True)]
    assert client.get("/api/posts", headers=sh).status_code == 200
    response = client.post("/api/v1/safety/reports", headers=rh,
                           json={"subject_id": subject, "section": 13, "reason": "Synthetic case"})
    assert response.status_code == 201, response.text
    assert len(client.get("/api/v1/safety/reports/mine", headers=rh).json()) == 1
    assert client.get("/api/v1/safety/reports/mine", headers=sh).json() == []

    decision = {"case_id": queue[0]["case_id"], "kind": "restriction", "hours": 24,
                "rationale": "Human reviewer verified a credible threat in synthetic evidence."}
    assert client.post(actions_path, headers=mh, json=decision).status_code == 409
    restricted = client.post(actions_path, headers=mh, json={**decision, "emergency": True})
    assert restricted.status_code == 201, restricted.text
    action_id = restricted.json()["id"]
    assert client.get("/api/posts", headers=sh).status_code == 403
    assert client.get("/api/v1/safety/actions/mine", headers=sh).json()[0]["id"] == action_id
    payload = {"action_id": action_id, "reason": "Synthetic contextual evidence"}
    assert client.post("/api/v1/safety/appeals", headers=rh, json=payload).status_code == 404
    appeal = client.post("/api/v1/safety/appeals", headers=sh, json=payload)
    assert appeal.status_code == 201, appeal.text
    resolve = f"/api/v1/admin/moderation/appeals/{appeal.json()['id']}/resolve"
    outcome = {"outcome": "reversed", "rationale": "Independent review verified the context."}
    assert client.post(resolve, headers=mh, json=outcome).status_code == 403
    assert client.post(resolve, headers=vh, json=outcome).status_code == 200
    assert client.get("/api/posts", headers=sh).status_code == 200
    audit = client.get("/api/v1/admin/moderation/audit", headers=ah)
    assert "appeal.reversed" in {event["event"] for event in audit.json()}
    assert client.get("/api/v1/admin/moderation/audit", headers=mh).status_code == 403


def test_ordinary_restriction_requires_three_reviewed_warnings(backend):
    main, client = backend
    subject, sh, state, _ = account(client)
    _, rh, _, _ = account(client)
    moderator, mh, _, _ = account(client)
    assert accept(client, sh, state).status_code == 200
    with main.db() as conn:
        conn.execute("INSERT INTO safety_staff(user_id,role) VALUES(%s,'moderator')", (moderator,))
    cases_path = "/api/v1/admin/moderation/cases"
    actions_path = "/api/v1/admin/moderation/actions"
    action_ids = []
    for incident in range(4):
        report = client.post("/api/v1/safety/reports", headers=rh,
                             json={"subject_id": subject, "section": 25,
                                   "reason": f"Synthetic distinct incident {incident}"})
        assert report.status_code == 201, report.text
        queue = client.get(cases_path, headers=mh).json()
        decision = {"case_id": queue[0]["case_id"], "kind": "warning",
                    "rationale": "Distinct verified incident."}
        if incident == 3:
            decision.update(kind="restriction", hours=1)
            # A challenged warning cannot count toward escalation.
            appeal = client.post("/api/v1/safety/appeals", headers=sh,
                                 json={"action_id": action_ids[0], "reason": "Synthetic dispute"})
            assert appeal.status_code == 201, appeal.text
            assert client.post(actions_path, headers=mh, json=decision).status_code == 409
            decision.pop("hours")
            decision["kind"] = "warning"
        action = client.post(actions_path, headers=mh, json=decision)
        assert action.status_code == 201, action.text
        action_ids.append(action.json()["id"])
        assert client.post(actions_path, headers=mh, json=decision).status_code == 409

    report = client.post("/api/v1/safety/reports", headers=rh,
                         json={"subject_id": subject, "section": 25, "reason": "Further incident"})
    assert report.status_code == 201, report.text
    queue = client.get(cases_path, headers=mh).json()
    action = client.post(actions_path, headers=mh,
                         json={"case_id": queue[0]["case_id"], "kind": "restriction", "hours": 1,
                               "rationale": "Three current warnings and further verified conduct."})
    assert action.status_code == 201, action.text
    assert client.get("/api/posts", headers=sh).status_code == 403
    with main.db() as conn:
        conn.execute("UPDATE moderation_actions SET expires_at=now() WHERE id=%s",
                     (action.json()["id"],))
    assert client.get("/api/posts", headers=sh).status_code == 200
