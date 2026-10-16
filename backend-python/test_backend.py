"""Regression tests use a temporary SQLite DB, local file storage and fake service settings."""

import collections
import importlib.util
import logging
import pathlib
import re
import sqlite3
import uuid

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect


@pytest.fixture(scope="module")
def backend(tmp_path_factory):
    root = tmp_path_factory.mktemp("noveam")
    spec = importlib.util.spec_from_file_location(
        "noveam_test_backend", pathlib.Path(__file__).with_name("main.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.DB_PATH = root / "test.db"
    module.UPLOAD_DIR = root / "uploads"
    module.NOVEAM_RATE_LIMIT_PER_MINUTE = 100000
    # Do not initialize external services even if the host has credentials.
    module.initialize_firebase = lambda: None
    assert not module.DB_PATH.exists()
    with TestClient(module.app) as client:
        yield module, client


def account(backend, role="Patient"):
    m, c = backend
    label = uuid.uuid4().hex[:18]
    response = c.post(
        "/api/auth/register",
        json={
            "display_name": "Test User",
            "email": label + "@example.invalid",
            "phone": str(int(label, 16))[:15],
            "role": "Patient",
            "password": "correct-password",
        },
    )
    assert response.status_code == 201, response.text
    data = response.json()
    if role != "Patient":
        with m.db() as conn:
            conn.execute(
                "UPDATE users SET role=? WHERE id=?", (role, data["user"]["id"])
            )
    return data["user"]["id"], {"Authorization": "Bearer " + data["token"]}, data


def connect_accounts(backend, a, b):
    m, c = backend
    request = c.post(f"/api/social/connections/{b[0]}", headers=a[1])
    assert request.status_code == 201, request.text
    response = c.post(
        f"/api/social/connections/requests/{request.json()['id']}/accept", headers=b[1]
    )
    assert response.status_code == 200, response.text


def test_startup_all_schemas_and_no_duplicate_routes(backend):
    m, c = backend
    with m.db() as conn:
        tables = {
            r["name"]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert {
        "users",
        "care_access_requests",
        "communication_meetings",
        "notifications",
        "community_groups",
        "jobs",
        "learning_courses",
        "user_interests",
    } <= tables
    pairs = [
        (r.path, method) for r in m.app.routes for method in getattr(r, "methods", [])
    ]
    assert all(v == 1 for v in collections.Counter(pairs).values())
    assert m.app.openapi()["paths"]["/api/auth/register"]
    assert c.get("/api/system/health").status_code == 200


def test_nested_database_scope_closes_and_rolls_back_audit(backend):
    m, c = backend
    marker = "rollback-" + uuid.uuid4().hex
    with pytest.raises(RuntimeError):
        with m.db() as outer:
            with m.db() as inner:
                assert outer is inner
            m.audit(None, marker, "test")
            raise RuntimeError("rollback")
    with pytest.raises(sqlite3.ProgrammingError):
        outer.execute("SELECT 1")
    with m.db() as conn:
        assert not conn.execute(
            "SELECT 1 FROM audit_log WHERE action=?", (marker,)
        ).fetchone()


def test_registration_login_and_clinical_role_restriction(backend):
    m, c = backend
    a = account(backend)
    response = c.post(
        "/api/auth/login",
        json={"username": a[2]["user"]["username"], "password": "correct-password"},
    )
    assert response.status_code == 200, response.text
    response = c.post(
        "/api/auth/register",
        json={
            "display_name": "Clinician",
            "email": "doctor@example.invalid",
            "phone": "5551234567",
            "role": "Doctor",
            "password": "correct-password",
        },
    )
    assert response.status_code == 400
    response = c.put(
        "/api/profiles/me",
        headers=a[1],
        json={"display_name": "Test", "role": "Doctor"},
    )
    assert response.status_code == 400
    assert c.get("/api/care/clinical", headers=a[1]).status_code == 403


def test_tasks_settings_and_profile_routes_restored(backend):
    m, c = backend
    a = account(backend)
    task = {"task_date": "2026-10-16", "time": "10:00", "title": "A task"}
    response = c.post("/api/tasks", headers=a[1], json=task)
    assert response.status_code == 201, response.text
    assert c.get("/api/tasks", headers=a[1]).json()[0]["title"] == "A task"
    assert c.get("/api/profiles/me", headers=a[1]).status_code == 200
    assert (
        c.put("/api/settings/me", headers=a[1], json={"theme": "dark"}).status_code
        == 200
    )


def test_two_factor_setup_reset_challenge_consumption(backend):
    m, c = backend
    a = account(backend)
    secret = c.post("/api/auth/2fa/setup", headers=a[1]).json()["secret"]
    assert (
        c.post(
            "/api/auth/2fa/enable", headers=a[1], json={"code": m._totp_code(secret)}
        ).status_code
        == 200
    )
    assert c.post("/api/auth/2fa/setup", headers=a[1]).status_code == 409
    login = c.post(
        "/api/auth/login",
        json={"username": a[2]["user"]["username"], "password": "correct-password"},
    ).json()
    payload = {"challenge": login["challenge"], "code": m._totp_code(secret)}
    assert c.post("/api/auth/2fa/login", json=payload).status_code == 200
    assert c.post("/api/auth/2fa/login", json=payload).status_code == 401


def test_private_message_audit_no_second_writer_lock_and_blocking(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    connect_accounts(backend, a, b)
    response = c.post(
        f"/api/messages/{b[0]}", headers=a[1], json={"body": "private sentinel"}
    )
    assert response.status_code == 201, response.text
    assert (
        c.post(
            f"/api/communication/conversation-controls/{a[0]}",
            headers=b[1],
            json={"action": "block"},
        ).status_code
        == 200
    )
    assert (
        c.post(
            f"/api/messages/{b[0]}", headers=a[1], json={"body": "blocked"}
        ).status_code
        == 403
    )
    assert (
        c.post(
            "/api/communication/direct-calls",
            headers=a[1],
            json={"callee_user_id": b[0]},
        ).status_code
        == 403
    )


def test_message_search_respects_deletion(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    connect_accounts(backend, a, b)
    msg = c.post(
        f"/api/messages/{b[0]}", headers=a[1], json={"body": "search_hidden_sentinel"}
    ).json()
    assert (
        c.delete(f"/api/messages/message/{msg['id']}", headers=a[1]).status_code == 204
    )
    response = c.get(
        f"/api/messages/search/{b[0]}", headers=a[1], params={"q": "search_hidden"}
    )
    assert response.status_code == 200, response.text
    assert response.json()["results"] == []


def test_space_creation_and_job_creation_audits_share_transaction(backend):
    m, c = backend
    a = account(backend)
    response = c.post(
        "/api/communication/spaces", headers=a[1], json={"name": "Test space"}
    )
    assert response.status_code == 201, response.text
    response = c.post("/api/jobs", headers=a[1], json={"title": "Test opportunity"})
    assert response.status_code == 201, response.text


def test_portfolio_learning_explore_auth_helper_exists(backend):
    m, c = backend
    a = account(backend)
    for path in (
        "/api/portfolio/projects",
        "/api/learning/courses",
        "/api/learning/certifications",
        "/api/explore/interests",
        "/api/explore",
    ):
        response = c.get(path, headers=a[1])
        assert response.status_code == 200, (path, response.text)
    assert (
        c.post(
            "/api/portfolio/projects", headers=a[1], json={"title": "Project"}
        ).status_code
        == 201
    )
    assert c.put(
        "/api/explore/interests", headers=a[1], json={"interests": ["Python", "python"]}
    ).json()["interests"] == ["Python"]


def test_private_event_requires_invitation_and_private_course_hidden(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    event = c.post(
        "/api/events",
        headers=a[1],
        json={
            "title": "Private event",
            "start_at": "2026-10-17T12:00:00Z",
            "visibility": "private",
        },
    ).json()["id"]
    assert (
        c.put(
            f"/api/events/{event}/rsvp", headers=b[1], json={"status": "going"}
        ).status_code
        == 403
    )
    course = c.post(
        "/api/learning/courses",
        headers=a[1],
        json={"title": "Private course", "visibility": "Private"},
    ).json()["id"]
    assert (
        c.post(f"/api/learning/courses/{course}/enroll", headers=b[1]).status_code
        == 404
    )


def test_upload_signatures_associations_and_owned_files(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    response = c.post(
        "/api/files/upload",
        headers=a[1],
        files={"upload": ("fake.png", b"not a PNG", "image/png")},
    )
    assert response.status_code == 415, response.text
    before = set(m.UPLOAD_DIR.iterdir())
    response = c.post(
        "/api/files/upload",
        headers=a[1],
        data={"record_id": "98765"},
        files={"upload": ("record.txt", b"hello", "text/plain")},
    )
    assert response.status_code == 400, response.text
    assert set(m.UPLOAD_DIR.iterdir()) == before
    response = c.post(
        "/api/files/upload",
        headers=a[1],
        files={"upload": ("safe.txt", b"hello", "text/plain")},
    )
    assert response.status_code == 201, response.text
    file_id = response.json()["id"]
    project = c.post(
        "/api/portfolio/projects", headers=b[1], json={"title": "Project"}
    ).json()["id"]
    assert (
        c.put(
            f"/api/portfolio/projects/{project}",
            headers=b[1],
            json={"title": "Project", "media_file_id": file_id},
        ).status_code
        == 400
    )
    job = c.post("/api/jobs", headers=a[1], json={"title": "Job"}).json()["id"]
    assert (
        c.post(
            f"/api/jobs/{job}/apply", headers=b[1], json={"resume_file_id": file_id}
        ).status_code
        == 400
    )


def test_post_public_search_and_foreign_key_cascade(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    post = c.post(
        "/api/posts",
        headers=a[1],
        json={"text": "unique_public_search_sentinel", "visibility": "PUBLIC"},
    ).json()["id"]
    response = c.get(
        "/api/search",
        headers=b[1],
        params={"q": "unique_public_search", "category": "posts"},
    )
    assert response.status_code == 200, response.text
    assert any(r["id"] == post for r in response.json()["results"])
    assert c.post(f"/api/posts/{post}/like", headers=b[1]).status_code == 201
    assert c.delete(f"/api/posts/{post}", headers=a[1]).status_code == 204
    with m.db() as conn:
        assert not conn.execute(
            "SELECT 1 FROM post_likes WHERE post_id=?", (post,)
        ).fetchone()


def test_direct_call_waits_for_acceptance_and_no_reaccept(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    connect_accounts(backend, a, b)
    response = c.post(
        "/api/communication/direct-calls", headers=a[1], json={"callee_user_id": b[0]}
    )
    assert response.status_code == 201, response.text
    call = response.json()
    mid = call["meeting_id"]
    with m.db() as conn:
        assert (
            conn.execute(
                "SELECT state FROM communication_meeting_participants WHERE meeting_id=? AND user_id=?",
                (mid, b[0]),
            ).fetchone()["state"]
            == "waiting"
        )
    assert (
        c.post(
            f"/api/communication/meetings/{mid}/polls",
            headers=b[1],
            json={"question": "Question?", "options": ["One", "Two"]},
        ).status_code
        == 403
    )
    path = f"/api/communication/direct-calls/{call['call_id']}/action"
    assert c.post(path, headers=b[1], json={"action": "accept"}).status_code == 200
    assert c.post(path, headers=b[1], json={"action": "accept"}).status_code == 409
    assert c.post(path, headers=a[1], json={"action": "end"}).status_code == 200
    assert c.post(path, headers=b[1], json={"action": "accept"}).status_code == 409


def test_meeting_notification_actor_positions_and_removed_participants(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    meeting = c.post(
        "/api/communication/meetings", headers=a[1], json={"title": "Meeting"}
    ).json()
    mid = meeting["id"]
    response = c.post(
        f"/api/communication/meetings/{mid}/invite",
        headers=a[1],
        json={"invitee_user_id": b[0]},
    )
    assert response.status_code == 201, response.text
    with m.db() as conn:
        notification = conn.execute(
            "SELECT actor_user_id,type FROM notifications WHERE user_id=? AND type='MEETING_INVITE'",
            (b[0],),
        ).fetchone()
        assert notification["actor_user_id"] == a[0]
    assert (
        c.post(
            "/api/communication/meetings/join/" + meeting["meeting_code"], headers=b[1]
        ).status_code
        == 200
    )
    assert (
        c.post(
            f"/api/communication/meetings/{mid}/actions",
            headers=a[1],
            json={"action": "remove", "user_id": b[0]},
        ).status_code
        == 200
    )
    assert (
        c.post(
            "/api/communication/meetings/join/" + meeting["meeting_code"], headers=b[1]
        ).status_code
        == 403
    )


def test_body_size_actual_bytes_and_early_response_headers(backend):
    m, c = backend
    old = m.NOVEAM_MAX_REQUEST_BYTES
    m.NOVEAM_MAX_REQUEST_BYTES = 100
    try:
        response = c.post(
            "/api/auth/login", content=b"x" * 101, headers={"content-length": "0"}
        )
        assert response.status_code == 413, response.text
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        response = c.post(
            "/api/auth/login", content=b"", headers={"content-length": "bad"}
        )
        assert response.status_code == 400
    finally:
        m.NOVEAM_MAX_REQUEST_BYTES = old


def test_websocket_rejects_expired_session(backend):
    m, c = backend
    a = account(backend)
    with m.db() as conn:
        conn.execute(
            "UPDATE sessions SET expires_at='2000-01-01' WHERE user_id=?", (a[0],)
        )
    with pytest.raises(WebSocketDisconnect) as caught:
        with c.websocket_connect("/ws/communication?token=" + a[2]["token"]):
            pass
    assert caught.value.code == 4401


def test_logs_do_not_expose_exception_messages_and_handler_failure(backend, caplog):
    m, c = backend

    @m.logged_operation
    def fail():
        raise ValueError("SECRET_SENTINEL")

    with caplog.at_level(logging.DEBUG, logger="noveam.backend"):
        with pytest.raises(ValueError):
            fail()
    assert "SECRET_SENTINEL" not in caplog.text
    old = m.logger

    class Broken:
        def log(self, *args):
            raise RuntimeError("sink down")

    m.logger = Broken()
    try:
        with pytest.raises(ValueError, match="SECRET_SENTINEL"):
            fail()
    finally:
        m.logger = old


def test_migrations_repeat_and_blank_fields_rejected(backend):
    m, c = backend
    m.initialize_database()
    a = account(backend)
    assert c.post("/api/posts", headers=a[1], json={"text": "   "}).status_code == 422
    assert c.post("/api/jobs", headers=a[1], json={"title": "   "}).status_code == 422


def test_account_deletion_cleans_uploaded_bytes_and_keeps_anonymous_audit(backend):
    m, c = backend
    a = account(backend)
    upload = c.post(
        "/api/files/upload",
        headers=a[1],
        files={"upload": ("delete.txt", b"cleanup", "text/plain")},
    ).json()
    with m.db() as conn:
        stored = conn.execute(
            "SELECT stored_name FROM uploaded_files WHERE id=?", (upload["id"],)
        ).fetchone()["stored_name"]
    path = m._stored_path(stored)
    assert path.exists()
    response = c.request(
        "DELETE",
        "/api/account/me",
        headers=a[1],
        json={"current_password": "correct-password", "confirmation": "DELETE"},
    )
    assert response.status_code == 200, response.text
    assert not path.exists()
    with m.db() as conn:
        assert conn.execute(
            "SELECT 1 FROM audit_log WHERE action='account_deleted' AND user_id IS NULL"
        ).fetchone()
        assert not conn.execute("SELECT 1 FROM users WHERE id=?", (a[0],)).fetchone()


def test_two_factor_same_time_code_not_reusable_across_challenges(backend):
    m, c = backend
    a = account(backend)
    secret = c.post("/api/auth/2fa/setup", headers=a[1]).json()["secret"]
    code = m._totp_code(secret)
    assert (
        c.post("/api/auth/2fa/enable", headers=a[1], json={"code": code}).status_code
        == 200
    )
    credentials = {"username": a[2]["user"]["username"], "password": "correct-password"}
    first = c.post("/api/auth/login", json=credentials).json()["challenge"]
    second = c.post("/api/auth/login", json=credentials).json()["challenge"]
    assert (
        c.post(
            "/api/auth/2fa/login", json={"challenge": first, "code": code}
        ).status_code
        == 200
    )
    assert (
        c.post(
            "/api/auth/2fa/login", json={"challenge": second, "code": code}
        ).status_code
        == 401
    )


def test_authentication_attempt_limit(backend):
    m, c = backend
    key = ("test", uuid.uuid4().hex)
    for _ in range(10):
        m._check_auth_attempt(key)
    with pytest.raises(m.HTTPException) as caught:
        m._check_auth_attempt(key)
    assert caught.value.status_code == 429


def test_mute_notifications_and_read_receipt_setting(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    connect_accounts(backend, a, b)
    assert (
        c.post(
            f"/api/communication/conversation-controls/{a[0]}",
            headers=b[1],
            json={"action": "mute"},
        ).status_code
        == 200
    )
    assert (
        c.put(
            "/api/communication/settings", headers=b[1], json={"read_receipts": False}
        ).status_code
        == 200
    )
    msg = c.post(
        f"/api/messages/{b[0]}", headers=a[1], json={"body": "muted sentinel"}
    ).json()["id"]
    assert c.get(f"/api/messages/{a[0]}", headers=b[1]).status_code == 200
    with m.db() as conn:
        assert not conn.execute(
            "SELECT 1 FROM notifications WHERE user_id=? AND type='NEW_MESSAGE'",
            (b[0],),
        ).fetchone()
        assert (
            conn.execute(
                "SELECT read_at FROM private_messages WHERE id=?", (msg,)
            ).fetchone()["read_at"]
            is None
        )


def test_websocket_presence_not_exposed_to_unconnected_user(backend):
    m, c = backend
    a = account(backend)
    b = account(backend)
    with c.websocket_connect("/ws/communication?token=" + b[2]["token"]) as target:
        assert target.receive_json()["type"] == "ready"
        with c.websocket_connect(
            "/ws/communication?token=" + a[2]["token"]
        ) as observer:
            assert observer.receive_json()["type"] == "ready"
            observer.send_json({"type": "presence_query", "peer_id": b[0]})
            assert observer.receive_json()["online"] is False


def test_upload_failed_insert_removes_new_disk_file(backend, monkeypatch):
    m, c = backend
    a = account(backend)
    before = set(m.UPLOAD_DIR.iterdir())
    # Force a metadata failure after writing bytes, without touching another user's files.
    with m.db() as conn:
        conn.execute(
            "CREATE TRIGGER fail_test_upload BEFORE INSERT ON uploaded_files BEGIN SELECT RAISE(ABORT,'synthetic failure'); END"
        )
    try:
        with pytest.raises(sqlite3.IntegrityError):
            c.post(
                "/api/profile-media/upload",
                headers=a[1],
                data={"kind": "photo"},
                files={
                    "upload": (
                        "image.png",
                        b"\x89PNG\r\n\x1a\n" + b"x" * 16,
                        "image/png",
                    )
                },
            )
        assert set(m.UPLOAD_DIR.iterdir()) == before
    finally:
        with m.db() as conn:
            conn.execute("DROP TRIGGER fail_test_upload")


def test_cleanup_queue_retry_is_durable(backend, monkeypatch):
    m, c = backend
    a = account(backend)
    upload = c.post(
        "/api/files/upload",
        headers=a[1],
        files={"upload": ("retry.txt", b"cleanup", "text/plain")},
    ).json()
    with m.db() as conn:
        stored = conn.execute(
            "SELECT stored_name FROM uploaded_files WHERE id=?", (upload["id"],)
        ).fetchone()["stored_name"]
    path = m._stored_path(stored)
    real = pathlib.Path.unlink

    def fail_locked(self, *args, **kwargs):
        if self == path:
            raise PermissionError("synthetic file lock")
        return real(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pathlib.Path, "unlink", fail_locked)
        assert c.delete(f"/api/files/{upload['id']}", headers=a[1]).status_code == 204
    with m.db() as conn:
        assert conn.execute(
            "SELECT 1 FROM pending_file_cleanup WHERE stored_name=?", (stored,)
        ).fetchone()
    m.flush_file_cleanup()
    assert not path.exists()
    with m.db() as conn:
        assert not conn.execute(
            "SELECT 1 FROM pending_file_cleanup WHERE stored_name=?", (stored,)
        ).fetchone()


def test_all_get_api_routes_respond_without_server_errors_and_signatures_survive(
    backend,
):
    m, c = backend
    for route in m.app.routes:
        if "GET" not in getattr(route, "methods", set()) or not route.path.startswith(
            "/api/"
        ):
            continue
        path = re.sub(r"\{[^}]+\}", "1", route.path)
        response = c.get(path)
        assert response.status_code < 500, (path, response.text)
    for path, methods in m.app.openapi()["paths"].items():
        for operation in methods.values():
            if isinstance(operation, dict):
                assert not {"args", "kwargs"} & {
                    p["name"] for p in operation.get("parameters", [])
                }
