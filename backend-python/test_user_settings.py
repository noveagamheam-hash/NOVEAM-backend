"""Settings API coverage using the policy suite's disposable PostgreSQL fixture."""

from test_policy_safety import MIGRATION, accept, account, backend


def test_settings_roundtrip_and_account_isolation(backend):
    main, client = backend
    first_id, first, first_state, first_credentials = account(client)
    second_id, second, second_state, _ = account(client)
    path = "/api/settings/me"

    assert client.get(path).status_code == 401
    assert client.patch(path, headers=first, json={"large_text": True}).status_code == 428
    assert accept(client, first, first_state).status_code == 200
    assert accept(client, second, second_state).status_code == 200

    # A pre-migration row retains its existing values.
    legacy = {"theme": "dark", "language": "en", "activity_status": False}
    assert client.put(path, headers=first, json=legacy).status_code == 200
    before = client.get(path, headers=first)
    assert before.status_code == 200, before.text
    assert before.json()["theme"] == "dark"
    assert before.json()["large_text"] is False
    migration = MIGRATION.with_name("017_user_settings.sql").read_text(encoding="utf-8")
    with main.db() as conn:
        conn.execute(migration)
        conn.execute(migration)

    response = client.get(path, headers=first)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    for key, value in legacy.items():
        assert response.json()[key] == value
    assert response.json()["email_notifications"] is False
    assert response.json()["sms_notifications"] is False

    for bad in ({}, {"user_id": second_id}, {"reduced_motion": "true"},
                {"profile_visibility": None}, {"language": "bad language"}):
        assert client.patch(path, headers=first, json=bad).status_code == 422

    extras = {"large_text": True, "reduced_motion": True, "email_notifications": True,
              "sms_notifications": True, "language": "ta", "connection_requests": False}
    response = client.patch(path, headers=first, json=extras)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    for key, value in extras.items():
        assert response.json()[key] == value
    assert response.json()["theme"] == "dark"
    assert client.get(path, headers=second).json()["large_text"] is False

    assert client.put(path, headers=first, json={"theme": "light"}).status_code == 200
    saved = client.get(path, headers=first).json()
    assert saved["theme"] == "light"
    assert saved["large_text"] is True
    assert saved["sms_notifications"] is True

    people = "/api/social/people"
    query = {"q": first_credentials["username"]}
    assert client.patch(path, headers=first, json={"profile_visibility": "public"}).status_code == 200
    visible = client.get(people, headers=second, params=query)
    assert visible.status_code == 200, visible.text
    assert [person["id"] for person in visible.json()["people"]] == [first_id]
    assert client.patch(path, headers=first, json={"profile_visibility": "private"}).status_code == 200
    assert client.get(people, headers=second, params=query).json()["people"] == []

    controls = f"/api/communication/conversation-controls/{second_id}"
    blocks = path + "/blocked-accounts"
    assert client.get(blocks).status_code == 401
    assert client.post(controls, headers=first, json={"action": "block"}).status_code == 200
    assert client.get(blocks, headers=first).json() == [{"peer_user_id": second_id}]
    assert client.get(blocks, headers=second).json() == []
    assert client.post(controls, headers=first, json={"action": "unblock"}).status_code == 200
    assert client.get(blocks, headers=first).json() == []


def test_first_patch_uses_database_defaults(backend):
    main, client = backend
    user_id, headers, state, _ = account(client)
    assert accept(client, headers, state).status_code == 200
    migration = MIGRATION.with_name("017_user_settings.sql").read_text(encoding="utf-8")
    with main.db() as conn:
        conn.execute(migration)
    response = client.patch("/api/settings/me", headers=headers, json={"large_text": True})
    assert response.status_code == 200, response.text
    assert response.json()["large_text"] is True
    assert response.json()["notifications"] is True
    assert response.json()["theme"] == "system"
    _, sender, sender_state, _ = account(client)
    assert accept(client, sender, sender_state).status_code == 200
    assert client.patch("/api/settings/me", headers=headers,
                        json={"connection_requests": False}).status_code == 200
    assert client.post(f"/api/social/connections/{user_id}", headers=sender).status_code == 403

