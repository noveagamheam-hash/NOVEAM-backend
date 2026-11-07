# NOVEAM v1.6 Rules and Safety

This package updates the supplied FastAPI backend and includes a frontend drop-in. The staging configuration uses synthetic accounts and a separate PostgreSQL database. The existing admin server and FastAPI routes are included.

## Staging setup

1. Start from a disposable PostgreSQL database or a dedicated Neon staging branch. Use the existing backend schema. On an empty database, first initialize the backend with `NOVEAM_POLICY_ENABLED=0` and stop it before applying the migration. Keep external integrations pointed at staging or disabled.
2. Install `backend-python/requirements.txt`. With `DATABASE_URL` set to the staging connection, run the migration with `psql -X -v ON_ERROR_STOP=1 -f migrations/016_policy_safety.sql` from `backend-python`. Supply connection settings through your normal secure PostgreSQL configuration. The migration creates only new tables and indexes.
3. Set `NOVEAM_ENVIRONMENT=staging`. Run `python publish_policy.py policy_catalog.v1.6.staging.json`. The 60 numbered entries and terms are clearly marked synthetic fixtures. For approved text, supply a catalog with the same structure and `status` set to `approved`. Use a fresh database for approved v1.6 if that version is already seeded with the fixture. A published version is immutable through the publisher.
4. Start the FastAPI app with `NOVEAM_POLICY_ENABLED=1` and `NRS_A01_OBSERVE=1`. Protected routes then require agreement. `NOVEAM_SAFETY_ENFORCE=1` separately enables application of human-issued restrictions in staging. Keep it at `0` for observation exercises where accounts should remain unrestricted.
5. Provision explicitly authorized synthetic staff using the staging database. Insert a vetted user ID and one of `moderator`, `appeal_reviewer`, or `auditor` into `safety_staff`. Existing profile roles and self-service registration grant none of these permissions. Give independent reviewers separate accounts. Staff grants and revocations are out-of-band database operations. Record them in your staging change log.

## Frontend contract

Place the files in `frontend-integration` at the staging frontend root. The Rules page defaults to a same-origin API. For a split-origin setup, set the `noveam-api-origin` meta value in `rules.html` to the staging API origin and configure the backend to allow the staging frontend origin.

After register, login, or completed two-factor sign-in, store the token using the frontend's existing token handling and route to the response's `policy.next`. Allow only `/rules.html`, `/visitor/onboarding.html`, and `/feed.html` as destinations. For an existing session, use `GET /api/v1/policies/status` or the added `policy` field on `GET /api/users/me`. In the shared frontend API handler, route a `428` with code `RULES_ACCEPTANCE_REQUIRED` to the Rules screen. Older clients can receive this response until their routing is wired. For a `403 SAFETY_RESTRICTION_ACTIVE`, surface the action notice and appeal routes. Static HTML may still load, while the API checks the session before returning protected data.

The Rules page displays the catalog and terms before collecting an explicit checkbox agreement. The server uses the authenticated identity and exact document digest. Repeating acceptance preserves the original timestamp. Onboarding drafts, revisions, skips, and completions remain in the existing table. Accounts with no onboarding record are routed to onboarding. Existing support routes for logout, sessions, account deletion, reporting, and appeals remain usable without agreement.

## Safety review

- `GET /api/v1/policies/current` and `/rules` serve the versioned catalog. `GET /status` and `POST /accept` under the same prefix support agreement.
- `POST /api/v1/safety/reports`, `GET /reports/mine`, `GET /actions/mine`, and `POST /appeals` under the safety prefix let members file reports, see their cases or notices, and challenge actions. The original communication report endpoint also opens a case while the feature is enabled.
- Under `/api/v1/admin/moderation`, staff use `GET /cases`, `POST /actions`, `GET /appeals`, `POST /appeals/{id}/resolve`, and `GET /audit`. Queues and histories return up to 100 rows and support an `after` ID cursor.
- The observation prototype uses two English text patterns for potential direct threats and credential solicitation. It observes public primary-feed posts and comments and records a signal and content pointer. A flag opens a case and cannot issue an action. Reviewers should verify the underlying evidence and context. An urgent flag marks the review queue entry.
- Human reviewers can dismiss a case, issue a reminder or formal warning, or record a temporary restriction. The staging default requires three current, unchallenged formal warnings in the last 90 days for ordinary restrictions. Reviewers should verify that warnings concern distinct incidents. Emergency restrictions require a human decision and rationale, and can bypass the warning threshold. Restrictions last 1 to 168 hours. Separate appeal reviewers can reverse actions and restore access if no other restriction applies. Action rationales are user-facing. Keep confidential evidence out of them.
- Store sensitive reports only in approved staging workflows. The package supplies staff roles and audit trails. Specialist case routing, moderator frontend screens, multilingual detection, push delivery of action notices, translations and narration remain separate work.

## Tests

Install pytest and httpx alongside the backend requirements. From `backend-python` run `python -m pytest -q test_policy_safety.py`. For the API and migration tests set `NOVEAM_TEST_DATABASE_URL` to a **local disposable PostgreSQL** instance. The tests create and remove isolated schemas, apply the migration twice, and use synthetic accounts. The bundled legacy `test_backend.py` assumes SQLite even though this archive uses PostgreSQL. Use the new suite for these workflows.

## Rollback

1. Set `NRS_A01_OBSERVE=0` and `NOVEAM_SAFETY_ENFORCE=0`, then restart the staging service to stop observation and restriction checks. To roll back the entire feature, also set `NOVEAM_POLICY_ENABLED=0` and remove the staging frontend routing hooks. Restart all staging instances together.
2. If rolling back application code, use the original archive. Leave the additive tables in place to preserve agreements, reports, appeals, and audit history. Active restrictions recorded before rollback should be reviewed before enforcement is enabled again.
3. If a disposable branch must be discarded, preserve any needed staging records first and remove the branch or restore its pre-migration snapshot. Never use destructive rollback SQL on an active database.
