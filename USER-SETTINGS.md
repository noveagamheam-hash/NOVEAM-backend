# NOVEAM user settings on staging

Use the frontend and backend settings ZIPs together. They extend the supplied v1.6 Rules and Safety staging packages.

## Database and launch

1. Use a disposable PostgreSQL database or a dedicated Neon staging branch. Follow the existing backend Rules and Safety setup through migration 016. On an empty database, initialize the base schema as described there.
2. From backend-python, apply `migrations/017_user_settings.sql` using `psql -X -v ON_ERROR_STOP=1 -f migrations/017_user_settings.sql` with your staging connection configuration. Apply it before serving the new settings code. The four new columns use false defaults and retain existing rows and values. Re-running the migration is safe.
3. Start the staging backend and serve the extracted frontend using the existing same-origin instructions in STAGING-RULES.md. For split-origin staging, configure the shared API origin and CORS as described there.
4. Sign in with a synthetic account, accept Rules when prompted, and open settings.html. Save a changed preference and reload. Sign in as a second test user to verify account separation. Use both accounts to verify directory visibility, connection requests, communication blocking, and reporting. Revisit the existing profile, contact, password, sessions, 2FA, and device push flows. In an installed PWA, reload online to receive the new service worker. Preference loading and saving require connectivity.

## Settings behavior and API

- `GET /api/settings/me` returns account preferences and defaults for a user with no row. `PATCH /api/settings/me` accepts only declared preference fields and returns the saved snapshot. It applies supplied fields atomically, using the account from the bearer session. Changes to distinct fields from separate clients do not overwrite one another. For the same field the last committed write wins. Legacy `PUT` remains available and updates the original fields.
- Theme, larger text and reduced motion apply on Settings. Preferred language is saved on the account. The Settings interface is currently English.
- Profile visibility applies to the People directory. Private is hidden. Connections is visible to accepted connections. Users without a settings row retain the earlier directory behavior. Existing content and other discovery surfaces have their own visibility rules. Activity status combines with Communication settings for presence responses. Connection request opt-out prevents new incoming requests and leaves accepted connections intact.
- In-app notification behavior continues to use the existing notification preference. Email and SMS are saved as delivery preferences. Delivery integration is outside this package. Browser push stays per device.
- The Safety section lists the signed-in account's communication blocks via `GET /api/settings/me/blocked-accounts`. Block and unblock use the existing conversation controls. Reports use the existing Rules and Safety reporting endpoint and require that feature to be enabled.
- Profile editing and account security reuse the existing routes and screens. API settings responses bypass the service worker, and account preferences are read from the server rather than a shared browser cache.

## Tests and rollback

Run `python -m pytest -q test_user_settings.py test_policy_safety.py` in backend-python with `NOVEAM_TEST_DATABASE_URL` pointing to disposable local PostgreSQL. Tests use synthetic accounts and temporary schemas. For staging rollback, restore both original v1.6 packages. Leave the additive columns in place to retain saved preferences.

Compatibility fix: GET settings uses defaults for absent new columns on older schemas. Apply migration 017 before writing the new preference fields. If authenticated requests still return 500, inspect the Render traceback for the exact database or application error.
