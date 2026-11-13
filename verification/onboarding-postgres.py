"""Exercise the onboarding API against an isolated PostgreSQL schema."""
from contextlib import contextmanager
import os
from pathlib import Path
import sys
import uuid
from time import perf_counter

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend/backend-python'))
from onboarding_backend import initialize_onboarding_database, install_onboarding_routes

dsn = os.environ['NOVEAM_TEST_POSTGRES_DSN']
schema = 'onboarding_test_' + uuid.uuid4().hex


@contextmanager
def db():
    with psycopg.connect(dsn, row_factory=dict_row, options=f'-c search_path={schema}') as conn:
        yield conn


def authenticate(authorization):
    if authorization == 'Bearer pending-rules':
        raise HTTPException(428, {'code': 'RULES_ACCEPTANCE_REQUIRED'})
    if authorization == 'Bearer pending-mfa':
        raise HTTPException(403, 'MFA enrollment required')
    if authorization not in ('Bearer 1', 'Bearer 2', 'Bearer 3', 'Bearer 4', 'Bearer 5'):
        raise HTTPException(401, 'Authentication required')
    return {'id': int(authorization[-1]), 'full_name': 'Registered Name'}, None


with psycopg.connect(dsn, autocommit=True) as admin:
    admin.execute(f'CREATE SCHEMA {schema}')
    try:
        with db() as conn:
            conn.execute("""
                CREATE TABLE users(id integer PRIMARY KEY, full_name text);
                CREATE TABLE profiles(user_id integer PRIMARY KEY, display_name text,
                    headline text DEFAULT '', bio text DEFAULT '', location text DEFAULT '');
                CREATE TABLE user_interests(user_id integer, interest text, created_at text);
                INSERT INTO users VALUES (1,'Registered Name'), (2,'Existing Name'),
                    (3,'Legacy Name'), (4,'Existing Name'), (5,'New Wizard');
                CREATE TABLE user_settings(user_id integer PRIMARY KEY, updated_at text, language text,
                    large_text integer, reduced_motion integer, notifications integer);
                INSERT INTO profiles VALUES (2,'Existing Name','Headline','',''),
                    (4,'Existing Name','Headline','','');
                INSERT INTO user_interests VALUES (2,'Social',''), (4,'Learning','');
            """)
        initialize_onboarding_database(db)
        with db() as conn:
            conn.execute("""
                INSERT INTO user_onboarding(user_id,step,completed_at,skipped)
                VALUES (3,3,CURRENT_TIMESTAMP,true);
                INSERT INTO user_onboarding(user_id,display_name) VALUES (4,'Old seed');
            """)
        verified = False

        def complete_auth(authorization):
            if authorization == 'Bearer 5' and not verified:
                raise HTTPException(403, 'MFA enrollment required')
            return authenticate(authorization)

        app = FastAPI()
        install_onboarding_routes(app, db, complete_auth, authenticate)
        client = TestClient(app)

        def call(method, user, path='/me', **kwargs):
            return client.request(method, '/api/onboarding' + path,
                                  headers={'Authorization': f'Bearer {user}'}, **kwargs)

        for user, status in [('expired', 401), ('pending-mfa', 403), ('pending-rules', 428)]:
            assert call('GET', user).status_code == status

        fresh = call('GET', 1).json()
        assert fresh['step'] == 0 and not fresh['completed']
        assert call('POST', 1, '/complete', json={'revision': 0, 'skip': True}).status_code == 422
        assert call('PATCH', 1, json={'step': 1, 'revision': 0}).status_code == 422
        draft = {'step': 2, 'revision': 0, 'display_name': 'Confirmed Name'}
        assert call('PATCH', 1, json=draft).json()['step'] == 2
        assert call('PATCH', 1, json=draft).status_code == 409
        draft.update(step=1, revision=1)
        assert call('PATCH', 1, json=draft).json()['step'] == 2
        assert call('GET', 1).json()['revision'] == 2
        assert call('POST', 1, '/complete', json={'revision': 2}).status_code == 422
        done = call('POST', 1, '/complete', json={'revision': 2, 'skip': True}).json()
        assert done['completed'] and done['skipped']
        assert call('POST', 1, '/complete', json={'revision': 2, 'skip': True}).json() == done

        existing = call('GET', 2).json()
        assert existing['step'] == 3 and existing['interests'] == ['Social']
        assert call('GET', 3).json()['completed']
        assert call('GET', 4).json()['interests'] == ['Learning']
        assert call('POST', 2, '/complete', json={'revision': 0}).json()['completed']
        draft = {'revision': 0, 'step': 3, 'wizard_step': 4, 'display_name': 'New Wizard',
                 'preferences': {'language': 'te', 'large_text': True}}
        assert call('PATCH', 5, json=draft).status_code == 200
        assert call('GET', 5).json()['wizard_step'] == 4
        draft.update(revision=1, wizard_step=5, preferences={})
        assert call('PATCH', 5, json=draft).status_code == 403
        assert call('POST', 5, '/complete', json={'revision': 1}).status_code == 403
        verified = True
        assert call('POST', 5, '/complete', json={'revision': 1}).status_code == 422
        assert call('PATCH', 5, json=draft).status_code == 200
        draft.update(revision=2, wizard_step=6, preferences={'notifications': False})
        assert call('PATCH', 5, json=draft).status_code == 200
        assert call('POST', 5, '/complete', json={'revision': 3}).json()['completed']
        if os.getenv('NOVEAM_BENCHMARK'):
            for user in (3, 4):
                samples = []
                for _ in range(50):
                    start = perf_counter()
                    result = call('GET', user)
                    samples.append((perf_counter() - start) * 1000)
                    assert result.status_code == 200
                samples.sort()
                print(f'GET /api/onboarding/me ({"completed" if user == 3 else "unconfirmed"}, n=50): '
                      f'p50={samples[24]:.1f} ms, p95={samples[47]:.1f} ms, max={samples[-1]:.1f} ms')
        with db() as conn:
            assert conn.execute('SELECT language, large_text, notifications FROM user_settings WHERE user_id=5').fetchone() == {
                'language': 'te', 'large_text': 1, 'notifications': 0}
            assert conn.execute('SELECT display_name FROM profiles WHERE user_id=1').fetchone()['display_name'] == 'Confirmed Name'
            assert conn.execute('SELECT interest FROM user_interests WHERE user_id=2').fetchone()['interest'] == 'Social'
    finally:
        admin.execute(f'DROP SCHEMA {schema} CASCADE')
print('PASS PostgreSQL resume, legacy completion, revision conflicts, mandatory name and protected endpoints')
