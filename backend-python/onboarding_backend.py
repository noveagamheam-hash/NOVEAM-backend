"""Account-scoped onboarding for the NOVEAM PostgreSQL backend."""
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, field_validator
from psycopg.types.json import Jsonb

SCHEMA = """
CREATE TABLE IF NOT EXISTS user_onboarding (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    step INTEGER NOT NULL DEFAULT 0 CHECK (step BETWEEN 0 AND 3),
    display_name TEXT NOT NULL DEFAULT '',
    headline TEXT NOT NULL DEFAULT '',
    bio TEXT NOT NULL DEFAULT '',
    location TEXT NOT NULL DEFAULT '',
    interests JSONB NOT NULL DEFAULT '[]'::jsonb,
    revision INTEGER NOT NULL DEFAULT 0,
    skipped BOOLEAN NOT NULL DEFAULT FALSE,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class OnboardingDraft(BaseModel):
    revision: int = Field(ge=0)
    step: int = Field(ge=0, le=3)
    display_name: str = Field(default='', max_length=100)
    headline: str = Field(default='', max_length=160)
    bio: str = Field(default='', max_length=1000)
    location: str = Field(default='', max_length=160)
    interests: list[str] = Field(default_factory=list, max_length=30)

    @field_validator('display_name', 'headline', 'bio', 'location')
    @classmethod
    def trim_text(cls, value):
        return value.strip()

    @field_validator('interests')
    @classmethod
    def normalize_interests(cls, values):
        result = list(dict.fromkeys(value.strip() for value in values if value.strip()))
        if any(len(value) > 80 for value in result):
            raise ValueError('Interest names must be at most 80 characters')
        return result


class OnboardingCompletion(BaseModel):
    revision: int = Field(ge=0)
    skip: bool = False


def initialize_onboarding_database(db):
    with db() as conn:
        conn.execute(SCHEMA)


def install_onboarding_routes(app, db, current_user):
    router = APIRouter(prefix='/api/onboarding', tags=['onboarding'])

    def state(conn, user, lock=False):
        # Existing profile/interests seed the first draft without modifying them.
        profile = conn.execute(
            'SELECT display_name,headline,bio,location FROM profiles WHERE user_id=%s',
            (user['id'],),
        ).fetchone()
        interests = [row['interest'] for row in conn.execute(
            'SELECT interest FROM user_interests WHERE user_id=%s ORDER BY interest',
            (user['id'],),
        )]
        profile = profile or {}
        conn.execute(
            '''INSERT INTO user_onboarding(user_id,display_name,headline,bio,location,interests)
               VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO NOTHING''',
            (user['id'], profile.get('display_name') or user['full_name'],
             profile.get('headline', ''), profile.get('bio', ''), profile.get('location', ''),
             Jsonb(interests)),
        )
        row = conn.execute(
            'SELECT * FROM user_onboarding WHERE user_id=%s' + (' FOR UPDATE' if lock else ''),
            (user['id'],),
        ).fetchone()
        return dict(row)

    def response(row):
        return {**row, 'completed': row['completed_at'] is not None}

    @router.get('/me')
    def get_onboarding(authorization: Optional[str] = Header(default=None)):
        user, _ = current_user(authorization)
        with db() as conn:
            return response(state(conn, user))

    @router.patch('/me')
    def save_onboarding(payload: OnboardingDraft, authorization: Optional[str] = Header(default=None)):
        user, _ = current_user(authorization)
        with db() as conn:
            current = state(conn, user, lock=True)
            if current['completed_at'] is not None:
                raise HTTPException(409, 'Onboarding is already complete. Reload this page.')
            if current['revision'] != payload.revision:
                raise HTTPException(409, 'Progress changed in another tab. Reload to continue.')
            row = conn.execute(
                '''UPDATE user_onboarding SET step=%s,display_name=%s,headline=%s,bio=%s,
                   location=%s,interests=%s,revision=revision+1,updated_at=CURRENT_TIMESTAMP
                   WHERE user_id=%s RETURNING *''',
                (payload.step, payload.display_name, payload.headline, payload.bio,
                 payload.location, Jsonb(payload.interests), user['id']),
            ).fetchone()
            return response(dict(row))

    @router.post('/complete')
    def complete_onboarding(payload: OnboardingCompletion, authorization: Optional[str] = Header(default=None)):
        user, _ = current_user(authorization)
        with db() as conn:
            current = state(conn, user, lock=True)
            if current['completed_at'] is not None:
                return response(current)
            if current['revision'] != payload.revision:
                raise HTTPException(409, 'Progress changed in another tab. Reload to continue.')
            if not payload.skip:
                name = current['display_name'].strip()
                if not name:
                    raise HTTPException(422, 'Enter a display name before finishing.')
                conn.execute('UPDATE users SET full_name=%s WHERE id=%s', (name, user['id']))
                conn.execute(
                    '''INSERT INTO profiles(user_id,display_name,headline,bio,location)
                       VALUES(%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET
                       display_name=excluded.display_name,headline=excluded.headline,
                       bio=excluded.bio,location=excluded.location''',
                    (user['id'], name, current['headline'], current['bio'], current['location']),
                )
                conn.execute('DELETE FROM user_interests WHERE user_id=%s', (user['id'],))
                now = datetime.now(timezone.utc).isoformat()
                if current['interests']:
                    with conn.cursor() as cursor:
                        cursor.executemany(
                            'INSERT INTO user_interests(user_id,interest,created_at) VALUES(%s,%s,%s)',
                            [(user['id'], interest, now) for interest in current['interests']],
                        )
            row = conn.execute(
                '''UPDATE user_onboarding SET step=3,skipped=%s,completed_at=CURRENT_TIMESTAMP,
                   updated_at=CURRENT_TIMESTAMP,revision=revision+1 WHERE user_id=%s RETURNING *''',
                (payload.skip, user['id']),
            ).fetchone()
            return response(dict(row))

    app.include_router(router)
