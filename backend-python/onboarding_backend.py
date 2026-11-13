"""Account-scoped onboarding for the NOVEAM PostgreSQL backend."""
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, StrictBool, field_validator
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
ALTER TABLE user_onboarding ADD COLUMN IF NOT EXISTS wizard_step INTEGER
    CHECK (wizard_step BETWEEN 1 AND 6);
ALTER TABLE user_onboarding ADD COLUMN IF NOT EXISTS preferences JSONB NOT NULL DEFAULT '{}'::jsonb;
"""


class OnboardingPreferences(BaseModel):
    language: str | None = Field(default=None, pattern=r"^[a-zA-Z]{2,3}(?:-[a-zA-Z0-9]{2,8}){0,2}$")
    large_text: StrictBool | None = None
    reduced_motion: StrictBool | None = None
    notifications: StrictBool | None = None


class OnboardingDraft(BaseModel):
    wizard_step: int | None = Field(default=None, ge=1, le=6)
    preferences: OnboardingPreferences | None = None
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


def initial_step(profile, interests):
    if not (profile.get('display_name') or '').strip():
        return 0
    if not any((profile.get(field) or '').strip() for field in ('headline', 'bio', 'location')):
        return 1
    return 3 if interests else 2


def initialize_onboarding_database(db):
    with db() as conn:
        conn.execute(SCHEMA)


def install_onboarding_routes(app, db, current_user, onboarding_user=None):
    draft_user = onboarding_user or current_user
    router = APIRouter(prefix='/api/onboarding', tags=['onboarding'])

    def state(conn, user, lock=False):
        select = 'SELECT * FROM user_onboarding WHERE user_id=%s'
        locking = ' FOR UPDATE' if lock else ''
        # Confirmed drafts and completed legacy rows no longer use profile seeding.
        saved = conn.execute(
            select + ' AND (revision > 0 OR completed_at IS NOT NULL)' + locking,
            (user['id'],),
        ).fetchone()
        if saved is not None:
            return dict(saved)

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
            '''INSERT INTO user_onboarding(user_id,step,display_name,headline,bio,location,interests)
               VALUES(%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(user_id) DO UPDATE SET
                   step=excluded.step,display_name=excluded.display_name,
                   headline=excluded.headline,bio=excluded.bio,location=excluded.location,
                   interests=excluded.interests
               WHERE user_onboarding.revision=0 AND user_onboarding.completed_at IS NULL
                 AND (user_onboarding.step,user_onboarding.display_name,user_onboarding.headline,
                      user_onboarding.bio,user_onboarding.location,user_onboarding.interests)
                  IS DISTINCT FROM (excluded.step,excluded.display_name,excluded.headline,
                                    excluded.bio,excluded.location,excluded.interests)''',
            (user['id'], initial_step(profile, interests), profile.get('display_name') or user['full_name'],
             profile.get('headline', ''), profile.get('bio', ''), profile.get('location', ''),
             Jsonb(interests)),
        )
        row = conn.execute(
            select + locking,
            (user['id'],),
        ).fetchone()
        return dict(row)

    def response(row):
        return {**row, 'completed': row['completed_at'] is not None}

    @router.get('/me')
    def get_onboarding(authorization: Optional[str] = Header(default=None)):
        user, _ = draft_user(authorization)
        with db() as conn:
            draft = response(state(conn, user))
            if onboarding_user:
                settings = conn.execute(
                    'SELECT language,large_text,reduced_motion,notifications FROM user_settings WHERE user_id=%s',
                    (user['id'],),
                ).fetchone()
                draft['settings'] = dict(settings) if settings else {}
            return draft

    @router.patch('/me')
    def save_onboarding(payload: OnboardingDraft, authorization: Optional[str] = Header(default=None)):
        user, _ = draft_user(authorization)
        with db() as conn:
            current = state(conn, user, lock=True)
            if current['completed_at'] is not None:
                raise HTTPException(409, 'Onboarding is already complete. Reload this page.')
            if current['revision'] != payload.revision:
                raise HTTPException(409, 'Progress changed in another tab. Reload to continue.')
            if max(current['step'], payload.step) > 0 and not payload.display_name:
                raise HTTPException(422, 'Enter a display name before continuing.')
            if payload.wizard_step is not None and payload.wizard_step >= 5:
                current_user(authorization)
            preferences = payload.preferences.model_dump(exclude_none=True) if payload.preferences else {}
            row = conn.execute(
                '''UPDATE user_onboarding SET
                   wizard_step=GREATEST(wizard_step,%s),preferences=preferences || %s,
                   step=GREATEST(step,%s),display_name=%s,headline=%s,bio=%s,
                   location=%s,interests=%s,revision=revision+1,updated_at=CURRENT_TIMESTAMP
                   WHERE user_id=%s RETURNING *''',
                (payload.wizard_step, Jsonb(preferences), payload.step, payload.display_name, payload.headline, payload.bio,
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
            if current['wizard_step'] is not None and current['wizard_step'] < 6:
                raise HTTPException(422, 'Review your onboarding steps before finishing.')
            name = current['display_name'].strip()
            if not name or current['step'] < 1:
                raise HTTPException(422, 'Confirm your display name before finishing.')
            if not payload.skip and current['step'] != 3:
                raise HTTPException(422, 'Review your introduction before finishing.')
            conn.execute('UPDATE users SET full_name=%s WHERE id=%s', (name, user['id']))
            if payload.skip:
                conn.execute(
                    '''INSERT INTO profiles(user_id,display_name) VALUES(%s,%s)
                       ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name''',
                    (user['id'], name),
                )
            else:
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
            preferences = OnboardingPreferences.model_validate(current['preferences']).model_dump(exclude_none=True)
            if preferences:
                columns = ', '.join(preferences)
                placeholders = ', '.join(['%s'] * len(preferences))
                assignments = ', '.join(f'{key}=excluded.{key}' for key in preferences)
                values = [int(value) if isinstance(value, bool) else value for value in preferences.values()]
                conn.execute(
                    f'''INSERT INTO user_settings(user_id,updated_at,{columns})
                        VALUES(%s,%s,{placeholders}) ON CONFLICT(user_id) DO UPDATE
                        SET updated_at=excluded.updated_at,{assignments}''',
                    (user['id'], datetime.now(timezone.utc).isoformat(), *values),
                )
            row = conn.execute(
                '''UPDATE user_onboarding SET step=3,skipped=%s,completed_at=CURRENT_TIMESTAMP,
                   updated_at=CURRENT_TIMESTAMP,revision=revision+1 WHERE user_id=%s RETURNING *''',
                (payload.skip, user['id']),
            ).fetchone()
            return response(dict(row))

    app.include_router(router)
