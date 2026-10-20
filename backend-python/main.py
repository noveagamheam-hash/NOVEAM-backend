"""NOVEAM PostgreSQL backend: configuration, schemas, services, routes and lifecycle.

Run with: uvicorn main:app --host 127.0.0.1 --port 8000
See CHANGELOG.md and DEPLOYMENT.md before replacing an existing deployment.
"""

from collections import OrderedDict, deque
from contextlib import contextmanager, asynccontextmanager
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from supabase import create_client, Client
import asyncio
import base64
import functools
import hashlib
import hmac
import inspect
import json
import logging
import os
import secrets
import psycopg
from psycopg.rows import dict_row
from onboarding_backend import initialize_onboarding_database, install_onboarding_routes
from push_backend import initialize_push_database, install_push_routes, enqueue_push, push_worker
from psycopg import sql as pg_sql
import time
import threading
import uuid
from fastapi import (
    FastAPI,
    HTTPException,
    Header,
    status,
    UploadFile,
    File,
    Form,
    WebSocket,
    WebSocketDisconnect,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from starlette.concurrency import run_in_threadpool

try:
    from livekit import api as livekit_api
except ImportError:
    livekit_api = None

# Logging is configured once; an existing host/root handler is respected.
logger = logging.getLogger("noveam.backend")


class NoveamModel(BaseModel):
    @field_validator("title", "name", "text", "body", "question", check_fields=False)
    @classmethod
    def reject_blank_text(cls, value):
        if isinstance(value, str) and not value.strip():
            raise ValueError("Text must contain non-whitespace characters")
        return value


def safe_log(level, operation, outcome, duration_ms=None, status_code=None):
    """Only code-owned labels, numeric status and timing; never request/exception data."""
    try:
        logger.log(
            level,
            "operation=%s outcome=%s status=%s duration_ms=%s",
            operation,
            outcome,
            status_code,
            round(duration_ms, 2) if duration_ms is not None else None,
        )
    except Exception:
        pass


def logged_operation(fn=None, *, level=logging.DEBUG):
    """Preserve sync/async signatures; helper return is not a transaction-commit claim."""

    def decorate(func):
        def failure(exc, started):
            code = exc.status_code if isinstance(exc, HTTPException) else None
            severity = (
                logging.WARNING
                if code in (401, 403)
                else (logging.DEBUG if code and code < 500 else logging.ERROR)
            )
            safe_log(
                severity,
                func.__name__,
                "rejected" if code else "failed",
                (time.perf_counter() - started) * 1000,
                code,
            )

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                started = time.perf_counter()
                safe_log(logging.DEBUG, func.__name__, "started")
                try:
                    result = await func(*args, **kwargs)
                except Exception as exc:
                    failure(exc, started)
                    raise
                safe_log(
                    level,
                    func.__name__,
                    "returned",
                    (time.perf_counter() - started) * 1000,
                )
                return result

        else:

            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                started = time.perf_counter()
                safe_log(logging.DEBUG, func.__name__, "started")
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    failure(exc, started)
                    raise
                safe_log(
                    level,
                    func.__name__,
                    "returned",
                    (time.perf_counter() - started) * 1000,
                )
                return result

        wrapped.__signature__ = inspect.signature(func)
        return wrapped

    return decorate(fn) if fn is not None else decorate


def _env_bool(name, default="0"):
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes"}


def _env_positive_int(name, default):
    try:
        value = int(os.environ.get(name, str(default)))
        if value <= 0:
            raise ValueError()
        return value
    except ValueError:
        safe_log(
            logging.WARNING, "configuration", "invalid_positive_integer_using_default"
        )
        return default


BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
FRONTEND_DIR = Path(
    os.environ.get(
        "NOVEAM_FRONTEND_DIR",
        os.environ.get(
            "NOVEAGAMHEAM_FRONTEND_DIR", str(BASE_DIR.parent / "frontend-web")
        ),
    )
).resolve()
UPLOAD_DIR = Path(
    os.environ.get(
        "NOVEAM_UPLOAD_DIR",
        os.environ.get("NOVEAGAMHEAM_UPLOAD_DIR", str(BASE_DIR / "uploads")),
    )
).resolve()

# NOVEAM persistent storage configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
SUPABASE_STORAGE_BUCKET = os.environ.get("SUPABASE_STORAGE_BUCKET", "noveam-media").strip()

supabase_storage_client: Client | None = None
if SUPABASE_URL and SUPABASE_SECRET_KEY:
    supabase_storage_client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)


def upload_to_supabase(stored_name: str, data: bytes, content_type: str):
    if supabase_storage_client is None:
        raise HTTPException(503, "Supabase Storage is not configured")
    try:
        supabase_storage_client.storage.from_(SUPABASE_STORAGE_BUCKET).upload(
            path=stored_name, file=data,
            file_options={"content-type": content_type, "upsert": "false"},
        )
        return stored_name
    except Exception:
        safe_log(logging.ERROR, "supabase_upload", "failed")
        raise HTTPException(502, "Media storage is temporarily unavailable") from None


def delete_from_supabase(stored_name: str) -> bool:
    if supabase_storage_client is None:
        return False
    try:
        supabase_storage_client.storage.from_(SUPABASE_STORAGE_BUCKET).remove([stored_name])
        return True
    except Exception:
        safe_log(logging.ERROR, "supabase_delete", "retry_required")
        return False


def stored_media_response(row, *, download=False):
    """Call only after the existing route has authorized this file."""
    from fastapi.responses import Response
    headers = {"Cache-Control": "private, no-store"}
    stored_name = row["stored_name"]
    if supabase_storage_client is not None:
        try:
            data = supabase_storage_client.storage.from_(SUPABASE_STORAGE_BUCKET).download(stored_name)
        except Exception:
            # Existing local uploads must remain readable during the transition.
            safe_log(logging.WARNING, "supabase_download", "local_fallback")
        else:
            return Response(content=data, media_type=row["content_type"], headers=headers)
    path = _stored_path(stored_name)
    if not path.exists():
        raise HTTPException(404, "Stored image is missing")
    return FileResponse(path, media_type=row["content_type"], headers=headers)


LIVEKIT_URL = os.environ.get("LIVEKIT_URL", "").strip()
LIVEKIT_API_KEY = os.environ.get("LIVEKIT_API_KEY", "").strip()
LIVEKIT_API_SECRET = os.environ.get("LIVEKIT_API_SECRET", "").strip()
NOVEAM_ENV = os.environ.get("NOVEAM_ENV", "development").strip().lower()
NOVEAM_REQUIRE_HTTPS = _env_bool("NOVEAM_REQUIRE_HTTPS")
NOVEAM_TRUST_PROXY_HTTPS = _env_bool("NOVEAM_TRUST_PROXY_HTTPS")
NOVEAM_MAX_REQUEST_BYTES = _env_positive_int("NOVEAM_MAX_REQUEST_BYTES", 10485760)
NOVEAM_RATE_LIMIT_PER_MINUTE = _env_positive_int("NOVEAM_RATE_LIMIT_PER_MINUTE", 180)
MAX_UPLOAD_BYTES = 10 * 1024 * 1024
# Clinical/privileged roles require deployment-managed approval, never profile self-service.
SELF_SERVICE_ROLES = {
    "Patient",
    "Doctor",
    "Parent",
    "Sibling",
    "Guardian",
    "Friend",
    "Gym Trainer",
    "Coach/Instructor",
    "Founder",
    "Co-Founder",
    "Owner",
    "Director",
    "Management",
    "Principal",
    "Dean",
    "Head/HOD",
    "Teacher",
    "Faculty",
    "Staff",
    "Student/Learner",
    "Member",
    "Volunteer",
    "Partner/Collaborator",
    "Other",
}
_active_connection = ContextVar("noveam_postgres_connection", default=None)
_rate_buckets = OrderedDict()
_auth_attempts = OrderedDict()
_auth_attempt_lock = threading.RLock()
firestore_db = None

# 1. Roles and file policies
ROLES = [
    "Patient",
    "Doctor",
    "Parent",
    "Sibling",
    "Guardian",
    "Friend",
    "Gym Trainer",
    "Coach/Instructor",
    "Founder",
    "Co-Founder",
    "Owner",
    "Director",
    "Management",
    "Principal",
    "Dean",
    "Head/HOD",
    "Teacher",
    "Faculty",
    "Staff",
    "Student/Learner",
    "Member",
    "Volunteer",
    "Partner/Collaborator",
    "Other",
]

PERMISSIONS = {
    "Patient": {
        "profile:read",
        "profile:write",
        "care:self",
        "records:self",
        "wellness:use",
    },
    "Doctor": {
        "profile:read",
        "profile:write",
        "care:self",
        "care:clinical",
        "records:self",
        "records:clinical",
        "wellness:use",
    },
    "Parent": {
        "profile:read",
        "profile:write",
        "care:self",
        "care:support",
        "records:self",
        "wellness:use",
    },
    "Sibling": {
        "profile:read",
        "profile:write",
        "care:self",
        "care:support",
        "records:self",
        "wellness:use",
    },
    "Guardian": {
        "profile:read",
        "profile:write",
        "care:self",
        "care:support",
        "records:self",
        "wellness:use",
    },
    "Friend": {
        "profile:read",
        "profile:write",
        "care:self",
        "care:support",
        "records:self",
        "wellness:use",
    },
    "Gym Trainer": {"profile:read", "profile:write", "wellness:use", "fitness:coach"},
    "Coach/Instructor": {
        "profile:read",
        "profile:write",
        "wellness:use",
        "fitness:coach",
    },
}

DEFAULT_PERMISSIONS = {"profile:read", "profile:write", "wellness:use"}

ALLOWED_UPLOAD_TYPES = {
    "application/pdf": ".pdf",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "text/plain": ".txt",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
}

SOCIAL_IMAGE_TYPES = {"image/png", "image/jpeg", "image/webp"}

SOCIAL_VIDEO_TYPES = {"video/mp4", "video/webm", "video/quicktime"}

SOCIAL_AUDIO_TYPES = {
    "audio/mpeg",
    "audio/mp4",
    "audio/wav",
    "audio/x-wav",
    "audio/ogg",
}

SOCIAL_DOCUMENT_TYPES = {
    "application/pdf",
    "text/plain",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}

SOCIAL_POST_MEDIA_TYPES = (
    SOCIAL_IMAGE_TYPES | SOCIAL_VIDEO_TYPES | SOCIAL_AUDIO_TYPES | SOCIAL_DOCUMENT_TYPES
)


# 2. Request and response schemas
class CareEntryCreate(NoveamModel):
    patient_username: Optional[str] = Field(default=None, max_length=40)
    title: str = Field(min_length=1, max_length=160)
    details: str = Field(default="", max_length=3000)
    status: str = Field(default="Active", max_length=40)


class RecordCreate(NoveamModel):
    patient_username: Optional[str] = Field(default=None, max_length=40)
    record_type: str = Field(default="General", max_length=80)
    title: str = Field(min_length=1, max_length=160)
    details: str = Field(default="", max_length=5000)


class PostCreate(NoveamModel):
    text: str = Field(min_length=1, max_length=20000)
    category: str = Field(default="Other", max_length=60)
    visibility: str = Field(default="PUBLIC", max_length=30)
    media_file_id: Optional[int] = None
    post_type: str = Field(default="POST", max_length=20)
    media_url: Optional[str] = Field(default=None, max_length=2000)
    article_title: Optional[str] = Field(default=None, max_length=240)
    article_cover_file_id: Optional[int] = None


class PostUpdate(NoveamModel):
    text: str = Field(min_length=1, max_length=5000)
    category: str = Field(default="Other", max_length=60)
    visibility: str = Field(default="PUBLIC", max_length=30)


class CommentCreate(NoveamModel):
    text: str = Field(min_length=1, max_length=1000)


class CommunicationSpaceCreate(NoveamModel):
    name: str = Field(min_length=2, max_length=100)
    description: str = Field(default="", max_length=1000)
    privacy: str = Field(default="private", max_length=20)


class CommunicationChannelCreate(NoveamModel):
    name: str = Field(min_length=1, max_length=80)
    channel_type: str = Field(default="text", max_length=20)
    topic: str = Field(default="", max_length=300)


class CommunicationMessageCreate(NoveamModel):
    body: str = Field(min_length=1, max_length=5000)
    reply_to_id: Optional[int] = None


class CommunicationReactionCreate(NoveamModel):
    emoji: str = Field(min_length=1, max_length=16)


class CommunicationMemberAdd(NoveamModel):
    username: str = Field(min_length=1, max_length=100)


class CommunicationSignalCreate(NoveamModel):
    recipient_user_id: Optional[int] = None
    signal_type: str = Field(min_length=1, max_length=30)
    payload: dict = Field(default_factory=dict)


class CommunicationPresenceUpdate(NoveamModel):
    status: str = Field(default="online", max_length=20)


class CommunicationMeetingCreate(NoveamModel):
    title: str = Field(min_length=2, max_length=160)
    description: str = Field(default="", max_length=2000)
    scheduled_at: str = Field(default="", max_length=50)
    duration_minutes: int = Field(default=60, ge=10, le=1440)
    waiting_room: bool = True
    space_id: Optional[int] = None
    channel_id: Optional[int] = None


class CommunicationMeetingChatCreate(NoveamModel):
    body: str = Field(min_length=1, max_length=5000)


class CommunicationMeetingParticipantAction(NoveamModel):
    action: str = Field(min_length=1, max_length=30)
    user_id: Optional[int] = None


class LiveKitTokenRequest(NoveamModel):
    meeting_id: int


class CommunicationSettingsUpdate(NoveamModel):
    allow_direct_calls: str = Field(default="connections", max_length=20)
    allow_video_calls: bool = True
    incoming_call_notifications: bool = True
    message_notifications: bool = True
    read_receipts: bool = True
    typing_indicators: bool = True
    online_status: bool = True
    auto_download_media: bool = False
    default_microphone: str = Field(default="", max_length=300)
    default_camera: str = Field(default="", max_length=300)
    default_speaker: str = Field(default="", max_length=300)


class DirectCallCreate(NoveamModel):
    callee_user_id: int
    call_type: str = Field(default="audio", max_length=10)


class DirectCallAction(NoveamModel):
    action: str = Field(min_length=1, max_length=20)


class MessageEditRequest(NoveamModel):
    body: str = Field(min_length=1, max_length=5000)


class MessageReactionRequest(NoveamModel):
    emoji: str = Field(min_length=1, max_length=16)


class ConversationControlRequest(NoveamModel):
    action: str = Field(min_length=1, max_length=20)


class CommunicationReportRequest(NoveamModel):
    reason: str = Field(min_length=3, max_length=500)


class PrivateMessageCreate(NoveamModel):
    body: str = Field(min_length=1, max_length=5000)


class MeetingReactionRequest(NoveamModel):
    emoji: str = Field(min_length=1, max_length=16)


class MeetingPollCreate(NoveamModel):
    question: str = Field(min_length=2, max_length=300)
    options: list[str]


class MeetingPollVote(NoveamModel):
    option_index: int


class MeetingInviteCreate(NoveamModel):
    invitee_user_id: int


class RealtimeMessageEvent(NoveamModel):
    peer_id: int
    message_id: Optional[int] = None
    event: str = Field(default="message", max_length=30)


class OrganizationPagePayload(NoveamModel):
    name: str = Field(min_length=1, max_length=150)
    page_type: str = Field(min_length=1, max_length=100)
    category: str = Field(default="", max_length=120)
    location: str = Field(default="", max_length=160)
    about: str = Field(default="", max_length=5000)
    website: str = Field(default="", max_length=500)
    contact: str = Field(default="", max_length=500)
    services: str = Field(default="", max_length=2000)


class OrganizationRolePayload(NoveamModel):
    role: str = Field(min_length=1, max_length=100)
    person: str = Field(min_length=1, max_length=150)
    designation: str = Field(default="", max_length=150)
    contact: str = Field(default="", max_length=500)
    about: str = Field(default="", max_length=3000)


class PageMemberPayload(NoveamModel):
    username: str = Field(min_length=1, max_length=100)
    role: str = Field(default="Member", max_length=30)


class PageMemberRolePayload(NoveamModel):
    role: str = Field(min_length=1, max_length=30)


class PagePostCreate(NoveamModel):
    text: str = Field(min_length=1, max_length=5000)
    media_file_id: Optional[int] = None


class PagePostUpdate(NoveamModel):
    text: str = Field(min_length=1, max_length=5000)


class PagePostCommentCreate(NoveamModel):
    text: str = Field(min_length=1, max_length=1000)


class CommunityGroupCreate(NoveamModel):
    name: str = Field(min_length=2, max_length=100)
    description: str = Field(default="", max_length=1000)
    privacy: str = "public"


class CommunityGroupPostCreate(NoveamModel):
    text: str = Field(min_length=1, max_length=5000)


class CommunityGroupCommentCreate(NoveamModel):
    text: str = Field(min_length=1, max_length=1000)


class CommunityGroupRoleUpdate(NoveamModel):
    role: str


class EventCreate(NoveamModel):
    title: str = Field(min_length=1, max_length=160)
    description: str = ""
    start_at: str
    end_at: str = ""
    location: str = ""
    meeting_url: str = ""
    visibility: str = "public"
    host_type: str = "user"
    host_id: Optional[int] = None


class EventInvite(NoveamModel):
    username: str


class EventRSVP(NoveamModel):
    status: str


class JobCreate(NoveamModel):
    title: str = Field(min_length=2, max_length=160)
    opportunity_type: str = Field(default="Job", max_length=40)
    work_mode: str = Field(default="On-site", max_length=40)
    location: str = Field(default="", max_length=160)
    description: str = Field(default="", max_length=10000)
    skills: str = Field(default="", max_length=2000)
    compensation: str = Field(default="", max_length=200)
    application_deadline: str = Field(default="", max_length=40)
    page_id: Optional[int] = None


class JobApplicationCreate(NoveamModel):
    cover_note: str = Field(default="", max_length=5000)
    portfolio_url: str = Field(default="", max_length=1000)
    resume_file_id: Optional[int] = None


class JobApplicationStatus(NoveamModel):
    status: str = Field(min_length=1, max_length=30)


class PortfolioProjectCreate(NoveamModel):
    title: str = Field(min_length=1, max_length=180)
    category: str = Field(default="Other", max_length=80)
    description: str = Field(default="", max_length=5000)
    skills: str = Field(default="", max_length=1000)
    tools: str = Field(default="", max_length=1000)
    project_url: str = Field(default="", max_length=1000)
    repository_url: str = Field(default="", max_length=1000)
    media_file_id: Optional[int] = None
    visibility: str = "Public"


class CourseCreate(NoveamModel):
    title: str = Field(min_length=1, max_length=180)
    provider: str = Field(default="", max_length=180)
    instructor: str = Field(default="", max_length=180)
    description: str = Field(default="", max_length=4000)
    skills: str = Field(default="", max_length=1000)
    course_url: str = Field(default="", max_length=1000)
    visibility: str = "Public"


class EnrollmentProgress(NoveamModel):
    progress: int = Field(default=0, ge=0, le=100)
    status: str = "Enrolled"


class CertificationCreate(NoveamModel):
    name: str = Field(min_length=1, max_length=200)
    issuer: str = Field(default="", max_length=200)
    credential_id: str = Field(default="", max_length=200)
    credential_url: str = Field(default="", max_length=1000)
    issue_date: str = Field(default="", max_length=40)
    skills: str = Field(default="", max_length=1000)


class ExploreInterests(NoveamModel):
    interests: list[str] = Field(default_factory=list, max_length=30)


class RegisterRequest(NoveamModel):
    username: str = Field(default="", max_length=40)
    full_name: str = Field(default="", max_length=100)
    display_name: str = Field(default="", max_length=100)
    email: str = Field(default="", max_length=254)
    phone: str = Field(default="", max_length=30)
    role: str
    password: str = Field(min_length=8, max_length=128)

# NOVEAM Step 5.5B — Analytics event validation

class NoveamAnalyticsEvent(NoveamModel):
    type: str = Field(pattern="^(pageview)$")
    session: str = Field(
        min_length=8,
        max_length=80,
        pattern=r"^[A-Za-z0-9_-]+$"
    )
    page: str = Field(max_length=150)

    deviceType: str = Field(
        default="unknown",
        pattern="^(mobile|desktop|tablet|unknown)$"
    )
    browser: str = Field(
        default="unknown",
        pattern="^(chrome|safari|firefox|edge|samsung-internet|opera|other|unknown)$"
    )
    operatingSystem: str = Field(
        default="unknown",
        pattern="^(android|ios|windows|macos|linux|chromeos|other|unknown)$"
    )
    trafficSource: str = Field(
        default="unknown",
        pattern="^(google|bing|yahoo|duckduckgo|brave|other-search|social|referral|direct|unknown)$"
    )
class LoginRequest(NoveamModel):
    username: str = Field(default="", max_length=254)
    identifier: str = Field(default="", max_length=254)
    password: str = Field(min_length=1, max_length=128)


class ChangePasswordRequest(NoveamModel):
    current_password: str = Field(min_length=1, max_length=128)
    new_password: str = Field(min_length=8, max_length=128)


class TwoFactorCodeRequest(NoveamModel):
    code: str = Field(min_length=6, max_length=12)


class TwoFactorDisableRequest(NoveamModel):
    current_password: str = Field(min_length=1, max_length=128)
    code: str = Field(min_length=6, max_length=12)


class TwoFactorLoginRequest(NoveamModel):
    challenge: str = Field(min_length=20, max_length=200)
    code: str = Field(min_length=6, max_length=12)


class AccountContactUpdate(NoveamModel):
    email: str = Field(default="", max_length=254)
    phone: str = Field(default="", max_length=30)
    current_password: str = Field(min_length=1, max_length=128)


class DeleteAccountRequest(NoveamModel):
    current_password: str = Field(min_length=1, max_length=128)
    confirmation: str = Field(min_length=1, max_length=30)


class UserSettingsUpdate(NoveamModel):
    theme: str = Field(default="system", max_length=20)
    language: str = Field(default="en", max_length=20)
    notifications: bool = True
    profile_visibility: str = Field(default="connections", max_length=30)
    activity_status: bool = True
    connection_requests: bool = True


class ProfileUpdate(NoveamModel):
    display_name: str = Field(min_length=1, max_length=100)
    role: str
    email: str = Field(default="", max_length=254)
    phone: str = Field(default="", max_length=30)
    headline: str = Field(default="", max_length=160)
    bio: str = Field(default="", max_length=1000)
    photo: str = Field(default="", max_length=1000)
    cover: str = Field(default="", max_length=1000)
    pronouns: str = Field(default="", max_length=80)
    location: str = Field(default="", max_length=100)
    languages: str = Field(default="", max_length=250)
    occupation: str = Field(default="", max_length=160)
    organization: str = Field(default="", max_length=200)
    education: str = Field(default="", max_length=1000)
    skills: str = Field(default="", max_length=1000)
    interests: str = Field(default="", max_length=1000)
    website: str = Field(default="", max_length=1000)
    linkedin: str = Field(default="", max_length=1000)
    youtube: str = Field(default="", max_length=1000)
    instagram: str = Field(default="", max_length=1000)
    xlink: str = Field(default="", max_length=1000)
    achievements: str = Field(default="", max_length=2000)


class Task(NoveamModel):
    id: Optional[int] = None
    task_date: date
    time: str
    title: str
    details: str = ""
    status: str = "To-Do"
    responsible_person: str = "Self"
    comment: str = ""


class AccessRequestCreate(NoveamModel):
    patient_username: str = Field(min_length=3, max_length=40)
    scope: str = Field(default="care", pattern="^(care|records|care_and_records)$")
    permission: str = Field(default="view", pattern="^(view|edit)$")
    note: str = Field(default="", max_length=500)


# 3. Realtime transport
class NoveamRealtimeHub:

    @logged_operation
    def __init__(self):
        self.clients = {}

    @logged_operation
    async def connect(self, uid, ws):
        await ws.accept()
        self.clients.setdefault(uid, set()).add(ws)

    @logged_operation
    def disconnect(self, uid, ws):
        g = self.clients.get(uid, set())
        g.discard(ws)
        if not g:
            self.clients.pop(uid, None)

    @logged_operation
    async def send(self, uid, event):
        connections = list(self.clients.get(uid, set()))
        for ws in connections:
            try:
                await ws.send_json(event)
            except Exception:
                self.disconnect(uid, ws)

    @logged_operation
    def online(self, uid):
        is_online = bool(self.clients.get(uid))
        return is_online


realtime_hub = NoveamRealtimeHub()


# 4. Database, authentication and shared services
@logged_operation
def check_livekit_settings():
    configured = {
        "LIVEKIT_URL": bool(LIVEKIT_URL),
        "LIVEKIT_API_KEY": bool(LIVEKIT_API_KEY),
        "LIVEKIT_API_SECRET": bool(LIVEKIT_API_SECRET),
    }
    if not all(configured.values()):
        return False
    if not LIVEKIT_URL.startswith(("wss://", "ws://")):
        return False
    return True


@logged_operation
def check_security_settings():
    if NOVEAM_ENV == "production" and (not NOVEAM_REQUIRE_HTTPS):
        safe_log(logging.WARNING, "security_configuration", "https_not_enforced")
    if NOVEAM_TRUST_PROXY_HTTPS:
        safe_log(
            logging.WARNING,
            "security_configuration",
            "proxy_boundary_must_be_configured",
        )


@logged_operation
def _client_key(request: Request):
    if request.client:
        return request.client.host
    return "unknown"


@logged_operation
def check_noveam_network_configuration():
    check_security_settings()
    configured = bool(check_livekit_settings() and livekit_api)
    safe_log(logging.INFO, "livekit", "configured" if configured else "unavailable")
    return configured


@logged_operation
def permissions_for(role: str):
    return sorted(PERMISSIONS.get(role, DEFAULT_PERMISSIONS))


@logged_operation
def require_permission(user, permission: str):
    if permission not in set(permissions_for(user["role"])):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Your role does not have permission: {permission}",
        )


@contextmanager
def db():
    """Commit one outer PostgreSQL transaction; nested helpers share it."""
    existing = _active_connection.get()
    if existing is not None:
        yield existing
        return
    if not DATABASE_URL:
        raise RuntimeError("Set DATABASE_URL to the Neon PostgreSQL connection string")
    conn = None
    marker = None
    try:
        conn = psycopg.connect(
            DATABASE_URL,
            row_factory=dict_row,
            connect_timeout=15,
            application_name="noveam-backend",
            # Compatible with Neon's pooled connection endpoint.
            prepare_threshold=None,
        )
        marker = _active_connection.set(conn)
        with conn:
            yield conn
        safe_log(logging.DEBUG, "database", "transaction_completed")
    except Exception:
        safe_log(logging.ERROR, "database", "transaction_failed")
        raise
    finally:
        if marker is not None:
            _active_connection.reset(marker)
        if conn is not None:
            conn.close()


@logged_operation
def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, username TEXT NOT NULL UNIQUE, full_name TEXT NOT NULL, role TEXT NOT NULL, password_hash TEXT NOT NULL, created_at TEXT NOT NULL );
            CREATE TABLE IF NOT EXISTS profiles ( user_id INTEGER PRIMARY KEY, display_name TEXT NOT NULL, headline TEXT NOT NULL DEFAULT '', bio TEXT NOT NULL DEFAULT '', photo TEXT NOT NULL DEFAULT '', cover TEXT NOT NULL DEFAULT '', pronouns TEXT NOT NULL DEFAULT '', location TEXT NOT NULL DEFAULT '', languages TEXT NOT NULL DEFAULT '', occupation TEXT NOT NULL DEFAULT '', organization TEXT NOT NULL DEFAULT '', education TEXT NOT NULL DEFAULT '', skills TEXT NOT NULL DEFAULT '', interests TEXT NOT NULL DEFAULT '', website TEXT NOT NULL DEFAULT '', linkedin TEXT NOT NULL DEFAULT '', youtube TEXT NOT NULL DEFAULT '', instagram TEXT NOT NULL DEFAULT '', xlink TEXT NOT NULL DEFAULT '', achievements TEXT NOT NULL DEFAULT '', FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS sessions ( token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT '', FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS user_2fa ( user_id INTEGER PRIMARY KEY, secret TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS pending_2fa_logins ( challenge TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS user_settings ( user_id INTEGER PRIMARY KEY, theme TEXT NOT NULL DEFAULT 'system', language TEXT NOT NULL DEFAULT 'en', notifications INTEGER NOT NULL DEFAULT 1, profile_visibility TEXT NOT NULL DEFAULT 'connections', activity_status INTEGER NOT NULL DEFAULT 1, connection_requests INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS audit_log ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, user_id INTEGER, action TEXT NOT NULL, resource TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE
            SET NULL );
            CREATE TABLE IF NOT EXISTS tasks ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, user_id INTEGER, task_date TEXT NOT NULL, time TEXT NOT NULL, title TEXT NOT NULL, details TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'To-Do', responsible_person TEXT NOT NULL DEFAULT 'Self', comment TEXT NOT NULL DEFAULT '', FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            """)
        existing = {
            row["name"]
            for row in conn.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='profiles'").fetchall()
        }
        profile_columns = {
            "headline": "TEXT NOT NULL DEFAULT ''",
            "photo": "TEXT NOT NULL DEFAULT ''",
            "cover": "TEXT NOT NULL DEFAULT ''",
            "pronouns": "TEXT NOT NULL DEFAULT ''",
            "languages": "TEXT NOT NULL DEFAULT ''",
            "occupation": "TEXT NOT NULL DEFAULT ''",
            "organization": "TEXT NOT NULL DEFAULT ''",
            "education": "TEXT NOT NULL DEFAULT ''",
            "skills": "TEXT NOT NULL DEFAULT ''",
            "interests": "TEXT NOT NULL DEFAULT ''",
            "website": "TEXT NOT NULL DEFAULT ''",
            "linkedin": "TEXT NOT NULL DEFAULT ''",
            "youtube": "TEXT NOT NULL DEFAULT ''",
            "instagram": "TEXT NOT NULL DEFAULT ''",
            "xlink": "TEXT NOT NULL DEFAULT ''",
            "achievements": "TEXT NOT NULL DEFAULT ''",
        }
        profile_columns_added = 0
        for name, definition in profile_columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE profiles ADD COLUMN {name} {definition}")
                profile_columns_added += 1
        session_existing = {
            row["name"]
            for row in conn.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='sessions'").fetchall()
        }
        if "created_at" not in session_existing:
            conn.execute(
                "ALTER TABLE sessions ADD COLUMN created_at TEXT NOT NULL DEFAULT ''"
            )
        task_existing = {
            row["name"] for row in conn.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='tasks'").fetchall()
        }
        if "user_id" not in task_existing:
            conn.execute("ALTER TABLE tasks ADD COLUMN user_id INTEGER")


@logged_operation
def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 210000)
    return "pbkdf2_sha256$210000$%s$%s" % (
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


@logged_operation
def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, iterations, salt_b64, digest_b64 = encoded.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(digest_b64)
        actual = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt, int(iterations)
        )
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


@logged_operation
def _totp_secret() -> str:
    return base64.b32encode(os.urandom(20)).decode("ascii").rstrip("=")


@logged_operation
def _totp_code(secret: str, when: Optional[datetime] = None) -> str:
    now = when or datetime.now(timezone.utc)
    counter = int(now.timestamp()) // 30
    padded = secret + "=" * ((8 - len(secret) % 8) % 8)
    key = base64.b32decode(padded, casefold=True)
    digest = hmac.new(key, counter.to_bytes(8, "big"), hashlib.sha1).digest()
    offset = digest[-1] & 15
    value = (
        (digest[offset] & 127) << 24
        | digest[offset + 1] << 16
        | digest[offset + 2] << 8
        | digest[offset + 3]
    )
    return str(value % 1000000).zfill(6)


@logged_operation
def verify_totp(secret: str, code: str) -> bool:
    clean = "".join((c for c in str(code) if c.isdigit()))
    if len(clean) != 6:
        return False
    now = datetime.now(timezone.utc)
    return any(
        (
            hmac.compare_digest(
                _totp_code(secret, now + timedelta(seconds=30 * d)), clean
            )
            for d in (-1, 0, 1)
        )
    )


@logged_operation
def public_user(row):
    return {
        "id": row["id"],
        "username": row["username"],
        "full_name": row["full_name"],
        "role": row["role"],
        "created_at": row["created_at"],
    }


@logged_operation
def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(40)
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=7)
    with db() as conn:
        conn.execute('DELETE FROM sessions WHERE expires_at < %s', (now.isoformat(),))
        conn.execute(
            'INSERT INTO sessions(token,user_id,expires_at,created_at) VALUES(%s,%s,%s,%s)',
            (token, user_id, expires.isoformat(), now.isoformat()),
        )
    return token


@logged_operation
def current_user(authorization: Optional[str]):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authentication required")
    token = authorization[7:].strip()
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        row = conn.execute(
            'SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=%s AND s.expires_at>%s',
            (token, now),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    return (row, token)


@logged_operation
def audit(user_id, action, resource):
    with db() as conn:
        conn.execute(
            'INSERT INTO audit_log(user_id,action,resource,created_at) VALUES(%s,%s,%s,%s)',
            (user_id, action, resource, datetime.now(timezone.utc).isoformat()),
        )


@logged_operation
def step13_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS care_entries ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, patient_user_id INTEGER NOT NULL, created_by_user_id INTEGER NOT NULL, title TEXT NOT NULL, details TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'Active', created_at TEXT NOT NULL, FOREIGN KEY(patient_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(created_by_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS patient_records ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, patient_user_id INTEGER NOT NULL, created_by_user_id INTEGER NOT NULL, record_type TEXT NOT NULL DEFAULT 'General', title TEXT NOT NULL, details TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, FOREIGN KEY(patient_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(created_by_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_care_entries_patient ON care_entries(patient_user_id,id);
            CREATE INDEX IF NOT EXISTS idx_patient_records_patient ON patient_records(patient_user_id,id);
            """)


@logged_operation
def patient_for_viewer(
    viewer, patient_username: Optional[str], scope: str, permission: str = "view"
):
    with db() as conn:
        if patient_username:
            patient = conn.execute(
                "SELECT id,username,full_name,role FROM users WHERE username=%s AND role='Patient'",
                (patient_username.strip().lower(),),
            ).fetchone()
        elif viewer["role"] == "Patient":
            patient = conn.execute(
                'SELECT id,username,full_name,role FROM users WHERE id=%s',
                (viewer["id"],),
            ).fetchone()
        else:
            raise HTTPException(
                status_code=400, detail="Choose an authorized patient username."
            )
    if not patient:
        raise HTTPException(status_code=404, detail="Patient account not found.")
    require_patient_access(viewer, patient["id"], scope, permission)
    return patient


@logged_operation
def step14_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_files ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, owner_user_id INTEGER NOT NULL, patient_user_id INTEGER, record_id INTEGER, category TEXT NOT NULL DEFAULT 'personal', original_name TEXT NOT NULL, stored_name TEXT NOT NULL UNIQUE, content_type TEXT NOT NULL, size_bytes INTEGER NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(patient_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(record_id) REFERENCES patient_records(id) ON DELETE
            SET NULL );
            CREATE INDEX IF NOT EXISTS idx_uploaded_files_owner ON uploaded_files(owner_user_id,id);
            CREATE INDEX IF NOT EXISTS idx_uploaded_files_patient ON uploaded_files(patient_user_id,id);
            """)


@logged_operation
def file_row_for_access(file_id: int, viewer, write: bool = False):
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM uploaded_files WHERE id=%s', (file_id,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="File not found.")
    if row["patient_user_id"] is not None:
        require_patient_access(
            viewer, row["patient_user_id"], "records", "edit" if write else "view"
        )
    elif row["owner_user_id"] != viewer["id"]:
        raise HTTPException(
            status_code=403, detail="You do not have access to this file."
        )
    return row


@logged_operation
def step15_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS posts ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, author_user_id INTEGER NOT NULL, text TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'Other', visibility TEXT NOT NULL DEFAULT 'PUBLIC', media_file_id INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(media_file_id) REFERENCES uploaded_files(id) ON DELETE
            SET NULL );
            CREATE TABLE IF NOT EXISTS post_likes ( post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(post_id,user_id), FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS post_comments ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, post_id INTEGER NOT NULL, author_user_id INTEGER NOT NULL, text TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE, FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(id DESC);
            CREATE INDEX IF NOT EXISTS idx_comments_post ON post_comments(post_id,id);
            """)


@logged_operation
def post_visible_to(row, viewer):
    if row["author_user_id"] == viewer["id"]:
        return True
    if row["visibility"] == "PUBLIC":
        return True
    if row["visibility"] == "CONNECTIONS":
        with db() as conn:
            return are_connected(conn, viewer["id"], row["author_user_id"])
    return False


@logged_operation
def serialize_post(row, viewer):
    with db() as conn:
        profile = conn.execute(
            'SELECT display_name,photo FROM profiles WHERE user_id=%s',
            (row["author_user_id"],),
        ).fetchone()
        author = conn.execute(
            'SELECT username,full_name FROM users WHERE id=%s', (row["author_user_id"],)
        ).fetchone()
        likes = conn.execute(
            'SELECT COUNT(*) n FROM post_likes WHERE post_id=%s', (row["id"],)
        ).fetchone()["n"]
        liked = (
            conn.execute(
                'SELECT 1 FROM post_likes WHERE post_id=%s AND user_id=%s',
                (row["id"], viewer["id"]),
            ).fetchone()
            is not None
        )
        comments = conn.execute(
            'SELECT COUNT(*) n FROM post_comments WHERE post_id=%s', (row["id"],)
        ).fetchone()["n"]
    attachment = None
    if row["media_file_id"]:
        with db() as conn:
            f = conn.execute(
                'SELECT original_name,content_type,size_bytes FROM uploaded_files WHERE id=%s',
                (row["media_file_id"],),
            ).fetchone()
        if f:
            attachment = dict(f)
    return {
        **dict(row),
        "author_username": author["username"],
        "author_name": profile["display_name"] if profile else author["full_name"],
        "author_photo": profile["photo"] if profile else "",
        "like_count": likes,
        "liked_by_me": liked,
        "comment_count": comments,
        "is_owner": row["author_user_id"] == viewer["id"],
        "attachment": attachment,
    }


@logged_operation
def step16_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS follows ( follower_user_id INTEGER NOT NULL, followed_user_id INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(follower_user_id, followed_user_id), FOREIGN KEY(follower_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(followed_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS connection_requests ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, sender_user_id INTEGER NOT NULL, receiver_user_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', created_at TEXT NOT NULL, responded_at TEXT, FOREIGN KEY(sender_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(receiver_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_follows_followed ON follows(followed_user_id);
            CREATE INDEX IF NOT EXISTS idx_connections_receiver ON connection_requests(receiver_user_id,status);
            """)


@logged_operation
def are_connected(conn, a: int, b: int):
    connected = (
        conn.execute(
            """
        SELECT 1
        FROM connection_requests
        WHERE status='ACCEPTED'
        AND ( (sender_user_id=%s
        AND receiver_user_id=%s)
        OR (sender_user_id=%s
        AND receiver_user_id=%s) )
        LIMIT 1
        """,
            (a, b, b, a),
        ).fetchone()
        is not None
    )
    return connected


@logged_operation
def social_user(conn, row, viewer_id):
    p = conn.execute(
        'SELECT display_name,headline,photo FROM profiles WHERE user_id=%s', (row["id"],)
    ).fetchone()
    following = (
        conn.execute(
            'SELECT 1 FROM follows WHERE follower_user_id=%s AND followed_user_id=%s',
            (viewer_id, row["id"]),
        ).fetchone()
        is not None
    )
    follower = (
        conn.execute(
            'SELECT 1 FROM follows WHERE follower_user_id=%s AND followed_user_id=%s',
            (row["id"], viewer_id),
        ).fetchone()
        is not None
    )
    pending_out = conn.execute(
        """
        SELECT id
        FROM connection_requests
        WHERE sender_user_id=%s
        AND receiver_user_id=%s
        AND status='PENDING'
        ORDER BY id DESC
        LIMIT 1
        """,
        (viewer_id, row["id"]),
    ).fetchone()
    pending_in = conn.execute(
        """
        SELECT id
        FROM connection_requests
        WHERE sender_user_id=%s
        AND receiver_user_id=%s
        AND status='PENDING'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["id"], viewer_id),
    ).fetchone()
    result = {
        "id": row["id"],
        "username": row["username"],
        "name": p["display_name"] if p else row["full_name"],
        "headline": p["headline"] if p else "",
        "photo": p["photo"] if p else "",
        "role": row["role"],
        "following": following,
        "follows_me": follower,
        "connected": are_connected(conn, viewer_id, row["id"]),
        "pending_out_id": pending_out["id"] if pending_out else None,
        "pending_in_id": pending_in["id"] if pending_in else None,
    }
    return result


@logged_operation
def step17_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS private_messages ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, sender_user_id INTEGER NOT NULL, receiver_user_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL, read_at TEXT, deleted_by_sender INTEGER NOT NULL DEFAULT 0, deleted_by_receiver INTEGER NOT NULL DEFAULT 0, FOREIGN KEY(sender_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(receiver_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_private_messages_pair ON private_messages(sender_user_id,receiver_user_id,created_at);
            CREATE INDEX IF NOT EXISTS idx_private_messages_receiver_read ON private_messages(receiver_user_id,read_at);
            """)


@logged_operation
def communication_phase1_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS communication_spaces ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, owner_user_id INTEGER NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', privacy TEXT NOT NULL DEFAULT 'private', created_at TEXT NOT NULL, FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_space_members ( space_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'member', joined_at TEXT NOT NULL, PRIMARY KEY(space_id,user_id), FOREIGN KEY(space_id) REFERENCES communication_spaces(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_channels ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, space_id INTEGER NOT NULL, name TEXT NOT NULL, channel_type TEXT NOT NULL DEFAULT 'text', topic TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, FOREIGN KEY(space_id) REFERENCES communication_spaces(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_channel_messages ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, channel_id INTEGER NOT NULL, author_user_id INTEGER NOT NULL, body TEXT NOT NULL, reply_to_id INTEGER, created_at TEXT NOT NULL, edited_at TEXT, FOREIGN KEY(channel_id) REFERENCES communication_channels(id) ON DELETE CASCADE, FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(reply_to_id) REFERENCES communication_channel_messages(id) ON DELETE
            SET NULL );
            CREATE TABLE IF NOT EXISTS communication_message_reactions ( message_id INTEGER NOT NULL, user_id INTEGER NOT NULL, emoji TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(message_id,user_id,emoji), FOREIGN KEY(message_id) REFERENCES communication_channel_messages(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_comm_channels_space ON communication_channels(space_id,id);
            CREATE INDEX IF NOT EXISTS idx_comm_messages_channel ON communication_channel_messages(channel_id,id);
            CREATE TABLE IF NOT EXISTS communication_presence ( user_id INTEGER PRIMARY KEY, status TEXT NOT NULL DEFAULT 'online', last_seen TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_call_signals ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, channel_id INTEGER NOT NULL, sender_user_id INTEGER NOT NULL, recipient_user_id INTEGER, signal_type TEXT NOT NULL, payload TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, FOREIGN KEY(channel_id) REFERENCES communication_channels(id) ON DELETE CASCADE, FOREIGN KEY(sender_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(recipient_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_comm_signals_channel_id ON communication_call_signals(channel_id,id);
            CREATE TABLE IF NOT EXISTS communication_meetings ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, host_user_id INTEGER NOT NULL, space_id INTEGER, channel_id INTEGER, title TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', meeting_code TEXT NOT NULL UNIQUE, scheduled_at TEXT NOT NULL DEFAULT '', duration_minutes INTEGER NOT NULL DEFAULT 60, waiting_room INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'scheduled', created_at TEXT NOT NULL, ended_at TEXT, FOREIGN KEY(host_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(space_id) REFERENCES communication_spaces(id) ON DELETE
            SET NULL, FOREIGN KEY(channel_id) REFERENCES communication_channels(id) ON DELETE
            SET NULL );
            CREATE TABLE IF NOT EXISTS communication_meeting_participants ( meeting_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'participant', state TEXT NOT NULL DEFAULT 'waiting', hand_raised INTEGER NOT NULL DEFAULT 0, microphone_enabled INTEGER NOT NULL DEFAULT 1, camera_enabled INTEGER NOT NULL DEFAULT 0, joined_at TEXT NOT NULL, admitted_at TEXT, left_at TEXT, PRIMARY KEY(meeting_id,user_id), FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_meeting_chat ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, meeting_id INTEGER NOT NULL, author_user_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE, FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_comm_meetings_host ON communication_meetings(host_user_id,id DESC);
            CREATE INDEX IF NOT EXISTS idx_comm_meeting_chat ON communication_meeting_chat(meeting_id,id);
            CREATE TABLE IF NOT EXISTS communication_meeting_signals ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, meeting_id INTEGER NOT NULL, sender_user_id INTEGER NOT NULL, recipient_user_id INTEGER, signal_type TEXT NOT NULL, payload TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE, FOREIGN KEY(sender_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(recipient_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_comm_meeting_signals ON communication_meeting_signals(meeting_id,id);
            CREATE TABLE IF NOT EXISTS communication_settings ( user_id INTEGER PRIMARY KEY, allow_direct_calls TEXT NOT NULL DEFAULT 'connections', allow_video_calls INTEGER NOT NULL DEFAULT 1, incoming_call_notifications INTEGER NOT NULL DEFAULT 1, message_notifications INTEGER NOT NULL DEFAULT 1, read_receipts INTEGER NOT NULL DEFAULT 1, typing_indicators INTEGER NOT NULL DEFAULT 1, online_status INTEGER NOT NULL DEFAULT 1, auto_download_media INTEGER NOT NULL DEFAULT 0, default_microphone TEXT NOT NULL DEFAULT '', default_camera TEXT NOT NULL DEFAULT '', default_speaker TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_direct_calls ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, caller_user_id INTEGER NOT NULL, callee_user_id INTEGER NOT NULL, call_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ringing', meeting_id INTEGER, created_at TEXT NOT NULL, answered_at TEXT, ended_at TEXT, FOREIGN KEY(caller_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(callee_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE
            SET NULL );
            CREATE INDEX IF NOT EXISTS idx_comm_direct_calls_users ON communication_direct_calls(caller_user_id,callee_user_id,id DESC);
            CREATE TABLE IF NOT EXISTS private_message_reactions ( message_id INTEGER NOT NULL, user_id INTEGER NOT NULL, emoji TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(message_id,user_id,emoji), FOREIGN KEY(message_id) REFERENCES private_messages(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS private_message_saved ( message_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(message_id,user_id), FOREIGN KEY(message_id) REFERENCES private_messages(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS conversation_controls ( user_id INTEGER NOT NULL, peer_user_id INTEGER NOT NULL, muted INTEGER NOT NULL DEFAULT 0, blocked INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL, PRIMARY KEY(user_id,peer_user_id), FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(peer_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_reports ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, reporter_user_id INTEGER NOT NULL, reported_user_id INTEGER NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(reporter_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(reported_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_meeting_reactions ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, meeting_id INTEGER NOT NULL, user_id INTEGER NOT NULL, emoji TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_meeting_polls ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, meeting_id INTEGER NOT NULL, creator_user_id INTEGER NOT NULL, question TEXT NOT NULL, options_json TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE, FOREIGN KEY(creator_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_meeting_poll_votes ( poll_id INTEGER NOT NULL, user_id INTEGER NOT NULL, option_index INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(poll_id,user_id), FOREIGN KEY(poll_id) REFERENCES communication_meeting_polls(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS communication_meeting_invites ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, meeting_id INTEGER NOT NULL, inviter_user_id INTEGER NOT NULL, invitee_user_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL, FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE, FOREIGN KEY(inviter_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(invitee_user_id) REFERENCES users(id) ON DELETE CASCADE );
            """)


@logged_operation
def message_user(conn, user_id: int):
    row = conn.execute('SELECT * FROM users WHERE id=%s', (user_id,)).fetchone()
    if not row:
        return None
    p = conn.execute(
        'SELECT display_name,photo,headline FROM profiles WHERE user_id=%s', (user_id,)
    ).fetchone()
    return {
        "id": row["id"],
        "username": row["username"],
        "name": p["display_name"] if p and p["display_name"] else row["full_name"],
        "photo": p["photo"] if p else "",
        "headline": p["headline"] if p else "",
        "role": row["role"],
    }


@logged_operation
def _space_role(conn, space_id: int, user_id: int):
    r = conn.execute(
        'SELECT role FROM communication_space_members WHERE space_id=%s AND user_id=%s',
        (space_id, user_id),
    ).fetchone()
    return r["role"] if r else None


@logged_operation
def _meeting_access(conn, meeting_id: int, user_id: int):
    m = conn.execute(
        'SELECT * FROM communication_meetings WHERE id=%s', (meeting_id,)
    ).fetchone()
    if not m:
        raise HTTPException(404, "Meeting not found")
    p = conn.execute(
        'SELECT * FROM communication_meeting_participants WHERE meeting_id=%s AND user_id=%s',
        (meeting_id, user_id),
    ).fetchone()
    return (m, p)


@logged_operation
def _meeting_hostish(conn, meeting_id: int, user_id: int):
    m, p = _meeting_access(conn, meeting_id, user_id)
    return (
        m,
        p,
        m["host_user_id"] == user_id
        or (p and p["state"] == "joined" and (p["role"] in ("host", "cohost"))),
    )


@logged_operation
def _communication_settings_row(conn, user_id: int):
    row = conn.execute(
        'SELECT * FROM communication_settings WHERE user_id=%s', (user_id,)
    ).fetchone()
    if row:
        return row
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        'INSERT INTO communication_settings(user_id,updated_at) VALUES(%s,%s) ON CONFLICT(user_id) DO NOTHING',
        (user_id, now),
    )
    return conn.execute(
        'SELECT * FROM communication_settings WHERE user_id=%s', (user_id,)
    ).fetchone()


@logged_operation
def _private_message_access(conn, message_id: int, user_id: int):
    row = conn.execute(
        'SELECT * FROM private_messages WHERE id=%s', (message_id,)
    ).fetchone()
    if (
        not row
        or (row["sender_user_id"] == user_id and row["deleted_by_sender"])
        or (row["receiver_user_id"] == user_id and row["deleted_by_receiver"])
        or (user_id not in (row["sender_user_id"], row["receiver_user_id"]))
    ):
        raise HTTPException(404, "Message not found")
    return row


@logged_operation
def _meeting_member(conn, meeting_id: int, user_id: int):
    row = conn.execute(
        """
        SELECT p.*
        FROM communication_meeting_participants p
        JOIN communication_meetings m ON m.id=p.meeting_id
        WHERE p.meeting_id=%s
        AND p.user_id=%s
        AND p.state='joined'
        AND m.status<>'ended'
        """,
        (meeting_id, user_id),
    ).fetchone()
    if not row:
        raise HTTPException(403, "Admitted meeting access required")
    return row


@logged_operation
def step18_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, user_id INTEGER NOT NULL, actor_user_id INTEGER, type TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '', target_type TEXT NOT NULL DEFAULT '', target_id INTEGER, created_at TEXT NOT NULL, read_at TEXT, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(actor_user_id) REFERENCES users(id) ON DELETE
            SET NULL );
            CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id,id DESC);
            CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(user_id,read_at);
            """)


@logged_operation
def create_notification(
    conn,
    user_id: int,
    actor_user_id: Optional[int],
    ntype: str,
    title: str,
    body: str = "",
    target_type: str = "",
    target_id: Optional[int] = None,
):
    if ntype in {"NEW_MESSAGE", "INCOMING_CALL"}:
        settings = _communication_settings_row(conn, user_id)
        if not settings[
            (
                "message_notifications"
                if ntype == "NEW_MESSAGE"
                else "incoming_call_notifications"
            )
        ]:
            return
        if (
            actor_user_id is not None
            and conn.execute(
                'SELECT 1 FROM conversation_controls WHERE user_id=%s AND peer_user_id=%s AND muted=1',
                (user_id, actor_user_id),
            ).fetchone()
        ):
            return
    if actor_user_id is not None and user_id == actor_user_id:
        return
    pref = conn.execute(
        'SELECT notifications FROM user_settings WHERE user_id=%s', (user_id,)
    ).fetchone()
    if pref is not None and (not bool(pref["notifications"])):
        return
    notification = conn.execute(
        """
        INSERT INTO notifications( user_id,actor_user_id,type,title,body, target_type,target_id,created_at )
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
        """,
        (
            user_id,
            actor_user_id,
            ntype,
            title,
            body,
            target_type,
            target_id,
            datetime.now(timezone.utc).isoformat(),
        ),
    ).fetchone()
    enqueue_push(conn, notification["id"], user_id)


@logged_operation
def notification_actor(conn, actor_id):
    if actor_id is None:
        return None
    u = conn.execute(
        'SELECT id,username,full_name,role FROM users WHERE id=%s', (actor_id,)
    ).fetchone()
    if not u:
        return None
    p = conn.execute(
        'SELECT display_name,photo FROM profiles WHERE user_id=%s', (actor_id,)
    ).fetchone()
    return {
        "id": u["id"],
        "username": u["username"],
        "name": p["display_name"] if p and p["display_name"] else u["full_name"],
        "photo": p["photo"] if p else "",
        "role": u["role"],
    }


@logged_operation
def step19_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS organization_pages ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, owner_user_id INTEGER NOT NULL, name TEXT NOT NULL, page_type TEXT NOT NULL, category TEXT NOT NULL DEFAULT '', location TEXT NOT NULL DEFAULT '', about TEXT NOT NULL DEFAULT '', website TEXT NOT NULL DEFAULT '', contact TEXT NOT NULL DEFAULT '', services TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS organization_page_roles ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, page_id INTEGER NOT NULL, created_by_user_id INTEGER NOT NULL, role TEXT NOT NULL, person TEXT NOT NULL, designation TEXT NOT NULL DEFAULT '', contact TEXT NOT NULL DEFAULT '', about TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE CASCADE, FOREIGN KEY(created_by_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_org_pages_owner ON organization_pages(owner_user_id,id DESC);
            CREATE INDEX IF NOT EXISTS idx_org_pages_type ON organization_pages(page_type);
            CREATE INDEX IF NOT EXISTS idx_org_roles_page ON organization_page_roles(page_id,id DESC);
            """)


@logged_operation
def _table_exists(conn, name: str):
    return bool(
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_type='BASE TABLE' AND table_name=%s", (name,)
        ).fetchone()
    )


@logged_operation
def serialize_org_page(row, viewer_id: int):
    d = dict(row)
    d["is_owner"] = row["owner_user_id"] == viewer_id
    with db() as conn:
        owner = conn.execute(
            'SELECT username,full_name FROM users WHERE id=%s', (row["owner_user_id"],)
        ).fetchone()
        d["owner_username"] = owner["username"] if owner else ""
        d["owner_name"] = owner["full_name"] if owner else ""
        d["role_count"] = conn.execute(
            'SELECT COUNT(*) n FROM organization_page_roles WHERE page_id=%s',
            (row["id"],),
        ).fetchone()["n"]
        d["follower_count"] = (
            conn.execute(
                'SELECT COUNT(*) n FROM organization_page_followers WHERE page_id=%s',
                (row["id"],),
            ).fetchone()["n"]
            if _table_exists(conn, "organization_page_followers")
            else 0
        )
        m = (
            conn.execute(
                'SELECT role FROM organization_page_members WHERE page_id=%s AND user_id=%s',
                (row["id"], viewer_id),
            ).fetchone()
            if _table_exists(conn, "organization_page_members")
            else None
        )
        d["member_role"] = m["role"] if m else "Owner" if d["is_owner"] else ""
        d["is_member"] = bool(m) or d["is_owner"]
        d["is_following"] = (
            bool(
                conn.execute(
                    'SELECT 1 FROM organization_page_followers WHERE page_id=%s AND user_id=%s',
                    (row["id"], viewer_id),
                ).fetchone()
            )
            if _table_exists(conn, "organization_page_followers")
            else False
        )
        d["can_manage_members"] = d["is_owner"] or d["member_role"] == "Admin"
        d["can_edit"] = d["is_owner"] or d["member_role"] in ("Admin", "Editor")
    return d


@logged_operation
def step20_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS organization_page_followers ( page_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(page_id,user_id), FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS organization_page_members ( page_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'Member', added_by_user_id INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(page_id,user_id), FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(added_by_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_page_followers_user ON organization_page_followers(user_id,page_id);
            CREATE INDEX IF NOT EXISTS idx_page_members_user ON organization_page_members(user_id,page_id);
            """)


@logged_operation
def _page_and_permission(conn, page_id, user_id):
    page = conn.execute(
        'SELECT * FROM organization_pages WHERE id=%s', (page_id,)
    ).fetchone()
    if not page:
        raise HTTPException(404, "Page not found")
    member = conn.execute(
        'SELECT role FROM organization_page_members WHERE page_id=%s AND user_id=%s',
        (page_id, user_id),
    ).fetchone()
    role = (
        "Owner"
        if page["owner_user_id"] == user_id
        else member["role"] if member else ""
    )
    return (page, role)


@logged_operation
def step21_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS page_posts ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, page_id INTEGER NOT NULL, author_user_id INTEGER NOT NULL, text TEXT NOT NULL, media_file_id INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE CASCADE, FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(media_file_id) REFERENCES uploaded_files(id) ON DELETE
            SET NULL );
            CREATE TABLE IF NOT EXISTS page_post_likes ( post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(post_id,user_id), FOREIGN KEY(post_id) REFERENCES page_posts(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS page_post_comments ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, post_id INTEGER NOT NULL, author_user_id INTEGER NOT NULL, text TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(post_id) REFERENCES page_posts(id) ON DELETE CASCADE, FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_page_posts_page ON page_posts(page_id,id DESC);
            CREATE INDEX IF NOT EXISTS idx_page_posts_created ON page_posts(id DESC);
            CREATE INDEX IF NOT EXISTS idx_page_post_comments ON page_post_comments(post_id,id);
            """)


@logged_operation
def _page_post_permission(conn, page_id, user_id):
    page, role = _page_and_permission(conn, page_id, user_id)
    can_post = role in ("Owner", "Admin", "Editor")
    return (page, role, can_post)


@logged_operation
def serialize_page_post(row, viewer_id):
    with db() as conn:
        page = conn.execute(
            'SELECT id,name,page_type FROM organization_pages WHERE id=%s',
            (row["page_id"],),
        ).fetchone()
        author = conn.execute(
            'SELECT username,full_name FROM users WHERE id=%s', (row["author_user_id"],)
        ).fetchone()
        _, role, can_post = _page_post_permission(conn, row["page_id"], viewer_id)
        likes = conn.execute(
            'SELECT COUNT(*) n FROM page_post_likes WHERE post_id=%s', (row["id"],)
        ).fetchone()["n"]
        liked = bool(
            conn.execute(
                'SELECT 1 FROM page_post_likes WHERE post_id=%s AND user_id=%s',
                (row["id"], viewer_id),
            ).fetchone()
        )
        comments = conn.execute(
            'SELECT COUNT(*) n FROM page_post_comments WHERE post_id=%s', (row["id"],)
        ).fetchone()["n"]
        return {
            **dict(row),
            "page_name": page["name"],
            "page_type": page["page_type"],
            "posted_by_username": author["username"] if author else "",
            "like_count": likes,
            "liked_by_me": liked,
            "comment_count": comments,
            "can_manage": can_post,
        }


@logged_operation
def init_groups_step22():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS community_groups ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, owner_user_id INTEGER NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', privacy TEXT NOT NULL DEFAULT 'public', created_at TEXT NOT NULL, FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS community_group_members ( group_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'member', status TEXT NOT NULL DEFAULT 'active', joined_at TEXT NOT NULL, PRIMARY KEY(group_id,user_id), FOREIGN KEY(group_id) REFERENCES community_groups(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS community_group_join_requests ( group_id INTEGER NOT NULL, user_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL, PRIMARY KEY(group_id,user_id), FOREIGN KEY(group_id) REFERENCES community_groups(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS community_group_posts ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, group_id INTEGER NOT NULL, author_user_id INTEGER NOT NULL, text TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY(group_id) REFERENCES community_groups(id) ON DELETE CASCADE, FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE TABLE IF NOT EXISTS community_group_comments ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, post_id INTEGER NOT NULL, author_user_id INTEGER NOT NULL, text TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(post_id) REFERENCES community_group_posts(id) ON DELETE CASCADE, FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_group_posts_group ON community_group_posts(group_id,id DESC);
            """)


@logged_operation
def _group_role(conn, gid, uid):
    r = conn.execute(
        "SELECT role FROM community_group_members WHERE group_id=%s AND user_id=%s AND status='active'",
        (gid, uid),
    ).fetchone()
    return r["role"] if r else None


@logged_operation
def _group_access(conn, gid, uid):
    g = conn.execute('SELECT * FROM community_groups WHERE id=%s', (gid,)).fetchone()
    if not g:
        raise HTTPException(404, "Group not found")
    role = _group_role(conn, gid, uid)
    return (g, role)


@logged_operation
def _group_json(conn, g, uid):
    role = _group_role(conn, g["id"], uid)
    req = conn.execute(
        'SELECT status FROM community_group_join_requests WHERE group_id=%s AND user_id=%s',
        (g["id"], uid),
    ).fetchone()
    count = conn.execute(
        "SELECT COUNT(*) n FROM community_group_members WHERE group_id=%s AND status='active'",
        (g["id"],),
    ).fetchone()["n"]
    return {
        "id": g["id"],
        "name": g["name"],
        "description": g["description"],
        "privacy": g["privacy"],
        "owner_user_id": g["owner_user_id"],
        "created_at": g["created_at"],
        "member_count": count,
        "my_role": role,
        "join_status": req["status"] if req else None,
        "can_manage": role in ("owner", "admin", "moderator"),
    }


@logged_operation
def init_events_step23():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, creator_user_id INTEGER NOT NULL, host_type TEXT NOT NULL DEFAULT 'user', host_id INTEGER, title TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', start_at TEXT NOT NULL, end_at TEXT NOT NULL DEFAULT '', location TEXT NOT NULL DEFAULT '', meeting_url TEXT NOT NULL DEFAULT '', visibility TEXT NOT NULL DEFAULT 'public', created_at TEXT NOT NULL, FOREIGN KEY(creator_user_id) REFERENCES users(id) ON DELETE CASCADE);
            CREATE TABLE IF NOT EXISTS event_attendees ( event_id INTEGER NOT NULL, user_id INTEGER NOT NULL, rsvp TEXT NOT NULL DEFAULT 'invited', invited_by INTEGER, updated_at TEXT NOT NULL, PRIMARY KEY(event_id,user_id), FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);
            """)


@logged_operation
def _event_json(conn, e, uid):
    mine = e["creator_user_id"] == uid
    a = conn.execute(
        'SELECT rsvp FROM event_attendees WHERE event_id=%s AND user_id=%s',
        (e["id"], uid),
    ).fetchone()
    counts = {
        r["rsvp"]: r["n"]
        for r in conn.execute(
            'SELECT rsvp,COUNT(*) n FROM event_attendees WHERE event_id=%s GROUP BY rsvp',
            (e["id"],),
        ).fetchall()
    }
    d = dict(e)
    d.update(
        {"is_creator": mine, "my_rsvp": a["rsvp"] if a else None, "rsvp_counts": counts}
    )
    return d


@logged_operation
def init_step25_jobs():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, creator_user_id INTEGER NOT NULL, page_id INTEGER, title TEXT NOT NULL, opportunity_type TEXT NOT NULL DEFAULT 'Job', work_mode TEXT NOT NULL DEFAULT 'On-site', location TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', skills TEXT NOT NULL DEFAULT '', compensation TEXT NOT NULL DEFAULT '', application_deadline TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'open', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY(creator_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE
            SET NULL);
            CREATE TABLE IF NOT EXISTS job_applications ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, job_id INTEGER NOT NULL, applicant_user_id INTEGER NOT NULL, cover_note TEXT NOT NULL DEFAULT '', portfolio_url TEXT NOT NULL DEFAULT '', resume_file_id INTEGER, status TEXT NOT NULL DEFAULT 'submitted', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(job_id,applicant_user_id), FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE, FOREIGN KEY(applicant_user_id) REFERENCES users(id) ON DELETE CASCADE);
            CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status,created_at);
            CREATE INDEX IF NOT EXISTS idx_job_apps_job ON job_applications(job_id,status);
            """)


@logged_operation
def _job_manage_ok(conn, j, user_id):
    if j["creator_user_id"] == user_id:
        return True
    if j["page_id"]:
        _, role = _page_and_permission(conn, j["page_id"], user_id)
        return role in ("Owner", "Admin", "Editor")
    return False


@logged_operation
def _job_json(conn, j, uid):
    d = dict(j)
    page = (
        conn.execute(
            'SELECT name FROM organization_pages WHERE id=%s', (j["page_id"],)
        ).fetchone()
        if j["page_id"]
        else None
    )
    creator = conn.execute(
        'SELECT username,full_name FROM users WHERE id=%s', (j["creator_user_id"],)
    ).fetchone()
    app = conn.execute(
        'SELECT id,status,created_at FROM job_applications WHERE job_id=%s AND applicant_user_id=%s',
        (j["id"], uid),
    ).fetchone()
    d.update(
        {
            "page_name": page["name"] if page else "",
            "creator_name": creator["full_name"] if creator else "",
            "creator_username": creator["username"] if creator else "",
            "can_manage": _job_manage_ok(conn, j, uid),
            "my_application": dict(app) if app else None,
        }
    )
    return d


@logged_operation
def init_step26():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_projects ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'Other', description TEXT NOT NULL DEFAULT '', skills TEXT NOT NULL DEFAULT '', tools TEXT NOT NULL DEFAULT '', project_url TEXT NOT NULL DEFAULT '', repository_url TEXT NOT NULL DEFAULT '', media_file_id INTEGER, visibility TEXT NOT NULL DEFAULT 'Public', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(media_file_id) REFERENCES uploaded_files(id) ON DELETE
            SET NULL );
            CREATE INDEX IF NOT EXISTS idx_portfolio_user ON portfolio_projects(user_id,id DESC);
            """)


@logged_operation
def _portfolio_json(conn, row, viewer_id):
    d = dict(row)
    u = conn.execute(
        'SELECT username,full_name FROM users WHERE id=%s', (row["user_id"],)
    ).fetchone()
    p = conn.execute(
        'SELECT headline,photo FROM profiles WHERE user_id=%s', (row["user_id"],)
    ).fetchone()
    d.update(
        {
            "username": u["username"] if u else "",
            "full_name": u["full_name"] if u else "",
            "headline": p["headline"] if p else "",
            "profile_photo": p["photo"] if p else "",
            "can_manage": row["user_id"] == viewer_id,
        }
    )
    return d


@logged_operation
def init_step27():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS learning_courses(id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,creator_user_id INTEGER NOT NULL,title TEXT NOT NULL,provider TEXT NOT NULL DEFAULT '',instructor TEXT NOT NULL DEFAULT '',description TEXT NOT NULL DEFAULT '',skills TEXT NOT NULL DEFAULT '',course_url TEXT NOT NULL DEFAULT '',visibility TEXT NOT NULL DEFAULT 'Public',created_at TEXT NOT NULL,FOREIGN KEY(creator_user_id) REFERENCES users(id) ON DELETE CASCADE);
            CREATE TABLE IF NOT EXISTS course_enrollments(id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,course_id INTEGER NOT NULL,user_id INTEGER NOT NULL,progress INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'Enrolled',created_at TEXT NOT NULL,UNIQUE(course_id,user_id),FOREIGN KEY(course_id) REFERENCES learning_courses(id) ON DELETE CASCADE,FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);
            CREATE TABLE IF NOT EXISTS certifications(id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,user_id INTEGER NOT NULL,name TEXT NOT NULL,issuer TEXT NOT NULL DEFAULT '',credential_id TEXT NOT NULL DEFAULT '',credential_url TEXT NOT NULL DEFAULT '',issue_date TEXT NOT NULL DEFAULT '',skills TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);
            """)


@logged_operation
def init_step27_registration_schema():
    with db() as conn:
        cols = {r["name"] for r in conn.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='users'").fetchall()}
        if "email" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT ''")
        if "phone" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN phone TEXT NOT NULL DEFAULT ''")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_nonempty ON users(email) WHERE email<>''"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_phone_nonempty ON users(phone) WHERE phone<>''"
        )


@logged_operation
def init_step28():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_interests( user_id INTEGER NOT NULL, interest TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(user_id,interest), FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_user_interests_interest ON user_interests(interest);
            """)


@logged_operation
def step12_init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS care_access_requests ( id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, patient_user_id INTEGER NOT NULL, requester_user_id INTEGER NOT NULL, requester_role TEXT NOT NULL, scope TEXT NOT NULL, permission TEXT NOT NULL DEFAULT 'view', note TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL, decided_at TEXT, revoked_at TEXT, FOREIGN KEY(patient_user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(requester_user_id) REFERENCES users(id) ON DELETE CASCADE );
            CREATE INDEX IF NOT EXISTS idx_care_access_patient ON care_access_requests(patient_user_id,status);
            CREATE INDEX IF NOT EXISTS idx_care_access_requester ON care_access_requests(requester_user_id,status);
            """)


@logged_operation
def access_row_dict(row):
    return dict(row) if row else None


@logged_operation
def scope_allows(granted_scope: str, required_scope: str) -> bool:
    return granted_scope == required_scope or granted_scope == "care_and_records"


@logged_operation
def require_patient_access(
    viewer, patient_user_id: int, required_scope: str, required_permission: str = "view"
):
    if viewer["id"] == patient_user_id:
        return
    with db() as conn:
        grants = conn.execute(
            """
            SELECT *
            FROM care_access_requests
            WHERE patient_user_id=%s
            AND requester_user_id=%s
            AND status='approved'
            """,
            (patient_user_id, viewer["id"]),
        ).fetchall()
    for grant in grants:
        if scope_allows(grant["scope"], required_scope):
            if required_permission == "view" or grant["permission"] == "edit":
                return
    raise HTTPException(
        status_code=403, detail="The patient has not granted the required access."
    )


@logged_operation
def patient_decide_request(
    request_id: int, decision: str, authorization: Optional[str]
):
    patient, _ = current_user(authorization)
    if patient["role"] != "Patient":
        raise HTTPException(
            status_code=403, detail="Only the patient can approve or deny this request."
        )
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM care_access_requests WHERE id=%s AND patient_user_id=%s',
            (request_id, patient["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Access request not found.")
        if row["status"] != "pending":
            raise HTTPException(
                status_code=409, detail="This request has already been decided."
            )
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            'UPDATE care_access_requests SET status=%s,decided_at=%s WHERE id=%s',
            (decision, now, request_id),
        )
        updated = conn.execute(
            'SELECT * FROM care_access_requests WHERE id=%s', (request_id,)
        ).fetchone()
    audit(patient["id"], decision, f"care_access_request:{request_id}")
    return access_row_dict(updated)


@logged_operation
def require_user(authorization):
    return current_user(authorization)[0]


@logged_operation
def _check_auth_attempt(key):
    with _auth_attempt_lock:
        "Bounded, process-local defense; shared deployments require a shared limiter."
        now = time.monotonic()
        stale = [k for k, v in _auth_attempts.items() if now - v[-1] > 300]
        for k in stale:
            _auth_attempts.pop(k, None)
        bucket = _auth_attempts.setdefault(key, deque())
        while bucket and bucket[0] < now - 300:
            bucket.popleft()
        if len(bucket) >= 10:
            raise HTTPException(
                429, "Too many authentication attempts; try again later"
            )
        bucket.append(now)
        _auth_attempts.move_to_end(key)
        while len(_auth_attempts) > 10000:
            _auth_attempts.popitem(last=False)


@logged_operation
def _not_blocked(conn, a, b):
    return not conn.execute(
        """
        SELECT 1
        FROM conversation_controls
        WHERE blocked=1
        AND ((user_id=%s
        AND peer_user_id=%s)
        OR (user_id=%s
        AND peer_user_id=%s))
        """,
        (a, b, b, a),
    ).fetchone()


@logged_operation
def _require_unblocked(conn, a, b):
    if not _not_blocked(conn, a, b):
        raise HTTPException(403, "Communication is blocked")


@logged_operation
def _valid_owned_file(conn, file_id, user_id):
    if file_id is not None and (
        not conn.execute(
            'SELECT 1 FROM uploaded_files WHERE id=%s AND owner_user_id=%s',
            (file_id, user_id),
        ).fetchone()
    ):
        raise HTTPException(400, "Invalid file attachment")


@logged_operation
def _stored_path(stored_name):
    path = (UPLOAD_DIR / stored_name).resolve()
    if path.parent != UPLOAD_DIR:
        raise HTTPException(400, "Invalid stored file path")
    return path


@logged_operation
def _validate_upload(data, content_type):
    """Reject obvious mislabeled content; this is not antivirus or a full file parser."""
    checks = {
        "application/pdf": lambda b: b.startswith(b"%PDF-"),
        "image/png": lambda b: b.startswith(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg": lambda b: b.startswith(b"\xff\xd8\xff"),
        "image/webp": lambda b: b.startswith(b"RIFF") and b[8:12] == b"WEBP",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": lambda b: b.startswith(
            b"PK\x03\x04"
        ),
        "application/msword": lambda b: b.startswith(bytes.fromhex("d0cf11e0a1b11ae1")),
        "text/plain": lambda b: b"\x00" not in b,
    }
    check = checks.get(content_type)
    if check and (not check(data)):
        raise HTTPException(415, "File content does not match its declared type")


@logged_operation
def initialize_firebase():
    global firestore_db
    project = os.environ.get("FIREBASE_PROJECT_ID", "").strip()
    email = os.environ.get("FIREBASE_CLIENT_EMAIL", "").strip()
    private_key = (
        os.environ.get("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n").strip()
    )
    if not (project and email and private_key):
        safe_log(logging.INFO, "firebase", "not_configured")
        return
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore

        try:
            firebase_admin.get_app()
        except ValueError:
            firebase_admin.initialize_app(
                credentials.Certificate(
                    {
                        "type": "service_account",
                        "project_id": project,
                        "client_email": email,
                        "private_key": private_key,
                        "token_uri": "https://oauth2.googleapis.com/token",
                    }
                )
            )
        firestore_db = firestore.client()
        safe_log(logging.INFO, "firebase", "initialized")
    except Exception:
        firestore_db = None
        safe_log(logging.WARNING, "firebase", "initialization_failed")


@logged_operation
def flush_file_cleanup():
    with db() as conn:
        names = [
            r["stored_name"]
            for r in conn.execute(
                "SELECT stored_name FROM pending_file_cleanup LIMIT 1000"
            )
        ]
    for name in names:
        try:
            # Metadata may already be deleted, so do not look up the file's MIME type.
            # Generated object names are unique; deleting a missing remote object is safe.
            if supabase_storage_client is not None:
                if not delete_from_supabase(name):
                    raise OSError("Supabase cleanup failed")
            elif Path(name).suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"} and not _stored_path(name).exists():
                raise OSError("Supabase image cleanup requires storage configuration")
            _stored_path(name).unlink(missing_ok=True)
            with db() as conn:
                conn.execute(
                    'DELETE FROM pending_file_cleanup WHERE stored_name=%s', (name,)
                )
        except (OSError, psycopg.Error, HTTPException):
            safe_log(logging.ERROR, "file_cleanup", "retry_required")


@logged_operation
def cleanup_failed_upload(stored):
    remote_failed = (
        supabase_storage_client is not None and not delete_from_supabase(stored)
    )
    local_failed = False
    try:
        _stored_path(stored).unlink(missing_ok=True)
    except OSError:
        local_failed = True
    if remote_failed or local_failed:
        safe_log(logging.ERROR, "upload_cleanup", "retry_required")
        try:
            with db() as conn:
                conn.execute(
                    "INSERT INTO pending_file_cleanup(stored_name) VALUES(%s) ON CONFLICT DO NOTHING",
                    (stored,),
                )
        except Exception:
            safe_log(logging.ERROR, "upload_cleanup", "retry_queue_failed")


@logged_operation
def migrate_post_columns():
    with db() as conn:
        cols = {r["name"] for r in conn.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='posts'")}
        for name, definition in {
            "post_type": "TEXT NOT NULL DEFAULT 'POST'",
            "media_url": "TEXT",
            "article_title": "TEXT",
            "article_cover_file_id": "INTEGER",
        }.items():
            if name not in cols:
                conn.execute(f"ALTER TABLE posts ADD COLUMN {name} {definition}")


@logged_operation
def initialize_database():
    initialize_onboarding_database(db)
    with db() as migration_conn:
        migration_conn.execute("SELECT pg_advisory_xact_lock(726683216)")
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        for migration in (
            init_db,
            init_step27_registration_schema,
            step12_init_db,
            step13_init_db,
            step14_init_db,
            step15_init_db,
            migrate_post_columns,
            step16_init_db,
            step17_init_db,
            communication_phase1_init_db,
            step18_init_db,
            step19_init_db,
            step20_init_db,
            step21_init_db,
            init_groups_step22,
            init_events_step23,
            init_step25_jobs,
            init_step26,
            init_step27,
            init_step28,
        ):
            migration()
        with db() as conn:
            cols = {r["name"] for r in conn.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='user_2fa'")}
            if "last_counter" not in cols:
                conn.execute(
                    "ALTER TABLE user_2fa ADD COLUMN last_counter INTEGER NOT NULL DEFAULT -1"
                )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS pending_file_cleanup(stored_name TEXT PRIMARY KEY)"
            )
        initialize_onboarding_database(db)
        flush_file_cleanup()


@asynccontextmanager
async def lifespan(app):
    requested = os.environ.get("NOVEAM_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, requested, logging.INFO)
    if not isinstance(level, int):
        level = logging.INFO
    logging.basicConfig(
        level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    logger.setLevel(level)
    await run_in_threadpool(initialize_database)
    await run_in_threadpool(initialize_push_database, db)
    await run_in_threadpool(initialize_firebase)
    check_noveam_network_configuration()
    safe_log(logging.INFO, "application", "ready")
    push_task = asyncio.create_task(push_worker(db))
    try:
        yield
    finally:
        push_task.cancel()
        try:
            await push_task
        except asyncio.CancelledError:
            pass
    for sockets in list(realtime_hub.clients.values()):
        for ws in list(sockets):
            try:
                await ws.close(code=1001)
            except Exception:
                pass
    realtime_hub.clients.clear()
    safe_log(logging.INFO, "application", "stopped")


# 5. Application and API routes


class BodyLimitMiddleware:
    """Bound streamed HTTP bodies before multipart/JSON parsing, including chunked requests."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = {k.lower(): v for k, v in scope.get("headers", [])}
        raw = headers.get(b"content-length")

        async def reject(code, detail):
            safe_log(logging.WARNING, "request_body", "rejected", status_code=code)
            response = JSONResponse(
                {"detail": detail},
                status_code=code,
                headers={
                    "X-Content-Type-Options": "nosniff",
                    "Cache-Control": "no-store",
                },
            )
            await response(scope, receive, send)

        if raw is not None:
            if not raw.isdigit():
                return await reject(400, "Invalid Content-Length")
            if int(raw) > NOVEAM_MAX_REQUEST_BYTES:
                return await reject(413, "Request too large")
        chunks = []
        size = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > NOVEAM_MAX_REQUEST_BYTES:
                return await reject(413, "Request too large")
            chunks.append(chunk)
            if not message.get("more_body", False):
                break
        body = b"".join(chunks)
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


app = FastAPI(title="Noveagamheam REST API", version="1.7.0", lifespan=lifespan)
install_onboarding_routes(app, db, current_user)
install_push_routes(app, db, current_user)
 # NOVEAM Step 5.5C — Analytics event validation
# Disabled by default; no data is stored or forwarded.

@app.post("/api/analytics/event")
async def receive_noveam_analytics(
    event: NoveamAnalyticsEvent,
    authorization: Optional[str] = Header(default=None),
):
    if not _env_bool("NOVEAM_ANALYTICS_ENABLED"):
        raise HTTPException(
            status_code=503,
            detail="Analytics collection is not enabled",
        )

    user, _ = await run_in_threadpool(
        current_user,
        authorization,
    )

    return {
        "status": "validated",
        "accepted": False,
        "stored": False,
    }   
app.add_middleware(BodyLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://noveam.in",
        "https://www.noveam.in",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/care/entries")
@logged_operation
def list_care_entries(
    patient_username: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    patient = patient_for_viewer(viewer, patient_username, "care", "view")
    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.*, u.full_name AS created_by_name, u.role AS created_by_role
            FROM care_entries c
            JOIN users u ON u.id=c.created_by_user_id
            WHERE c.patient_user_id=%s
            ORDER BY c.id DESC
            """,
            (patient["id"],),
        ).fetchall()
    audit(viewer["id"], "read", f"care_entries:{patient['id']}")
    return {"patient": dict(patient), "entries": [dict(r) for r in rows]}


@app.post("/api/care/entries", status_code=201)
@logged_operation(level=logging.INFO)
def create_care_entry(
    payload: CareEntryCreate, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    patient = patient_for_viewer(
        viewer,
        payload.patient_username,
        "care",
        "edit" if viewer["role"] != "Patient" else "view",
    )
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO care_entries( patient_user_id, created_by_user_id, title, details, status, created_at )
            VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                patient["id"],
                viewer["id"],
                payload.title.strip(),
                payload.details.strip(),
                payload.status.strip(),
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM care_entries WHERE id=%s', (cur_id,)
        ).fetchone()
    audit(viewer["id"], "create", f"care_entry:{row['id']}")
    return dict(row)


@app.get("/api/records/entries")
@logged_operation
def list_record_entries(
    patient_username: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    patient = patient_for_viewer(viewer, patient_username, "records", "view")
    with db() as conn:
        rows = conn.execute(
            """
            SELECT r.*, u.full_name AS created_by_name, u.role AS created_by_role
            FROM patient_records r
            JOIN users u ON u.id=r.created_by_user_id
            WHERE r.patient_user_id=%s
            ORDER BY r.id DESC
            """,
            (patient["id"],),
        ).fetchall()
    audit(viewer["id"], "read", f"record_entries:{patient['id']}")
    return {"patient": dict(patient), "records": [dict(r) for r in rows]}


@app.post("/api/records/entries", status_code=201)
@logged_operation(level=logging.INFO)
def create_record_entry(
    payload: RecordCreate, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    patient = patient_for_viewer(
        viewer,
        payload.patient_username,
        "records",
        "edit" if viewer["role"] != "Patient" else "view",
    )
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO patient_records( patient_user_id, created_by_user_id, record_type, title, details, created_at )
            VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                patient["id"],
                viewer["id"],
                payload.record_type.strip(),
                payload.title.strip(),
                payload.details.strip(),
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM patient_records WHERE id=%s', (cur_id,)
        ).fetchone()
    audit(viewer["id"], "create", f"patient_record:{row['id']}")
    return dict(row)


@app.get("/api/files")
@logged_operation
def list_files(
    patient_username: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        if patient_username:
            patient = patient_for_viewer(viewer, patient_username, "records", "view")
            rows = conn.execute(
                'SELECT * FROM uploaded_files WHERE patient_user_id=%s ORDER BY id DESC',
                (patient["id"],),
            ).fetchall()
        elif viewer["role"] == "Patient":
            rows = conn.execute(
                'SELECT * FROM uploaded_files WHERE owner_user_id=%s OR patient_user_id=%s ORDER BY id DESC',
                (viewer["id"], viewer["id"]),
            ).fetchall()
        else:
            rows = conn.execute(
                'SELECT * FROM uploaded_files WHERE owner_user_id=%s AND patient_user_id IS NULL ORDER BY id DESC',
                (viewer["id"],),
            ).fetchall()
    return {"files": [{k: r[k] for k in r.keys() if k != "stored_name"} for r in rows]}


@app.post("/api/files/upload", status_code=201)
@logged_operation(level=logging.INFO)
def upload_file(
    upload: UploadFile = File(...),
    category: str = Form("personal"),
    patient_username: Optional[str] = Form(None),
    record_id: Optional[int] = Form(None),
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    category = (category or "personal").strip().lower()[:40]
    patient_id = None
    if patient_username:
        patient = patient_for_viewer(
            viewer,
            patient_username,
            "records",
            "edit" if viewer["role"] != "Patient" else "view",
        )
        patient_id = patient["id"]
    elif category == "record" and viewer["role"] == "Patient":
        patient_id = viewer["id"]
    ctype = (upload.content_type or "").lower()
    if ctype not in ALLOWED_UPLOAD_TYPES:
        raise HTTPException(
            status_code=415,
            detail="Allowed files: PDF, PNG, JPG, WEBP, TXT, DOC and DOCX.",
        )
    data = upload.file.read(MAX_UPLOAD_BYTES + 1)
    if not data:
        raise HTTPException(status_code=400, detail="The selected file is empty.")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Maximum file size is 10 MB.")
    original = Path(upload.filename or "upload").name[:255]
    stored = secrets.token_hex(24) + ALLOWED_UPLOAD_TYPES[ctype]
    with db() as conn:
        if record_id is not None:
            record = conn.execute(
                'SELECT patient_user_id FROM patient_records WHERE id=%s', (record_id,)
            ).fetchone()
            if (
                not record
                or patient_id is None
                or record["patient_user_id"] != patient_id
            ):
                raise HTTPException(400, "Record must belong to the selected patient")
    _validate_upload(data, ctype)
    try:
        _stored_path(stored).write_bytes(data)
        now = datetime.now(timezone.utc).isoformat()
        with db() as conn:
            cur = conn.execute(
                """
                INSERT INTO uploaded_files( owner_user_id, patient_user_id, record_id, category, original_name, stored_name, content_type, size_bytes, created_at )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    viewer["id"],
                    patient_id,
                    record_id,
                    category,
                    original,
                    stored,
                    ctype,
                    len(data),
                    now,
                ),
            )
            cur_id = cur.fetchone()["id"]
            file_id = cur_id
            audit(viewer["id"], "upload", f"file:{cur_id}")
        return {
            "id": file_id,
            "original_name": original,
            "content_type": ctype,
            "size_bytes": len(data),
            "category": category,
            "created_at": now,
        }
    except BaseException:
        cleanup_failed_upload(stored)
        raise


@app.get("/api/files/{file_id}/download")
@logged_operation
def download_file(file_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    row = file_row_for_access(file_id, viewer)
    return stored_media_response(row, download=True)


@app.delete("/api/files/{file_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_file(file_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    row = file_row_for_access(file_id, viewer, write=True)
    if row["owner_user_id"] != viewer["id"] and viewer["role"] != "Patient":
        raise HTTPException(403, "Only the uploader or patient can delete this file.")
    with db() as conn:
        conn.execute(
            'INSERT INTO pending_file_cleanup(stored_name) VALUES(%s) ON CONFLICT DO NOTHING',
            (row["stored_name"],),
        )
        conn.execute('DELETE FROM uploaded_files WHERE id=%s', (file_id,))
        audit(viewer["id"], "delete", f"file:{file_id}")
    flush_file_cleanup()


@app.post("/api/profile-media/upload", status_code=201)
@logged_operation(level=logging.INFO)
def upload_profile_media(
    kind: str = Form(...),
    upload: UploadFile = File(...),
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    require_permission(viewer, "profile:write")
    if kind not in {"photo", "cover"}:
        raise HTTPException(status_code=400, detail="kind must be photo or cover")
    ctype = (upload.content_type or "").lower()
    if ctype not in SOCIAL_IMAGE_TYPES:
        raise HTTPException(
            status_code=415, detail="Profile images must be PNG, JPG or WEBP."
        )
    data = upload.file.read(MAX_UPLOAD_BYTES + 1)
    if not data or len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413 if data else 400,
            detail="Profile image must be non-empty and at most 10 MB.",
        )
    stored = secrets.token_hex(24) + ALLOWED_UPLOAD_TYPES[ctype]
    _validate_upload(data, ctype)
    try:
        upload_to_supabase(stored_name=stored, data=data, content_type=ctype)
        now = datetime.now(timezone.utc).isoformat()
        with db() as conn:
            cur = conn.execute(
                """
                INSERT INTO uploaded_files( owner_user_id,category,original_name,stored_name, content_type,size_bytes,created_at )
                VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    viewer["id"],
                    "profile_" + kind,
                    Path(upload.filename or kind).name[:255],
                    stored,
                    ctype,
                    len(data),
                    now,
                ),
            )
            cur_id = cur.fetchone()["id"]
            file_id = cur_id
            conn.execute(
                f"UPDATE profiles SET {kind}=%s WHERE user_id=%s",
                (f"file:{file_id}", viewer["id"]),
            )
            audit(viewer["id"], "upload", f"file:{cur_id}")
        return {"id": file_id, "kind": kind, "value": f"file:{file_id}"}
    except BaseException:
        cleanup_failed_upload(stored)
        raise


@app.get("/api/media/{file_id}")
@logged_operation
def authenticated_media(
    file_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM uploaded_files WHERE id=%s', (file_id,)
        ).fetchone()
        post = conn.execute(
            'SELECT * FROM posts WHERE media_file_id=%s OR article_cover_file_id=%s',
            (file_id, file_id),
        ).fetchone()
        page_post = (
            conn.execute(
                'SELECT * FROM page_posts WHERE media_file_id=%s', (file_id,)
            ).fetchone()
            if _table_exists(conn, "page_posts")
            else None
        )
    if not row or row["content_type"] not in SOCIAL_POST_MEDIA_TYPES:
        raise HTTPException(status_code=404, detail="Media not found")
    allowed = row["owner_user_id"] == viewer["id"] or row["category"] in {
        "profile_photo",
        "profile_cover",
    }
    if post and post_visible_to(post, viewer):
        allowed = True
    if page_post:
        allowed = True
    if not allowed:
        raise HTTPException(
            status_code=403, detail="You do not have access to this image"
        )
    return stored_media_response(row)


@app.post("/api/posts/media", status_code=201)
@logged_operation(level=logging.INFO)
def upload_post_media(
    upload: UploadFile = File(...), authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    ctype = (upload.content_type or "").lower()
    if ctype not in SOCIAL_POST_MEDIA_TYPES:
        raise HTTPException(
            status_code=415,
            detail="Supported post attachments: PNG/JPG/WEBP, MP4/WEBM/MOV, MP3/M4A/WAV/OGG, PDF/TXT/DOC/DOCX.",
        )
    data = upload.file.read(MAX_UPLOAD_BYTES + 1)
    if not data or len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413 if data else 400,
            detail="Attachment must be non-empty and at most 10 MB.",
        )
    ext = (
        ALLOWED_UPLOAD_TYPES.get(ctype)
        or {
            "video/mp4": ".mp4",
            "video/webm": ".webm",
            "video/quicktime": ".mov",
            "audio/mpeg": ".mp3",
            "audio/mp4": ".m4a",
            "audio/wav": ".wav",
            "audio/x-wav": ".wav",
            "audio/ogg": ".ogg",
        }[ctype]
    )
    if not ext:
        ext = ALLOWED_UPLOAD_TYPES.get(ctype, "")
    stored = secrets.token_hex(24) + ext
    _validate_upload(data, ctype)
    try:
        if ctype in SOCIAL_IMAGE_TYPES:
            upload_to_supabase(stored_name=stored, data=data, content_type=ctype)
        else:
            _stored_path(stored).write_bytes(data)
        now = datetime.now(timezone.utc).isoformat()
        category = "post_image" if ctype in SOCIAL_IMAGE_TYPES else "post_media"
        with db() as conn:
            cur = conn.execute(
                """
                INSERT INTO uploaded_files( owner_user_id,category,original_name,stored_name, content_type,size_bytes,created_at )
                VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    viewer["id"],
                    category,
                    Path(upload.filename or "post-attachment").name[:255],
                    stored,
                    ctype,
                    len(data),
                    now,
                ),
            )
            cur_id = cur.fetchone()["id"]
            audit(viewer["id"], "upload", f"file:{cur_id}")
        return {
            "id": cur_id,
            "content_type": ctype,
            "name": Path(upload.filename or "post-attachment").name[:255],
        }
    except BaseException:
        cleanup_failed_upload(stored)
        raise


@app.get("/api/posts/ranked")
@logged_operation
def list_ranked_posts(
    mode: str = "personalized",
    authorization: Optional[str] = Header(default=None)
):
    from urllib.request import Request as URLRequest, urlopen
    from urllib.error import URLError

    viewer, _ = current_user(authorization)

    if mode not in {
        "personalized",
        "discovery",
        "following",
        "chronological"
    }:
        raise HTTPException(400, "Invalid feed mode")

    # Keep the existing authentication and visibility rules.
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM posts ORDER BY id DESC LIMIT 200"
        ).fetchall()

    visible_rows = [
        row for row in rows
        if post_visible_to(row, viewer)
    ]

    posts = [
        serialize_post(row, viewer)
        for row in visible_rows
    ]

    if not posts:
        return {"posts": []}

    # Retrieve following relationships.
    with db() as conn:
        following_ids = {
            row["followed_user_id"]
            for row in conn.execute(
                """
                SELECT followed_user_id
                FROM follows
                WHERE follower_user_id=%s
                """,
                (viewer["id"],)
            ).fetchall()
        }

    now = datetime.now(timezone.utc).timestamp()

    def created_timestamp(value):
        try:
            if isinstance(value, (int, float)):
                return float(value)
            parsed = datetime.fromisoformat(
                str(value).replace("Z", "+00:00")
            )
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except (ValueError, TypeError, OverflowError):
            return 0.0

    ranking_posts = []

    for post in posts:
        author_id = post["author_user_id"]
        followed = author_id in following_ids

        # Conservative temporary ranking signals.
        # Replace with genuine user-interest and quality
        # measurements in a later phase.
        ranking_posts.append({
            "id": str(post["id"]),
            "created_at": created_timestamp(
                post["created_at"]
            ),
            "visible": True,
            "followed": followed,
            "interest": 0.5,
            "relationship": 1.0 if followed else 0.0,
            "quality": min(
                1.0,
                max(0, post["like_count"]) / 20.0
            ),
            "diversity": 0.5
        })

    payload = {
        "mode": mode,
        "now": now,
        "posts": ranking_posts
    }

    algorithm_url = os.environ.get(
        "NOVEAM_ALGORITHM_URL",
        "https://noveam-algorithm.onrender.com"
    ).rstrip("/")

    # Original feed order is the safe fallback.
    try:
        request = URLRequest(
            algorithm_url + "/api/ai/rank",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        with urlopen(request, timeout=3) as response:
            result = json.load(response)

        post_by_id = {
            str(post["id"]): post
            for post in posts
        }

        ranked = []
        seen = set()

        for item in result["results"]:
            post_id = str(item["id"])
            if post_id in post_by_id and post_id not in seen:
                ranked.append(post_by_id[post_id])
                seen.add(post_id)

        # Keep any eligible posts omitted by the service,
        # except in following mode.
        if mode != "following":
            ranked.extend(
                post for post in posts
                if str(post["id"]) not in seen
            )

        return {"posts": ranked}

    except Exception:
        safe_log(
            logging.WARNING,
            "ai_feed",
            "algorithm_fallback"
        )

        if mode == "following":
            posts = [
                post for post in posts
                if post["author_user_id"] in following_ids
            ]
        elif mode == "chronological":
            posts.sort(
                key=lambda p: created_timestamp(
                    p["created_at"]
                ),
                reverse=True
            )

        return {"posts": posts}

@app.get("/api/posts")
@logged_operation
def list_posts(authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute("SELECT * FROM posts ORDER BY id DESC LIMIT 200").fetchall()
    posts = [serialize_post(r, viewer) for r in rows if post_visible_to(r, viewer)]
    return {"posts": posts}


@app.get("/api/posts/{post_id}")
@logged_operation
def get_post(post_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute('SELECT * FROM posts WHERE id=%s', (post_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Post not found")
    if not post_visible_to(row, viewer):
        raise HTTPException(status_code=403, detail="Post is not visible to you")
    return serialize_post(row, viewer)


@app.post("/api/posts", status_code=201)
@logged_operation(level=logging.INFO)
def create_post(
    payload: PostCreate, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    visibility = payload.visibility.upper()
    if visibility not in {"PUBLIC", "CONNECTIONS", "CARE_TEAM", "PRIVATE"}:
        raise HTTPException(status_code=400, detail="Invalid visibility")
    post_type = payload.post_type.upper()
    if post_type not in {"POST", "PHOTO", "VIDEO", "AUDIO", "FILE", "ARTICLE"}:
        raise HTTPException(status_code=400, detail="Invalid post type")
    if payload.media_file_id:
        with db() as conn:
            f = conn.execute(
                """
                SELECT *
                FROM uploaded_files
                WHERE id=%s
                AND owner_user_id=%s
                AND category IN ('post_image','post_media')
                """,
                (payload.media_file_id, viewer["id"]),
            ).fetchone()
        if not f:
            raise HTTPException(status_code=400, detail="Invalid post attachment")
    if payload.media_url:
        if not (
            payload.media_url.startswith("https://")
            or payload.media_url.startswith("http://")
        ):
            raise HTTPException(
                status_code=400, detail="Media URL must start with http:// or https://"
            )
    if post_type == "ARTICLE" and (not (payload.article_title or "").strip()):
        raise HTTPException(status_code=400, detail="Article title is required")
    if payload.article_cover_file_id:
        with db() as conn:
            cover = conn.execute(
                "SELECT * FROM uploaded_files WHERE id=%s AND owner_user_id=%s AND category='post_image'",
                (payload.article_cover_file_id, viewer["id"]),
            ).fetchone()
        if not cover or cover["content_type"] not in SOCIAL_IMAGE_TYPES:
            raise HTTPException(status_code=400, detail="Invalid article cover image")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO posts( author_user_id,text,category,visibility,media_file_id, created_at,updated_at,post_type,media_url, article_title,article_cover_file_id )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                viewer["id"],
                payload.text.strip(),
                payload.category.strip() or "Other",
                visibility,
                payload.media_file_id,
                now,
                now,
                post_type,
                (payload.media_url or "").strip() or None,
                (payload.article_title or "").strip() or None,
                payload.article_cover_file_id,
            ),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM posts WHERE id=%s', (cur_id,)
        ).fetchone()
    audit(viewer["id"], "create", f"post:{row['id']}")
    return serialize_post(row, viewer)


@app.put("/api/posts/{post_id}")
@logged_operation(level=logging.INFO)
def update_post(
    post_id: int,
    payload: PostUpdate,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    visibility = payload.visibility.upper()
    if visibility not in {"PUBLIC", "CONNECTIONS", "CARE_TEAM", "PRIVATE"}:
        raise HTTPException(status_code=400, detail="Invalid visibility")
    with db() as conn:
        row = conn.execute('SELECT * FROM posts WHERE id=%s', (post_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Post not found")
        if row["author_user_id"] != viewer["id"]:
            raise HTTPException(
                status_code=403, detail="Only the author can edit this post"
            )
        conn.execute(
            'UPDATE posts SET text=%s,category=%s,visibility=%s,updated_at=%s WHERE id=%s',
            (
                payload.text.strip(),
                payload.category.strip(),
                visibility,
                datetime.now(timezone.utc).isoformat(),
                post_id,
            ),
        )
        row = conn.execute('SELECT * FROM posts WHERE id=%s', (post_id,)).fetchone()
    return serialize_post(row, viewer)


@app.delete("/api/posts/{post_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_post(post_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute('SELECT * FROM posts WHERE id=%s', (post_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Post not found")
        if row["author_user_id"] != viewer["id"]:
            raise HTTPException(
                status_code=403, detail="Only the author can delete this post"
            )
        conn.execute('DELETE FROM posts WHERE id=%s', (post_id,))
    audit(viewer["id"], "delete", f"post:{post_id}")


@app.post("/api/posts/{post_id}/like", status_code=201)
@logged_operation(level=logging.INFO)
def like_post(post_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute('SELECT * FROM posts WHERE id=%s', (post_id,)).fetchone()
        if not row or not post_visible_to(row, viewer):
            raise HTTPException(status_code=404, detail="Post not found")
        conn.execute(
            'INSERT INTO post_likes( post_id,user_id,created_at ) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
            (post_id, viewer["id"], datetime.now(timezone.utc).isoformat()),
        )
        if row["author_user_id"] != viewer["id"]:
            create_notification(
                conn,
                row["author_user_id"],
                viewer["id"],
                "POST_LIKE",
                "New like",
                "liked your post",
                "post",
                post_id,
            )
    return {"ok": True}


@app.delete("/api/posts/{post_id}/like", status_code=204)
@logged_operation(level=logging.INFO)
def unlike_post(post_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        conn.execute(
            'DELETE FROM post_likes WHERE post_id=%s AND user_id=%s',
            (post_id, viewer["id"]),
        )


@app.get("/api/posts/{post_id}/comments")
@logged_operation
def list_comments(post_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        post = conn.execute('SELECT * FROM posts WHERE id=%s', (post_id,)).fetchone()
        if not post or not post_visible_to(post, viewer):
            raise HTTPException(status_code=404, detail="Post not found")
        rows = conn.execute(
            """
            SELECT c.*,u.username,u.full_name,p.display_name
            FROM post_comments c
            JOIN users u ON u.id=c.author_user_id
            LEFT JOIN profiles p ON p.user_id=u.id
            WHERE c.post_id=%s
            ORDER BY c.id
            """,
            (post_id,),
        ).fetchall()
    return {
        "comments": [
            {
                **dict(r),
                "author_name": r["display_name"] or r["full_name"],
                "is_owner": r["author_user_id"] == viewer["id"],
            }
            for r in rows
        ]
    }


@app.post("/api/posts/{post_id}/comments", status_code=201)
@logged_operation(level=logging.INFO)
def add_comment(
    post_id: int,
    payload: CommentCreate,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        post = conn.execute('SELECT * FROM posts WHERE id=%s', (post_id,)).fetchone()
        if not post or not post_visible_to(post, viewer):
            raise HTTPException(status_code=404, detail="Post not found")
        cur = conn.execute(
            'INSERT INTO post_comments( post_id,author_user_id,text,created_at ) VALUES(%s,%s,%s,%s) RETURNING id',
            (
                post_id,
                viewer["id"],
                payload.text.strip(),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        cur_id = cur.fetchone()["id"]
        if post["author_user_id"] != viewer["id"]:
            create_notification(
                conn,
                post["author_user_id"],
                viewer["id"],
                "POST_COMMENT",
                "New comment",
                "commented on your post",
                "post",
                post_id,
            )
    return {"id": cur_id, "ok": True}


@app.delete("/api/comments/{comment_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_comment(
    comment_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM post_comments WHERE id=%s', (comment_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Comment not found")
        if row["author_user_id"] != viewer["id"]:
            raise HTTPException(
                status_code=403, detail="Only the author can delete this comment"
            )
        conn.execute('DELETE FROM post_comments WHERE id=%s', (comment_id,))


@app.get("/api/social/people")
@logged_operation
def social_people(q: str = "", authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    term = "%" + q.strip() + "%"
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM users
            WHERE id<>%s
            AND ( username ILIKE %s
            OR full_name ILIKE %s )
            ORDER BY full_name
            LIMIT 100
            """,
            (viewer["id"], term, term),
        ).fetchall()
        people = [social_user(conn, r, viewer["id"]) for r in rows]
        return {"people": people}


@app.post("/api/social/follow/{user_id}", status_code=201)
@logged_operation(level=logging.INFO)
def follow_user(user_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    if user_id == viewer["id"]:
        raise HTTPException(400, "You cannot follow yourself")
    with db() as conn:
        if not conn.execute('SELECT 1 FROM users WHERE id=%s', (user_id,)).fetchone():
            raise HTTPException(404, "User not found")
        conn.execute(
            'INSERT INTO follows( follower_user_id, followed_user_id, created_at ) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
            (viewer["id"], user_id, datetime.now(timezone.utc).isoformat()),
        )
        create_notification(
            conn,
            user_id,
            viewer["id"],
            "NEW_FOLLOWER",
            "New follower",
            "started following you",
            "user",
            viewer["id"],
        )
    return {"ok": True}


@app.delete("/api/social/follow/{user_id}", status_code=204)
@logged_operation(level=logging.INFO)
def unfollow_user(user_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        conn.execute(
            'DELETE FROM follows WHERE follower_user_id=%s AND followed_user_id=%s',
            (viewer["id"], user_id),
        )


@app.post("/api/social/connections/{user_id}", status_code=201)
@logged_operation(level=logging.INFO)
def request_connection(
    user_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    if user_id == viewer["id"]:
        raise HTTPException(400, "You cannot connect with yourself")
    with db() as conn:
        if not conn.execute('SELECT 1 FROM users WHERE id=%s', (user_id,)).fetchone():
            raise HTTPException(404, "User not found")
        if are_connected(conn, viewer["id"], user_id):
            return {"ok": True, "status": "ACCEPTED"}
        incoming = conn.execute(
            """
            SELECT id
            FROM connection_requests
            WHERE sender_user_id=%s
            AND receiver_user_id=%s
            AND status='PENDING'
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_id, viewer["id"]),
        ).fetchone()
        if incoming:
            conn.execute(
                "UPDATE connection_requests SET status='ACCEPTED', responded_at=%s WHERE id=%s",
                (datetime.now(timezone.utc).isoformat(), incoming["id"]),
            )
            return {"ok": True, "status": "ACCEPTED"}
        existing = conn.execute(
            """
            SELECT id
            FROM connection_requests
            WHERE sender_user_id=%s
            AND receiver_user_id=%s
            AND status='PENDING'
            """,
            (viewer["id"], user_id),
        ).fetchone()
        if existing:
            return {"ok": True, "status": "PENDING", "id": existing["id"]}
        cur = conn.execute(
            """
            INSERT INTO connection_requests( sender_user_id, receiver_user_id, status, created_at )
            VALUES(%s,%s,'PENDING',%s) RETURNING id""",
            (viewer["id"], user_id, datetime.now(timezone.utc).isoformat()),
        )
        cur_id = cur.fetchone()["id"]
        create_notification(
            conn,
            user_id,
            viewer["id"],
            "CONNECTION_REQUEST",
            "Connection request",
            "sent you a connection request",
            "connection_request",
            cur_id,
        )
    return {"ok": True, "status": "PENDING", "id": cur_id}


@app.post("/api/social/connections/requests/{request_id}/accept")
@logged_operation(level=logging.INFO)
def accept_connection(
    request_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        r = conn.execute(
            "SELECT * FROM connection_requests WHERE id=%s AND receiver_user_id=%s AND status='PENDING'",
            (request_id, viewer["id"]),
        ).fetchone()
        if not r:
            raise HTTPException(404, "Pending request not found")
        conn.execute(
            "UPDATE connection_requests SET status='ACCEPTED', responded_at=%s WHERE id=%s",
            (datetime.now(timezone.utc).isoformat(), request_id),
        )
        create_notification(
            conn,
            r["sender_user_id"],
            viewer["id"],
            "CONNECTION_ACCEPTED",
            "Connection accepted",
            "accepted your connection request",
            "user",
            viewer["id"],
        )
    return {"ok": True}


@app.post("/api/social/connections/requests/{request_id}/decline")
@logged_operation(level=logging.INFO)
def decline_connection(
    request_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        r = conn.execute(
            "SELECT * FROM connection_requests WHERE id=%s AND receiver_user_id=%s AND status='PENDING'",
            (request_id, viewer["id"]),
        ).fetchone()
        if not r:
            raise HTTPException(404, "Pending request not found")
        conn.execute(
            "UPDATE connection_requests SET status='DECLINED', responded_at=%s WHERE id=%s",
            (datetime.now(timezone.utc).isoformat(), request_id),
        )
    return {"ok": True}


@app.delete("/api/social/connections/{user_id}", status_code=204)
@logged_operation(level=logging.INFO)
def remove_connection(
    user_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        conn.execute(
            """
            DELETE
            FROM connection_requests
            WHERE status='ACCEPTED'
            AND ( (sender_user_id=%s
            AND receiver_user_id=%s)
            OR (sender_user_id=%s
            AND receiver_user_id=%s) )
            """,
            (viewer["id"], user_id, user_id, viewer["id"]),
        )


@app.get("/api/social/network")
@logged_operation
def my_network(authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        followers = conn.execute(
            """
            SELECT u.*
            FROM follows f
            JOIN users u ON u.id=f.follower_user_id
            WHERE f.followed_user_id=%s
            ORDER BY f.created_at DESC
            """,
            (viewer["id"],),
        ).fetchall()
        following = conn.execute(
            """
            SELECT u.*
            FROM follows f
            JOIN users u ON u.id=f.followed_user_id
            WHERE f.follower_user_id=%s
            ORDER BY f.created_at DESC
            """,
            (viewer["id"],),
        ).fetchall()
        connections = conn.execute(
            """
            SELECT u.*
            FROM users u
            WHERE u.id<>%s
            AND EXISTS( SELECT 1
            FROM connection_requests c
            WHERE c.status='ACCEPTED'
            AND ( (c.sender_user_id=%s
            AND c.receiver_user_id=u.id)
            OR (c.receiver_user_id=%s
            AND c.sender_user_id=u.id) ) )
            ORDER BY u.full_name
            """,
            (viewer["id"], viewer["id"], viewer["id"]),
        ).fetchall()
        requests = conn.execute(
            """
            SELECT c.id request_id,u.*
            FROM connection_requests c
            JOIN users u ON u.id=c.sender_user_id
            WHERE c.receiver_user_id=%s
            AND c.status='PENDING'
            ORDER BY c.id DESC
            """,
            (viewer["id"],),
        ).fetchall()
        result = {
            "followers": [social_user(conn, r, viewer["id"]) for r in followers],
            "following": [social_user(conn, r, viewer["id"]) for r in following],
            "connections": [social_user(conn, r, viewer["id"]) for r in connections],
            "requests": [
                dict(social_user(conn, r, viewer["id"]), request_id=r["request_id"])
                for r in requests
            ],
        }
        return result


@app.get("/api/messages/conversations")
@logged_operation
def message_conversations(authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        peers = conn.execute(
            """
            SELECT u.*
            FROM users u
            WHERE u.id<>%s
            AND EXISTS( SELECT 1
            FROM connection_requests c
            WHERE c.status='ACCEPTED'
            AND ((c.sender_user_id=%s
            AND c.receiver_user_id=u.id)
            OR (c.receiver_user_id=%s
            AND c.sender_user_id=u.id)))
            ORDER BY u.full_name
            """,
            (viewer["id"], viewer["id"], viewer["id"]),
        ).fetchall()
        out = []
        for peer in peers:
            last = conn.execute(
                """
                SELECT *
                FROM private_messages
                WHERE ((sender_user_id=%s
                AND receiver_user_id=%s)
                OR (sender_user_id=%s
                AND receiver_user_id=%s))
                AND NOT ((sender_user_id=%s
                AND deleted_by_sender=1)
                OR (receiver_user_id=%s
                AND deleted_by_receiver=1))
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    viewer["id"],
                    peer["id"],
                    peer["id"],
                    viewer["id"],
                    viewer["id"],
                    viewer["id"],
                ),
            ).fetchone()
            unread = conn.execute(
                """
                SELECT COUNT(*) n
                FROM private_messages
                WHERE sender_user_id=%s
                AND receiver_user_id=%s
                AND read_at IS NULL
                AND deleted_by_receiver=0
                """,
                (peer["id"], viewer["id"]),
            ).fetchone()["n"]
            out.append(
                {
                    "user": message_user(conn, peer["id"]),
                    "last_message": dict(last) if last else None,
                    "unread": unread,
                }
            )
        out.sort(
            key=lambda x: x["last_message"]["created_at"] if x["last_message"] else "",
            reverse=True,
        )
        return {"conversations": out}


@app.get("/api/messages/{user_id}")
@logged_operation
def get_private_messages(
    user_id: int, limit: int = 100, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    limit = max(1, min(limit, 200))
    with db() as conn:
        _require_unblocked(conn, viewer["id"], user_id)
        if not are_connected(conn, viewer["id"], user_id):
            raise HTTPException(
                403, "Private messages are available only between accepted connections"
            )
        rows = conn.execute(
            """
            SELECT *
            FROM private_messages
            WHERE ((sender_user_id=%s
            AND receiver_user_id=%s
            AND deleted_by_sender=0)
            OR (sender_user_id=%s
            AND receiver_user_id=%s
            AND deleted_by_receiver=0))
            ORDER BY id DESC
            LIMIT %s
            """,
            (viewer["id"], user_id, user_id, viewer["id"], limit),
        ).fetchall()
        if _communication_settings_row(conn, viewer["id"])["read_receipts"]:
            conn.execute(
                """
                UPDATE private_messages
                SET read_at=%s
                WHERE sender_user_id=%s
                AND receiver_user_id=%s
                AND read_at IS NULL
                """,
                (datetime.now(timezone.utc).isoformat(), user_id, viewer["id"]),
            )
        return {
            "user": message_user(conn, user_id),
            "messages": [dict(r) for r in reversed(rows)],
        }


@app.post("/api/messages/{user_id}", status_code=201)
@logged_operation(level=logging.INFO)
def send_private_message(
    user_id: int,
    payload: PrivateMessageCreate,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    body = payload.body.strip()
    if not body:
        raise HTTPException(400, "Message cannot be empty")
    with db() as conn:
        _require_unblocked(conn, viewer["id"], user_id)
        if not are_connected(conn, viewer["id"], user_id):
            raise HTTPException(403, "You can message only accepted connections")
        if not conn.execute('SELECT 1 FROM users WHERE id=%s', (user_id,)).fetchone():
            raise HTTPException(404, "User not found")
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            'INSERT INTO private_messages(sender_user_id,receiver_user_id,body,created_at) VALUES(%s,%s,%s,%s) RETURNING id',
            (viewer["id"], user_id, body, now),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM private_messages WHERE id=%s', (cur_id,)
        ).fetchone()
        create_notification(
            conn,
            user_id,
            viewer["id"],
            "NEW_MESSAGE",
            "New message",
            "sent you a private message",
            "message",
            cur_id,
        )
        audit(viewer["id"], "message.send", f"user:{user_id}")
        return dict(row)


@app.delete("/api/messages/message/{message_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_private_message(
    message_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM private_messages WHERE id=%s', (message_id,)
        ).fetchone()
        if not row or viewer["id"] not in (
            row["sender_user_id"],
            row["receiver_user_id"],
        ):
            raise HTTPException(404, "Message not found")
        if viewer["id"] == row["sender_user_id"]:
            conn.execute(
                'UPDATE private_messages SET deleted_by_sender=1 WHERE id=%s',
                (message_id,),
            )
        else:
            conn.execute(
                'UPDATE private_messages SET deleted_by_receiver=1 WHERE id=%s',
                (message_id,),
            )


@app.get("/api/communication/spaces")
@logged_operation
def communication_spaces(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT s.*,m.role
            FROM communication_spaces s
            JOIN communication_space_members m ON m.space_id=s.id
            WHERE m.user_id=%s
            ORDER BY s.id DESC
            """,
            (u["id"],),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["channels"] = [
                dict(x)
                for x in conn.execute(
                    'SELECT * FROM communication_channels WHERE space_id=%s ORDER BY id',
                    (r["id"],),
                ).fetchall()
            ]
            out.append(d)
        return {"spaces": out}


@app.post("/api/communication/spaces", status_code=201)
@logged_operation(level=logging.INFO)
def create_communication_space(
    payload: CommunicationSpaceCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    if payload.privacy not in {"private", "public"}:
        raise HTTPException(400, "Invalid privacy")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO communication_spaces(owner_user_id,name,description,privacy,created_at)
            VALUES(%s,%s,%s,%s,%s) RETURNING id""",
            (
                u["id"],
                payload.name.strip(),
                payload.description.strip(),
                payload.privacy,
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        sid = cur_id
        conn.execute(
            'INSERT INTO communication_space_members(space_id,user_id,role,joined_at) VALUES(%s,%s,%s,%s)',
            (sid, u["id"], "owner", now),
        )
        conn.execute(
            'INSERT INTO communication_channels(space_id,name,channel_type,topic,created_at) VALUES(%s,%s,%s,%s,%s)',
            (sid, "general", "text", "General discussion", now),
        )
        audit(u["id"], "communication.space.create", f"space:{sid}")
        return {"id": sid}


@app.post("/api/communication/spaces/{space_id}/members", status_code=201)
@logged_operation(level=logging.INFO)
def add_communication_member(
    space_id: int,
    payload: CommunicationMemberAdd,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    with db() as conn:
        role = _space_role(conn, space_id, u["id"])
        if role not in {"owner", "admin"}:
            raise HTTPException(403, "Only owners/admins can add members")
        target = conn.execute(
            'SELECT id FROM users WHERE lower(username)=lower(%s)',
            (payload.username.strip(),),
        ).fetchone()
        if not target:
            raise HTTPException(404, "User not found")
        conn.execute(
            'INSERT INTO communication_space_members(space_id,user_id,role,joined_at) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (space_id, target["id"], "member", datetime.now(timezone.utc).isoformat()),
        )
        return {"ok": True}


@app.post("/api/communication/spaces/{space_id}/channels", status_code=201)
@logged_operation(level=logging.INFO)
def create_communication_channel(
    space_id: int,
    payload: CommunicationChannelCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    if payload.channel_type not in {"text", "announcement", "voice"}:
        raise HTTPException(400, "Invalid channel type")
    with db() as conn:
        role = _space_role(conn, space_id, u["id"])
        if role not in {"owner", "admin"}:
            raise HTTPException(403, "Only owners/admins can create channels")
        cur = conn.execute(
            'INSERT INTO communication_channels(space_id,name,channel_type,topic,created_at) VALUES(%s,%s,%s,%s,%s) RETURNING id',
            (
                space_id,
                payload.name.strip(),
                payload.channel_type,
                payload.topic.strip(),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id}


@app.get("/api/communication/channels/{channel_id}/messages")
@logged_operation
def communication_channel_messages(
    channel_id: int,
    limit: int = 100,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    limit = max(1, min(limit, 200))
    with db() as conn:
        ch = conn.execute(
            'SELECT * FROM communication_channels WHERE id=%s', (channel_id,)
        ).fetchone()
        if not ch or not _space_role(conn, ch["space_id"], u["id"]):
            raise HTTPException(403, "Not a member of this communication space")
        rows = conn.execute(
            """
            SELECT m.*,u.username,u.full_name,p.display_name
            FROM communication_channel_messages m
            JOIN users u ON u.id=m.author_user_id
            LEFT JOIN profiles p ON p.user_id=u.id
            WHERE m.channel_id=%s
            ORDER BY m.id DESC
            LIMIT %s
            """,
            (channel_id, limit),
        ).fetchall()
        out = []
        for r in reversed(rows):
            d = dict(r)
            d["author_name"] = r["display_name"] or r["full_name"]
            d["reactions"] = [
                dict(x)
                for x in conn.execute(
                    'SELECT emoji,COUNT(*) count FROM communication_message_reactions WHERE message_id=%s GROUP BY emoji',
                    (r["id"],),
                ).fetchall()
            ]
            out.append(d)
        return {"channel": dict(ch), "messages": out}


@app.post("/api/communication/channels/{channel_id}/messages", status_code=201)
@logged_operation(level=logging.INFO)
def send_communication_channel_message(
    channel_id: int,
    payload: CommunicationMessageCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    body = payload.body.strip()
    if not body:
        raise HTTPException(400, "Message cannot be empty")
    with db() as conn:
        ch = conn.execute(
            'SELECT * FROM communication_channels WHERE id=%s', (channel_id,)
        ).fetchone()
        if not ch or not _space_role(conn, ch["space_id"], u["id"]):
            raise HTTPException(403, "Not a member of this communication space")
        if ch["channel_type"] == "announcement" and _space_role(
            conn, ch["space_id"], u["id"]
        ) not in {"owner", "admin"}:
            raise HTTPException(403, "Only owners/admins can post announcements")
        if payload.reply_to_id and (
            not conn.execute(
                'SELECT 1 FROM communication_channel_messages WHERE id=%s AND channel_id=%s',
                (payload.reply_to_id, channel_id),
            ).fetchone()
        ):
            raise HTTPException(400, "Reply target is invalid")
        cur = conn.execute(
            """
            INSERT INTO communication_channel_messages(channel_id,author_user_id,body,reply_to_id,created_at)
            VALUES(%s,%s,%s,%s,%s) RETURNING id""",
            (
                channel_id,
                u["id"],
                body,
                payload.reply_to_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id}


@app.post("/api/communication/messages/{message_id}/reactions")
@logged_operation(level=logging.INFO)
def react_communication_message(
    message_id: int,
    payload: CommunicationReactionCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    with db() as conn:
        m = conn.execute(
            """
            SELECT m.id,c.space_id
            FROM communication_channel_messages m
            JOIN communication_channels c ON c.id=m.channel_id
            WHERE m.id=%s
            """,
            (message_id,),
        ).fetchone()
        if not m or not _space_role(conn, m["space_id"], u["id"]):
            raise HTTPException(403, "Not allowed")
        existing = conn.execute(
            'SELECT 1 FROM communication_message_reactions WHERE message_id=%s AND user_id=%s AND emoji=%s',
            (message_id, u["id"], payload.emoji),
        ).fetchone()
        if existing:
            conn.execute(
                'DELETE FROM communication_message_reactions WHERE message_id=%s AND user_id=%s AND emoji=%s',
                (message_id, u["id"], payload.emoji),
            )
        else:
            conn.execute(
                'INSERT INTO communication_message_reactions(message_id,user_id,emoji,created_at) VALUES(%s,%s,%s,%s)',
                (
                    message_id,
                    u["id"],
                    payload.emoji,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        return {"ok": True, "active": not bool(existing)}


@app.post("/api/communication/presence")
@logged_operation(level=logging.INFO)
def communication_presence_update(
    payload: CommunicationPresenceUpdate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    if payload.status not in {"online", "away", "busy", "offline"}:
        raise HTTPException(400, "Invalid presence status")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        conn.execute(
            """
            INSERT INTO communication_presence(user_id,status,last_seen)
            VALUES(%s,%s,%s)
            ON CONFLICT(user_id) DO UPDATE
            SET status=excluded.status,last_seen=excluded.last_seen
            """,
            (u["id"], payload.status, now),
        )
    return {"ok": True, "status": payload.status, "last_seen": now}


@app.get("/api/communication/spaces/{space_id}/presence")
@logged_operation
def communication_space_presence(
    space_id: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        if not _space_role(conn, space_id, u["id"]):
            raise HTTPException(403, "Not a member of this communication space")
        rows = conn.execute(
            """
            SELECT m.user_id,u.username,u.full_name,p.display_name, COALESCE(pr.status,'offline') status,pr.last_seen
            FROM communication_space_members m
            JOIN users u ON u.id=m.user_id
            LEFT JOIN profiles p ON p.user_id=u.id
            LEFT JOIN communication_presence pr ON pr.user_id=u.id
            WHERE m.space_id=%s
            ORDER BY COALESCE(p.display_name,u.full_name)
            """,
            (space_id,),
        ).fetchall()
        return {
            "members": [
                {
                    "user_id": r["user_id"],
                    "username": r["username"],
                    "name": r["display_name"] or r["full_name"],
                    "status": (
                        r["status"]
                        if _communication_settings_row(conn, r["user_id"])[
                            "online_status"
                        ]
                        else "offline"
                    ),
                    "last_seen": (
                        r["last_seen"]
                        if _communication_settings_row(conn, r["user_id"])[
                            "online_status"
                        ]
                        else None
                    ),
                }
                for r in rows
            ]
        }


@app.post("/api/communication/channels/{channel_id}/signals", status_code=201)
@logged_operation(level=logging.INFO)
def communication_send_signal(
    channel_id: int,
    payload: CommunicationSignalCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    allowed = {
        "join",
        "leave",
        "offer",
        "answer",
        "ice",
        "camera",
        "microphone",
        "screen",
    }
    if payload.signal_type not in allowed:
        raise HTTPException(400, "Invalid call signal")
    with db() as conn:
        ch = conn.execute(
            'SELECT * FROM communication_channels WHERE id=%s', (channel_id,)
        ).fetchone()
        if (
            not ch
            or ch["channel_type"] != "voice"
            or (not _space_role(conn, ch["space_id"], u["id"]))
        ):
            raise HTTPException(
                403,
                "Voice/video signaling is available only to members of this voice channel",
            )
        if payload.recipient_user_id and (
            not _space_role(conn, ch["space_id"], payload.recipient_user_id)
        ):
            raise HTTPException(
                400, "Recipient is not a member of this communication space"
            )
        cur = conn.execute(
            """
            INSERT INTO communication_call_signals(channel_id,sender_user_id,recipient_user_id,signal_type,payload,created_at)
            VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                channel_id,
                u["id"],
                payload.recipient_user_id,
                payload.signal_type,
                json.dumps(payload.payload),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id}


@app.get("/api/communication/channels/{channel_id}/signals")
@logged_operation
def communication_get_signals(
    channel_id: int, after: int = 0, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        ch = conn.execute(
            'SELECT * FROM communication_channels WHERE id=%s', (channel_id,)
        ).fetchone()
        if (
            not ch
            or ch["channel_type"] != "voice"
            or (not _space_role(conn, ch["space_id"], u["id"]))
        ):
            raise HTTPException(403, "Not allowed")
        rows = conn.execute(
            """
            SELECT s.*,u.username,u.full_name,p.display_name
            FROM communication_call_signals s
            JOIN users u ON u.id=s.sender_user_id
            LEFT JOIN profiles p ON p.user_id=u.id
            WHERE s.channel_id=%s
            AND s.id>%s
            AND s.sender_user_id<>%s
            AND (s.recipient_user_id IS NULL
            OR s.recipient_user_id=%s)
            ORDER BY s.id
            LIMIT 200
            """,
            (channel_id, after, u["id"], u["id"]),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["sender_name"] = r["display_name"] or r["full_name"]
            try:
                d["payload"] = json.loads(r["payload"] or "{}")
            except:
                d["payload"] = {}
            out.append(d)
        return {"signals": out}


@app.get("/api/communication/meetings")
@logged_operation
def communication_meetings(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT m.*
            FROM communication_meetings m
            LEFT JOIN communication_meeting_participants p ON p.meeting_id=m.id
            WHERE m.host_user_id=%s
            OR p.user_id=%s
            ORDER BY m.id DESC
            LIMIT 100
            """,
            (u["id"], u["id"]),
        ).fetchall()
        return {"meetings": [dict(r) for r in rows]}


@app.post("/api/communication/meetings", status_code=201)
@logged_operation(level=logging.INFO)
def create_communication_meeting(
    payload: CommunicationMeetingCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        if payload.channel_id:
            selected_channel = conn.execute(
                'SELECT space_id FROM communication_channels WHERE id=%s',
                (payload.channel_id,),
            ).fetchone()
            if not selected_channel or not _space_role(
                conn, selected_channel["space_id"], u["id"]
            ):
                raise HTTPException(403, "Channel membership required")
        if payload.space_id and (not _space_role(conn, payload.space_id, u["id"])):
            raise HTTPException(403, "Not a member of that communication space")
        if payload.channel_id:
            ch = conn.execute(
                'SELECT * FROM communication_channels WHERE id=%s', (payload.channel_id,)
            ).fetchone()
            if not ch or (payload.space_id and ch["space_id"] != payload.space_id):
                raise HTTPException(400, "Invalid meeting channel")
        code = secrets.token_urlsafe(7).replace("-", "").replace("_", "")[:10].upper()
        cur = conn.execute(
            """
            INSERT INTO communication_meetings(host_user_id,space_id,channel_id,title,description,meeting_code,scheduled_at,duration_minutes,waiting_room,status,created_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,'scheduled',%s) RETURNING id""",
            (
                u["id"],
                payload.space_id,
                payload.channel_id,
                payload.title.strip(),
                payload.description.strip(),
                code,
                payload.scheduled_at.strip(),
                payload.duration_minutes,
                1 if payload.waiting_room else 0,
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        mid = cur_id
        conn.execute(
            """
            INSERT INTO communication_meeting_participants(meeting_id,user_id,role,state,joined_at,admitted_at)
            VALUES(%s,%s,'host','joined',%s,%s)
            """,
            (mid, u["id"], now, now),
        )
        return {"id": mid, "meeting_code": code}


@app.post("/api/communication/meetings/join/{meeting_code}")
@logged_operation(level=logging.INFO)
def join_communication_meeting(
    meeting_code: str, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        m = conn.execute(
            'SELECT * FROM communication_meetings WHERE upper(meeting_code)=upper(%s)',
            (meeting_code.strip(),),
        ).fetchone()
        if not m:
            raise HTTPException(404, "Meeting not found")
        previous = conn.execute(
            'SELECT state FROM communication_meeting_participants WHERE meeting_id=%s AND user_id=%s',
            (m["id"], u["id"]),
        ).fetchone()
        if previous and previous["state"] == "removed":
            raise HTTPException(403, "You were removed from this meeting")
        if m["space_id"] and (not _space_role(conn, m["space_id"], u["id"])):
            raise HTTPException(403, "Space membership required")
        if conn.execute(
            "SELECT 1 FROM communication_direct_calls WHERE meeting_id=%s AND status='ringing'",
            (m["id"],),
        ).fetchone():
            raise HTTPException(409, "Accept the direct call before joining")
        if m["status"] == "ended":
            raise HTTPException(409, "Meeting has ended")
        state = (
            "joined"
            if m["host_user_id"] == u["id"] or not m["waiting_room"]
            else "waiting"
        )
        role = "host" if m["host_user_id"] == u["id"] else "participant"
        conn.execute(
            """
            INSERT INTO communication_meeting_participants(meeting_id,user_id,role,state,joined_at,admitted_at,left_at)
            VALUES(%s,%s,%s,%s,%s,%s,NULL)
            ON CONFLICT(meeting_id,user_id) DO UPDATE
            SET state=excluded.state,joined_at=excluded.joined_at,left_at=NULL,admitted_at=excluded.admitted_at
            """,
            (m["id"], u["id"], role, state, now, now if state == "joined" else None),
        )
        return {"meeting_id": m["id"], "state": state}


@app.get("/api/communication/meetings/{meeting_id}")
@logged_operation
def communication_meeting_detail(
    meeting_id: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        m, p = _meeting_access(conn, meeting_id, u["id"])
        if m["host_user_id"] != u["id"] and (not p):
            raise HTTPException(403, "Join this meeting first")
        people = conn.execute(
            """
            SELECT mp.*,u.username,u.full_name,pr.display_name
            FROM communication_meeting_participants mp
            JOIN users u ON u.id=mp.user_id
            LEFT JOIN profiles pr ON pr.user_id=u.id
            WHERE mp.meeting_id=%s
            ORDER BY mp.role,mp.joined_at
            """,
            (meeting_id,),
        ).fetchall()
        return {
            "meeting": dict(m),
            "me": dict(p) if p else None,
            "participants": [dict(x) for x in people],
        }


@app.post("/api/communication/meetings/{meeting_id}/actions")
@logged_operation(level=logging.INFO)
def communication_meeting_action(
    meeting_id: int,
    payload: CommunicationMeetingParticipantAction,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        m, p, hostish = _meeting_hostish(conn, meeting_id, u["id"])
        a = payload.action
        if a == "raise_hand":
            if not p:
                raise HTTPException(403, "Not a participant")
            conn.execute(
                'UPDATE communication_meeting_participants SET hand_raised=1 WHERE meeting_id=%s AND user_id=%s',
                (meeting_id, u["id"]),
            )
        elif a == "lower_hand":
            target = payload.user_id or u["id"]
            if target != u["id"] and (not hostish):
                raise HTTPException(403, "Not allowed")
            conn.execute(
                'UPDATE communication_meeting_participants SET hand_raised=0 WHERE meeting_id=%s AND user_id=%s',
                (meeting_id, target),
            )
        elif a in {"admit", "make_cohost", "remove", "mute"}:
            if not hostish or not payload.user_id:
                raise HTTPException(403, "Host/co-host permission required")
            if a == "admit":
                conn.execute(
                    """
                    UPDATE communication_meeting_participants
                    SET state='joined',admitted_at=%s
                    WHERE meeting_id=%s
                    AND user_id=%s
                    """,
                    (now, meeting_id, payload.user_id),
                )
            elif a == "make_cohost":
                conn.execute(
                    "UPDATE communication_meeting_participants SET role='cohost' WHERE meeting_id=%s AND user_id=%s",
                    (meeting_id, payload.user_id),
                )
            elif a == "remove":
                conn.execute(
                    """
                    UPDATE communication_meeting_participants
                    SET state='removed',left_at=%s
                    WHERE meeting_id=%s
                    AND user_id=%s
                    """,
                    (now, meeting_id, payload.user_id),
                )
            elif a == "mute":
                conn.execute(
                    'UPDATE communication_meeting_participants SET microphone_enabled=0 WHERE meeting_id=%s AND user_id=%s',
                    (meeting_id, payload.user_id),
                )
        elif a == "leave":
            conn.execute(
                """
                UPDATE communication_meeting_participants
                SET state='left',left_at=%s
                WHERE meeting_id=%s
                AND user_id=%s
                """,
                (now, meeting_id, u["id"]),
            )
        elif a == "end":
            if m["host_user_id"] != u["id"]:
                raise HTTPException(403, "Only the host can end the meeting")
            conn.execute(
                "UPDATE communication_meetings SET status='ended',ended_at=%s WHERE id=%s",
                (now, meeting_id),
            )
        else:
            raise HTTPException(400, "Invalid meeting action")
        return {"ok": True}


@app.get("/api/communication/meetings/{meeting_id}/chat")
@logged_operation
def communication_meeting_chat(
    meeting_id: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        m, p = _meeting_access(conn, meeting_id, u["id"])
        if m["host_user_id"] != u["id"] and (not p or p["state"] != "joined"):
            raise HTTPException(403, "Not admitted to meeting")
        rows = conn.execute(
            """
            SELECT c.*,u.username,u.full_name,pr.display_name
            FROM communication_meeting_chat c
            JOIN users u ON u.id=c.author_user_id
            LEFT JOIN profiles pr ON pr.user_id=u.id
            WHERE c.meeting_id=%s
            ORDER BY c.id DESC
            LIMIT 200
            """,
            (meeting_id,),
        ).fetchall()
        return {"messages": [dict(x) for x in reversed(rows)]}


@app.post("/api/communication/meetings/{meeting_id}/chat", status_code=201)
@logged_operation(level=logging.INFO)
def communication_meeting_chat_send(
    meeting_id: int,
    payload: CommunicationMeetingChatCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    body = payload.body.strip()
    with db() as conn:
        m, p = _meeting_access(conn, meeting_id, u["id"])
        if m["host_user_id"] != u["id"] and (not p or p["state"] != "joined"):
            raise HTTPException(403, "Not admitted to meeting")
        cur = conn.execute(
            'INSERT INTO communication_meeting_chat(meeting_id,author_user_id,body,created_at) VALUES(%s,%s,%s,%s) RETURNING id',
            (meeting_id, u["id"], body, datetime.now(timezone.utc).isoformat()),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id}


@app.post("/api/communication/meetings/{meeting_id}/signals", status_code=201)
@logged_operation(level=logging.INFO)
def communication_meeting_send_signal(
    meeting_id: int,
    payload: CommunicationSignalCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    allowed = {
        "join",
        "leave",
        "offer",
        "answer",
        "ice",
        "camera",
        "microphone",
        "screen",
    }
    if payload.signal_type not in allowed:
        raise HTTPException(400, "Invalid meeting signal")
    with db() as conn:
        m, p = _meeting_access(conn, meeting_id, u["id"])
        if m["status"] == "ended":
            raise HTTPException(409, "Meeting has ended")
        if m["host_user_id"] != u["id"] and (not p or p["state"] != "joined"):
            raise HTTPException(
                403, "You must be admitted before joining meeting media"
            )
        if payload.recipient_user_id:
            rp = conn.execute(
                'SELECT state FROM communication_meeting_participants WHERE meeting_id=%s AND user_id=%s',
                (meeting_id, payload.recipient_user_id),
            ).fetchone()
            if not rp or rp["state"] != "joined":
                raise HTTPException(400, "Recipient is not an admitted participant")
        cur = conn.execute(
            """
            INSERT INTO communication_meeting_signals(meeting_id,sender_user_id,recipient_user_id,signal_type,payload,created_at)
            VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                meeting_id,
                u["id"],
                payload.recipient_user_id,
                payload.signal_type,
                json.dumps(payload.payload),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id}


@app.get("/api/communication/meetings/{meeting_id}/signals")
@logged_operation
def communication_meeting_get_signals(
    meeting_id: int, after: int = 0, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        m, p = _meeting_access(conn, meeting_id, u["id"])
        if m["host_user_id"] != u["id"] and (not p or p["state"] != "joined"):
            raise HTTPException(403, "Not admitted to meeting")
        rows = conn.execute(
            """
            SELECT s.*,u.username,u.full_name,pr.display_name
            FROM communication_meeting_signals s
            JOIN users u ON u.id=s.sender_user_id
            LEFT JOIN profiles pr ON pr.user_id=u.id
            WHERE s.meeting_id=%s
            AND s.id>%s
            AND s.sender_user_id<>%s
            AND (s.recipient_user_id IS NULL
            OR s.recipient_user_id=%s)
            ORDER BY s.id
            LIMIT 300
            """,
            (meeting_id, after, u["id"], u["id"]),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["sender_name"] = r["display_name"] or r["full_name"]
            try:
                d["payload"] = json.loads(r["payload"] or "{}")
            except:
                d["payload"] = {}
            out.append(d)
        return {"signals": out}


@app.get("/api/communication/livekit/status")
@logged_operation
def communication_livekit_status(authorization: Optional[str] = Header(default=None)):
    current_user(authorization)
    configured = bool(
        LIVEKIT_URL and LIVEKIT_API_KEY and LIVEKIT_API_SECRET and livekit_api
    )
    return {
        "configured": configured,
        "server_url": LIVEKIT_URL if configured else "",
        "sdk_installed": bool(livekit_api),
        "mode": "livekit-sfu" if configured else "phase4-peer-to-peer-fallback",
    }


@app.post("/api/communication/livekit/token")
@logged_operation(level=logging.INFO)
def communication_livekit_token(
    payload: LiveKitTokenRequest, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    if not livekit_api:
        raise HTTPException(
            503, "LiveKit Python SDK is not installed. Install livekit-api."
        )
    if not (LIVEKIT_URL and LIVEKIT_API_KEY and LIVEKIT_API_SECRET):
        raise HTTPException(503, "LiveKit environment variables are not configured")
    with db() as conn:
        m, p = _meeting_access(conn, payload.meeting_id, u["id"])
        if m["status"] == "ended":
            raise HTTPException(409, "Meeting has ended")
        if m["host_user_id"] != u["id"] and (not p or p["state"] != "joined"):
            raise HTTPException(
                403, "You must be admitted before joining meeting media"
            )
        room_name = f"noveam-meeting-{m['id']}"
        participant_identity = f"noveam-user-{u['id']}-{uuid.uuid4().hex[:8]}"
        display_name = u["full_name"] or u["username"]
        token = (
            livekit_api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
            .with_ttl(timedelta(minutes=10))
            .with_identity(participant_identity)
            .with_name(display_name)
            .with_grants(
                livekit_api.VideoGrants(
                    room_join=True,
                    room=room_name,
                    can_publish=True,
                    can_subscribe=True,
                    can_publish_data=True,
                )
            )
            .to_jwt()
        )
        return {
            "server_url": LIVEKIT_URL,
            "participant_token": token,
            "room_name": room_name,
            "participant_identity": participant_identity,
        }


@app.get("/api/communication/settings")
@logged_operation
def get_communication_settings(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        return dict(_communication_settings_row(conn, u["id"]))


@app.put("/api/communication/settings")
@logged_operation(level=logging.INFO)
def update_communication_settings(
    payload: CommunicationSettingsUpdate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    if payload.allow_direct_calls not in {"everyone", "connections", "nobody"}:
        raise HTTPException(400, "Invalid direct-call privacy")
    with db() as conn:
        _communication_settings_row(conn, u["id"])
        conn.execute(
            """
            UPDATE communication_settings
            SET allow_direct_calls=%s,allow_video_calls=%s,incoming_call_notifications=%s, message_notifications=%s,read_receipts=%s,typing_indicators=%s,online_status=%s,auto_download_media=%s, default_microphone=%s,default_camera=%s,default_speaker=%s,updated_at=%s
            WHERE user_id=%s
            """,
            (
                payload.allow_direct_calls,
                int(payload.allow_video_calls),
                int(payload.incoming_call_notifications),
                int(payload.message_notifications),
                int(payload.read_receipts),
                int(payload.typing_indicators),
                int(payload.online_status),
                int(payload.auto_download_media),
                payload.default_microphone,
                payload.default_camera,
                payload.default_speaker,
                datetime.now(timezone.utc).isoformat(),
                u["id"],
            ),
        )
        return dict(_communication_settings_row(conn, u["id"]))


@app.post("/api/communication/direct-calls", status_code=201)
@logged_operation(level=logging.INFO)
def create_direct_call(
    payload: DirectCallCreate, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    if payload.call_type not in {"audio", "video"}:
        raise HTTPException(400, "Call type must be audio or video")
    if payload.callee_user_id == u["id"]:
        raise HTTPException(400, "You cannot call yourself")
    with db() as conn:
        _require_unblocked(conn, u["id"], payload.callee_user_id)
        target = conn.execute(
            'SELECT id FROM users WHERE id=%s', (payload.callee_user_id,)
        ).fetchone()
        if not target:
            raise HTTPException(404, "User not found")
        settings = _communication_settings_row(conn, payload.callee_user_id)
        policy = settings["allow_direct_calls"]
        if policy == "nobody":
            raise HTTPException(403, "This user is not accepting direct calls")
        if policy == "connections" and (
            not are_connected(conn, u["id"], payload.callee_user_id)
        ):
            raise HTTPException(403, "This user accepts calls only from connections")
        if payload.call_type == "video" and (not settings["allow_video_calls"]):
            raise HTTPException(403, "This user is not accepting video calls")
        now = datetime.now(timezone.utc).isoformat()
        title = ("Video" if payload.call_type == "video" else "Audio") + " call"
        code = secrets.token_urlsafe(7).replace("-", "").replace("_", "")[:10].upper()
        mcur = conn.execute(
            """
            INSERT INTO communication_meetings(host_user_id,title,description,meeting_code,scheduled_at,duration_minutes,waiting_room,status,created_at)
            VALUES(%s,%s,%s,%s,'',60,0,'scheduled',%s) RETURNING id""",
            (u["id"], title, "Direct NOVEAM call", code, now),
        )
        mcur_id = mcur.fetchone()["id"]
        mid = mcur_id
        conn.execute(
            """
            INSERT INTO communication_meeting_participants(meeting_id,user_id,role,state,joined_at,admitted_at)
            VALUES(%s,%s,'host','joined',%s,%s)
            """,
            (mid, u["id"], now, now),
        )
        conn.execute(
            """
            INSERT INTO communication_meeting_participants(meeting_id,user_id,role,state,joined_at,admitted_at)
            VALUES(%s,%s,'participant','waiting',%s,%s)
            """,
            (mid, payload.callee_user_id, now, now),
        )
        cur = conn.execute(
            """
            INSERT INTO communication_direct_calls(caller_user_id,callee_user_id,call_type,status,meeting_id,created_at)
            VALUES(%s,%s,%s,'ringing',%s,%s) RETURNING id""",
            (u["id"], payload.callee_user_id, payload.call_type, mid, now),
        )
        cur_id = cur.fetchone()["id"]
        if settings["incoming_call_notifications"]:
            create_notification(
                conn,
                payload.callee_user_id,
                u["id"],
                "INCOMING_CALL",
                "Incoming " + payload.call_type + " call",
                "is calling you",
                "meeting",
                mid,
            )
        return {
            "call_id": cur_id,
            "meeting_id": mid,
            "meeting_code": code,
            "call_type": payload.call_type,
        }


@app.get("/api/communication/direct-calls/incoming")
@logged_operation
def incoming_direct_calls(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.*,u.username,u.full_name,p.display_name
            FROM communication_direct_calls c
            JOIN users u ON u.id=c.caller_user_id
            LEFT JOIN profiles p ON p.user_id=u.id
            WHERE c.callee_user_id=%s
            AND c.status='ringing'
            ORDER BY c.id DESC
            LIMIT 20
            """,
            (u["id"],),
        ).fetchall()
        return {"calls": [dict(x) for x in rows]}


@app.post("/api/communication/direct-calls/{call_id}/action")
@logged_operation(level=logging.INFO)
def direct_call_action(
    call_id: int,
    payload: DirectCallAction,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        c = conn.execute(
            'SELECT * FROM communication_direct_calls WHERE id=%s', (call_id,)
        ).fetchone()
        if not c or u["id"] not in (c["caller_user_id"], c["callee_user_id"]):
            raise HTTPException(404, "Call not found")
        if c["status"] not in {"ringing", "active"}:
            raise HTTPException(409, "Call is no longer active")
        if payload.action == "accept":
            if u["id"] != c["callee_user_id"]:
                raise HTTPException(403, "Only the recipient can accept")
            if c["status"] != "ringing":
                raise HTTPException(409, "Call has already been answered")
            _require_unblocked(conn, c["caller_user_id"], c["callee_user_id"])
            conn.execute(
                "UPDATE communication_direct_calls SET status='active',answered_at=%s WHERE id=%s",
                (now, call_id),
            )
            conn.execute(
                """
                UPDATE communication_meeting_participants
                SET state='joined',admitted_at=%s
                WHERE meeting_id=%s
                AND user_id=%s
                """,
                (now, c["meeting_id"], u["id"]),
            )
        elif payload.action in {"decline", "end"}:
            if payload.action == "decline" and (
                u["id"] != c["callee_user_id"] or c["status"] != "ringing"
            ):
                raise HTTPException(403, "Only the ringing-call recipient can decline")
            conn.execute(
                'UPDATE communication_direct_calls SET status=%s,ended_at=%s WHERE id=%s',
                ("declined" if payload.action == "decline" else "ended", now, call_id),
            )
            conn.execute(
                "UPDATE communication_meetings SET status='ended',ended_at=%s WHERE id=%s",
                (now, c["meeting_id"]),
            )
        else:
            raise HTTPException(400, "Invalid call action")
        return {
            "ok": True,
            "meeting_id": c["meeting_id"],
            "status": "active" if payload.action == "accept" else payload.action,
        }


@app.post("/api/communication/realtime/fanout")
@logged_operation(level=logging.INFO)
async def communication_realtime_fanout(
    payload: RealtimeMessageEvent, authorization: Optional[str] = Header(default=None)
):

    def prepare():
        u, _ = current_user(authorization)
        if payload.event not in {"message", "read", "deleted", "edited", "reaction"}:
            raise HTTPException(400, "Invalid realtime event")
        with db() as conn:
            if not are_connected(conn, u["id"], payload.peer_id):
                raise HTTPException(403, "Accepted connection required")
            _require_unblocked(conn, u["id"], payload.peer_id)
            if payload.message_id is not None:
                message = _private_message_access(conn, payload.message_id, u["id"])
                if payload.peer_id not in (
                    message["sender_user_id"],
                    message["receiver_user_id"],
                ):
                    raise HTTPException(403, "Invalid message peer")
            if payload.event == "read" and (
                not _communication_settings_row(conn, u["id"])["read_receipts"]
            ):
                return (u, False)
        return (u, True)

    u, allowed = await run_in_threadpool(prepare)
    if allowed:
        await realtime_hub.send(
            payload.peer_id,
            {
                "type": payload.event,
                "user_id": u["id"],
                "message_id": payload.message_id,
            },
        )
    return {"ok": True}


@app.get("/api/communication/presence/{user_id}")
@logged_operation
def communication_realtime_presence(
    user_id: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        if user_id != u["id"] and (not are_connected(conn, u["id"], user_id)):
            raise HTTPException(403, "Accepted connection required")
        visible = bool(_communication_settings_row(conn, user_id)["online_status"])
    return {
        "user_id": user_id,
        "online": bool(visible and realtime_hub.online(user_id)),
    }


@app.put("/api/messages/message/{message_id}/edit")
@logged_operation(level=logging.INFO)
async def edit_private_message(
    message_id: int,
    payload: MessageEditRequest,
    authorization: Optional[str] = Header(default=None),
):

    def update():
        u, _ = current_user(authorization)
        with db() as conn:
            message = _private_message_access(conn, message_id, u["id"])
            if message["sender_user_id"] != u["id"]:
                raise HTTPException(403, "Only the sender can edit this message")
            _require_unblocked(
                conn, message["sender_user_id"], message["receiver_user_id"]
            )
            conn.execute(
                'UPDATE private_messages SET body=%s WHERE id=%s',
                (payload.body.strip(), message_id),
            )
        return (u["id"], message["receiver_user_id"])

    uid, peer = await run_in_threadpool(update)
    await realtime_hub.send(
        peer, {"type": "edited", "user_id": uid, "message_id": message_id}
    )
    return {"ok": True, "edited_at": datetime.now(timezone.utc).isoformat()}


@app.post("/api/messages/message/{message_id}/reaction")
@logged_operation(level=logging.INFO)
async def react_private_message(
    message_id: int,
    payload: MessageReactionRequest,
    authorization: Optional[str] = Header(default=None),
):

    def update():
        u, _ = current_user(authorization)
        with db() as conn:
            message = _private_message_access(conn, message_id, u["id"])
            _require_unblocked(
                conn, message["sender_user_id"], message["receiver_user_id"]
            )
            exists = conn.execute(
                'SELECT 1 FROM private_message_reactions WHERE message_id=%s AND user_id=%s AND emoji=%s',
                (message_id, u["id"], payload.emoji),
            ).fetchone()
            if exists:
                conn.execute(
                    'DELETE FROM private_message_reactions WHERE message_id=%s AND user_id=%s AND emoji=%s',
                    (message_id, u["id"], payload.emoji),
                )
            else:
                conn.execute(
                    'INSERT INTO private_message_reactions(message_id,user_id,emoji,created_at) VALUES(%s,%s,%s,%s)',
                    (
                        message_id,
                        u["id"],
                        payload.emoji,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
        peer = (
            message["receiver_user_id"]
            if message["sender_user_id"] == u["id"]
            else message["sender_user_id"]
        )
        return (u["id"], peer, not bool(exists))

    uid, peer, active = await run_in_threadpool(update)
    await realtime_hub.send(
        peer, {"type": "reaction", "user_id": uid, "message_id": message_id}
    )
    return {"ok": True, "active": active}


@app.post("/api/messages/message/{message_id}/save")
@logged_operation(level=logging.INFO)
def save_private_message(
    message_id: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _private_message_access(conn, message_id, u["id"])
        exists = conn.execute(
            'SELECT 1 FROM private_message_saved WHERE message_id=%s AND user_id=%s',
            (message_id, u["id"]),
        ).fetchone()
        if exists:
            conn.execute(
                'DELETE FROM private_message_saved WHERE message_id=%s AND user_id=%s',
                (message_id, u["id"]),
            )
        else:
            conn.execute(
                'INSERT INTO private_message_saved(message_id,user_id,created_at) VALUES(%s,%s,%s)',
                (message_id, u["id"], now),
            )
    return {"ok": True, "saved": not bool(exists)}


@app.get("/api/messages/search/{peer_user_id}")
@logged_operation
def search_private_messages(
    peer_user_id: int, q: str = "", authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    q = q.strip()
    if len(q) < 2:
        return {"results": []}
    with db() as conn:
        _require_unblocked(conn, u["id"], peer_user_id)
        if not are_connected(conn, u["id"], peer_user_id):
            raise HTTPException(403, "Accepted connection required")
        rows = conn.execute(
            """
            SELECT *
            FROM private_messages
            WHERE ((sender_user_id=%s
            AND receiver_user_id=%s)
            OR (sender_user_id=%s
            AND receiver_user_id=%s))
            AND NOT ((sender_user_id=%s
            AND deleted_by_sender=1)
            OR (receiver_user_id=%s
            AND deleted_by_receiver=1))
            AND body ILIKE %s
            ORDER BY id DESC
            LIMIT 100
            """,
            (u["id"], peer_user_id, peer_user_id, u["id"], u["id"], u["id"], f"%{q}%"),
        ).fetchall()
    return {"results": [dict(x) for x in rows]}


@app.get("/api/communication/call-history")
@logged_operation
def communication_call_history(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.*, CASE WHEN c.caller_user_id=%s THEN c.callee_user_id ELSE c.caller_user_id END peer_user_id, CASE WHEN c.caller_user_id=%s THEN 'outgoing' ELSE 'incoming' END direction
            FROM communication_direct_calls c
            WHERE c.caller_user_id=%s
            OR c.callee_user_id=%s
            ORDER BY c.id DESC
            LIMIT 100
            """,
            (u["id"], u["id"], u["id"], u["id"]),
        ).fetchall()
    return {"calls": [dict(x) for x in rows]}


@app.get("/api/communication/conversation-controls/{peer_user_id}")
@logged_operation
def get_conversation_controls(
    peer_user_id: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        r = conn.execute(
            'SELECT * FROM conversation_controls WHERE user_id=%s AND peer_user_id=%s',
            (u["id"], peer_user_id),
        ).fetchone()
    return (
        dict(r)
        if r
        else {
            "user_id": u["id"],
            "peer_user_id": peer_user_id,
            "muted": 0,
            "blocked": 0,
        }
    )


@app.post("/api/communication/conversation-controls/{peer_user_id}")
@logged_operation(level=logging.INFO)
def set_conversation_controls(
    peer_user_id: int,
    payload: ConversationControlRequest,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    if payload.action not in {"mute", "unmute", "block", "unblock"}:
        raise HTTPException(400, "Invalid action")
    with db() as conn:
        conn.execute(
            'INSERT INTO conversation_controls(user_id,peer_user_id,updated_at) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
            (u["id"], peer_user_id, now),
        )
        if payload.action in {"mute", "unmute"}:
            conn.execute(
                'UPDATE conversation_controls SET muted=%s,updated_at=%s WHERE user_id=%s AND peer_user_id=%s',
                (1 if payload.action == "mute" else 0, now, u["id"], peer_user_id),
            )
        else:
            conn.execute(
                'UPDATE conversation_controls SET blocked=%s,updated_at=%s WHERE user_id=%s AND peer_user_id=%s',
                (1 if payload.action == "block" else 0, now, u["id"], peer_user_id),
            )
    return {"ok": True}


@app.post("/api/communication/report/{peer_user_id}", status_code=201)
@logged_operation(level=logging.INFO)
def report_communication_user(
    peer_user_id: int,
    payload: CommunicationReportRequest,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    if peer_user_id == u["id"]:
        raise HTTPException(400, "Invalid user")
    with db() as conn:
        conn.execute(
            """
            INSERT INTO communication_reports(reporter_user_id,reported_user_id,reason,created_at)
            VALUES(%s,%s,%s,%s)
            """,
            (u["id"], peer_user_id, payload.reason.strip(), now),
        )
    return {"ok": True}


@app.post("/api/communication/meetings/{meeting_id}/reaction", status_code=201)
@logged_operation(level=logging.INFO)
def meeting_reaction(
    meeting_id: int,
    payload: MeetingReactionRequest,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _meeting_member(conn, meeting_id, u["id"])
        conn.execute(
            'INSERT INTO communication_meeting_reactions(meeting_id,user_id,emoji,created_at) VALUES(%s,%s,%s,%s)',
            (meeting_id, u["id"], payload.emoji, now),
        )
    return {"ok": True, "emoji": payload.emoji, "user_id": u["id"], "created_at": now}


@app.get("/api/communication/meetings/{meeting_id}/reactions")
@logged_operation
def meeting_reactions(
    meeting_id: int,
    after_id: int = 0,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    with db() as conn:
        _meeting_member(conn, meeting_id, u["id"])
        rows = conn.execute(
            'SELECT * FROM communication_meeting_reactions WHERE meeting_id=%s AND id>%s ORDER BY id LIMIT 100',
            (meeting_id, after_id),
        ).fetchall()
    return {"reactions": [dict(x) for x in rows]}


@app.post("/api/communication/meetings/{meeting_id}/polls", status_code=201)
@logged_operation(level=logging.INFO)
def create_meeting_poll(
    meeting_id: int,
    payload: MeetingPollCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    opts = [str(x).strip() for x in payload.options if str(x).strip()]
    if len(opts) < 2 or len(opts) > 10:
        raise HTTPException(400, "Poll requires 2 to 10 options")
    with db() as conn:
        _meeting_member(conn, meeting_id, u["id"])
        cur = conn.execute(
            """
            INSERT INTO communication_meeting_polls(meeting_id,creator_user_id,question,options_json,created_at)
            VALUES(%s,%s,%s,%s,%s) RETURNING id""",
            (meeting_id, u["id"], payload.question.strip(), json.dumps(opts), now),
        )
        cur_id = cur.fetchone()["id"]
        pid = cur_id
    return {"id": pid, "question": payload.question.strip(), "options": opts}


@app.get("/api/communication/meetings/{meeting_id}/polls")
@logged_operation
def list_meeting_polls(
    meeting_id: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        _meeting_member(conn, meeting_id, u["id"])
        rows = conn.execute(
            'SELECT * FROM communication_meeting_polls WHERE meeting_id=%s ORDER BY id DESC',
            (meeting_id,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["options"] = json.loads(d.pop("options_json"))
            votes = conn.execute(
                """
                SELECT option_index,COUNT(*) n
                FROM communication_meeting_poll_votes
                WHERE poll_id=%s
                GROUP BY option_index
                """,
                (r["id"],),
            ).fetchall()
            d["votes"] = {str(v["option_index"]): v["n"] for v in votes}
            out.append(d)
    return {"polls": out}


@app.post("/api/communication/meetings/polls/{poll_id}/vote")
@logged_operation(level=logging.INFO)
def vote_meeting_poll(
    poll_id: int,
    payload: MeetingPollVote,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        p = conn.execute(
            'SELECT * FROM communication_meeting_polls WHERE id=%s', (poll_id,)
        ).fetchone()
        if not p:
            raise HTTPException(404, "Poll not found")
        _meeting_member(conn, p["meeting_id"], u["id"])
        opts = json.loads(p["options_json"])
        if payload.option_index < 0 or payload.option_index >= len(opts):
            raise HTTPException(400, "Invalid option")
        conn.execute(
            """
            INSERT INTO communication_meeting_poll_votes(poll_id,user_id,option_index,created_at)
            VALUES(%s,%s,%s,%s)
            ON CONFLICT(poll_id,user_id) DO UPDATE
            SET option_index=excluded.option_index,created_at=excluded.created_at
            """,
            (poll_id, u["id"], payload.option_index, now),
        )
    return {"ok": True}


@app.post("/api/communication/meetings/{meeting_id}/invite", status_code=201)
@logged_operation(level=logging.INFO)
def invite_to_meeting(
    meeting_id: int,
    payload: MeetingInviteCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _ = _meeting_member(conn, meeting_id, u["id"])
        meeting = conn.execute(
            'SELECT * FROM communication_meetings WHERE id=%s', (meeting_id,)
        ).fetchone()
        if not meeting:
            raise HTTPException(404, "Meeting not found")
        if payload.invitee_user_id == u["id"]:
            raise HTTPException(400, "Cannot invite yourself")
        target = conn.execute(
            'SELECT id FROM users WHERE id=%s', (payload.invitee_user_id,)
        ).fetchone()
        if not target:
            raise HTTPException(404, "User not found")
        cur = conn.execute(
            """
            INSERT INTO communication_meeting_invites(meeting_id,inviter_user_id,invitee_user_id,status,created_at)
            VALUES(%s,%s,%s,%s,%s) RETURNING id""",
            (meeting_id, u["id"], payload.invitee_user_id, "pending", now),
        )
        cur_id = cur.fetchone()["id"]
        try:
            create_notification(
                conn,
                payload.invitee_user_id,
                u["id"],
                "MEETING_INVITE",
                "Meeting invitation",
                "You were invited to a meeting",
                "meeting",
                meeting_id,
            )
        except Exception:
            safe_log(logging.WARNING, "meeting_invite", "notification_failed")
    return {"ok": True, "invite_id": cur_id}


@app.get("/api/communication/meeting-invites")
@logged_operation
def my_meeting_invites(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT i.*,m.title,m.description,m.meeting_code,m.scheduled_at,m.status AS meeting_status
            FROM communication_meeting_invites i
            JOIN communication_meetings m ON m.id=i.meeting_id
            WHERE i.invitee_user_id=%s
            AND i.status='pending'
            ORDER BY i.id DESC
            LIMIT 50
            """,
            (u["id"],),
        ).fetchall()
    return {"invites": [dict(x) for x in rows]}


@app.get("/api/notifications")
@logged_operation
def list_notifications(
    limit: int = 100,
    unread_only: bool = False,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    limit = max(1, min(limit, 200))
    with db() as conn:
        sql = (
            'SELECT * FROM notifications WHERE user_id=%s'
            + (" AND read_at IS NULL" if unread_only else "")
            + ' ORDER BY id DESC LIMIT %s'
        )
        rows = conn.execute(sql, (viewer["id"], limit)).fetchall()
        unread = conn.execute(
            'SELECT COUNT(*) n FROM notifications WHERE user_id=%s AND read_at IS NULL',
            (viewer["id"],),
        ).fetchone()["n"]
        items = []
        for r in rows:
            d = dict(r)
            d["actor"] = notification_actor(conn, r["actor_user_id"])
            items.append(d)
    return {"notifications": items, "unread_count": unread}


@app.post("/api/notifications/{notification_id}/read")
@logged_operation(level=logging.INFO)
def read_notification(
    notification_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        r = conn.execute(
            'SELECT id FROM notifications WHERE id=%s AND user_id=%s',
            (notification_id, viewer["id"]),
        ).fetchone()
        if not r:
            raise HTTPException(404, "Notification not found")
        conn.execute(
            'UPDATE notifications SET read_at=COALESCE(read_at,%s) WHERE id=%s',
            (datetime.now(timezone.utc).isoformat(), notification_id),
        )
    return {"ok": True}


@app.post("/api/notifications/read-all")
@logged_operation(level=logging.INFO)
def read_all_notifications(authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        conn.execute(
            'UPDATE notifications SET read_at=%s WHERE user_id=%s AND read_at IS NULL',
            (datetime.now(timezone.utc).isoformat(), viewer["id"]),
        )
    return {"ok": True}


@app.delete("/api/notifications/{notification_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_notification(
    notification_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        r = conn.execute(
            'SELECT id FROM notifications WHERE id=%s AND user_id=%s',
            (notification_id, viewer["id"]),
        ).fetchone()
        if not r:
            raise HTTPException(404, "Notification not found")
        conn.execute('DELETE FROM notifications WHERE id=%s', (notification_id,))


@app.get("/api/search")
@logged_operation
def global_search(
    q: str = "",
    category: str = "all",
    limit: int = 8,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    term = q.strip()
    if len(term) < 2:
        return {"query": term, "results": [], "counts": {}}
    limit = max(1, min(limit, 25))
    allowed = {
        "all",
        "people",
        "posts",
        "pages",
        "groups",
        "jobs",
        "portfolio",
        "education",
        "events",
    }
    if category not in allowed:
        raise HTTPException(400, "Invalid search category")
    like = "%" + term + "%"
    results = []
    counts = {}
    with db() as conn:

        def add(kind, rows, mapper):
            counts[kind] = len(rows)
            results.extend((mapper(r) for r in rows))

        if category in ("all", "people"):
            rows = conn.execute(
                """
                SELECT u.id,u.username,u.full_name,u.role, p.display_name,p.headline,p.photo
                FROM users u
                LEFT JOIN profiles p ON p.user_id=u.id
                WHERE u.id<>%s
                AND ( u.username ILIKE %s
                OR u.full_name ILIKE %s
                OR p.display_name ILIKE %s
                OR p.headline ILIKE %s
                OR p.skills ILIKE %s
                OR p.occupation ILIKE %s
                OR p.organization ILIKE %s )
                ORDER BY COALESCE(p.display_name,u.full_name)
                LIMIT %s
                """,
                (viewer["id"], like, like, like, like, like, like, like, limit),
            ).fetchall()
            add(
                "people",
                rows,
                lambda r: {
                    "type": "people",
                    "id": r["id"],
                    "title": r["display_name"] or r["full_name"],
                    "subtitle": (
                        "@" + r["username"] + " · " + (r["headline"] or r["role"] or "")
                    ).strip(" ·"),
                    "url": "profile.html?username=" + r["username"],
                },
            )
        if category in ("all", "posts"):
            rows = conn.execute(
                """
                SELECT p.id,p.text,p.category,p.created_at, u.username,u.full_name,pr.display_name
                FROM posts p
                JOIN users u ON u.id=p.author_user_id
                LEFT JOIN profiles pr ON pr.user_id=u.id
                WHERE ( upper(p.visibility)='PUBLIC'
                OR p.author_user_id=%s )
                AND ( p.text ILIKE %s
                OR p.category ILIKE %s )
                ORDER BY p.id DESC
                LIMIT %s
                """,
                (viewer["id"], like, like, limit),
            ).fetchall()
            add(
                "posts",
                rows,
                lambda r: {
                    "type": "posts",
                    "id": r["id"],
                    "title": r["text"][:120] or "Post",
                    "subtitle": "Post by "
                    + (r["display_name"] or r["full_name"])
                    + (" · " + r["category"] if r["category"] else ""),
                    "url": "feed.html?post=" + str(r["id"]),
                },
            )
        if category in ("all", "pages"):
            rows = conn.execute(
                """
                SELECT id,name,page_type,category,location
                FROM organization_pages
                WHERE name ILIKE %s
                OR category ILIKE %s
                OR about ILIKE %s
                OR services ILIKE %s
                OR location ILIKE %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (like, like, like, like, like, limit),
            ).fetchall()
            add(
                "pages",
                rows,
                lambda r: {
                    "type": "pages",
                    "id": r["id"],
                    "title": r["name"],
                    "subtitle": " · ".join(
                        (x for x in [r["page_type"], r["category"], r["location"]] if x)
                    ),
                    "url": "pages.html?page=" + str(r["id"]),
                },
            )
        if category in ("all", "groups"):
            rows = conn.execute(
                """
                SELECT id,name,description,privacy
                FROM community_groups
                WHERE privacy='public'
                AND ( name ILIKE %s
                OR description ILIKE %s )
                ORDER BY id DESC
                LIMIT %s
                """,
                (like, like, limit),
            ).fetchall()
            add(
                "groups",
                rows,
                lambda r: {
                    "type": "groups",
                    "id": r["id"],
                    "title": r["name"],
                    "subtitle": r["description"][:140] or "Public group",
                    "url": "groups.html?group=" + str(r["id"]),
                },
            )
        if category in ("all", "jobs"):
            rows = conn.execute(
                """
                SELECT id,title,opportunity_type, work_mode,location,skills
                FROM jobs
                WHERE status='open'
                AND ( title ILIKE %s
                OR description ILIKE %s
                OR skills ILIKE %s
                OR location ILIKE %s )
                ORDER BY id DESC
                LIMIT %s
                """,
                (like, like, like, like, limit),
            ).fetchall()
            add(
                "jobs",
                rows,
                lambda r: {
                    "type": "jobs",
                    "id": r["id"],
                    "title": r["title"],
                    "subtitle": " · ".join(
                        (
                            x
                            for x in [
                                r["opportunity_type"],
                                r["work_mode"],
                                r["location"],
                            ]
                            if x
                        )
                    ),
                    "url": "jobs.html?job=" + str(r["id"]),
                },
            )
        if category in ("all", "portfolio"):
            rows = conn.execute(
                """
                SELECT pp.id,pp.title,pp.category,pp.skills, u.username,u.full_name
                FROM portfolio_projects pp
                JOIN users u ON u.id=pp.user_id
                WHERE ( pp.visibility='Public'
                OR pp.user_id=%s )
                AND ( pp.title ILIKE %s
                OR pp.description ILIKE %s
                OR pp.skills ILIKE %s
                OR pp.tools ILIKE %s )
                ORDER BY pp.id DESC
                LIMIT %s
                """,
                (viewer["id"], like, like, like, like, limit),
            ).fetchall()
            add(
                "portfolio",
                rows,
                lambda r: {
                    "type": "portfolio",
                    "id": r["id"],
                    "title": r["title"],
                    "subtitle": "Portfolio · "
                    + r["full_name"]
                    + (" · " + r["category"] if r["category"] else ""),
                    "url": "portfolio.html?username=" + r["username"],
                },
            )
        if category in ("all", "education"):
            rows = conn.execute(
                """
                SELECT id,title,provider,instructor,skills
                FROM learning_courses
                WHERE visibility='Public'
                AND ( title ILIKE %s
                OR provider ILIKE %s
                OR instructor ILIKE %s
                OR description ILIKE %s
                OR skills ILIKE %s )
                ORDER BY id DESC
                LIMIT %s
                """,
                (like, like, like, like, like, limit),
            ).fetchall()
            add(
                "education",
                rows,
                lambda r: {
                    "type": "education",
                    "id": r["id"],
                    "title": r["title"],
                    "subtitle": " · ".join(
                        (x for x in [r["provider"], r["instructor"]] if x)
                    ),
                    "url": "education.html?course=" + str(r["id"]),
                },
            )
        if category in ("all", "events"):
            rows = conn.execute(
                """
                SELECT id,title,start_at,location,description
                FROM events
                WHERE visibility='public'
                AND ( title ILIKE %s
                OR description ILIKE %s
                OR location ILIKE %s )
                ORDER BY start_at DESC
                LIMIT %s
                """,
                (like, like, like, limit),
            ).fetchall()
            add(
                "events",
                rows,
                lambda r: {
                    "type": "events",
                    "id": r["id"],
                    "title": r["title"],
                    "subtitle": " · ".join(
                        (x for x in [r["start_at"], r["location"]] if x)
                    ),
                    "url": "events.html?event=" + str(r["id"]),
                },
            )
    order = {
        "people": 0,
        "posts": 1,
        "pages": 2,
        "groups": 3,
        "jobs": 4,
        "portfolio": 5,
        "education": 6,
        "events": 7,
    }
    results.sort(key=lambda x: order.get(x["type"], 99))
    return {"query": term, "category": category, "results": results, "counts": counts}


@app.get("/api/activity-summary")
@logged_operation
def activity_summary(authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        notifications_unread = conn.execute(
            'SELECT COUNT(*) n FROM notifications WHERE user_id=%s AND read_at IS NULL',
            (viewer["id"],),
        ).fetchone()["n"]
        messages_unread = conn.execute(
            """
            SELECT COUNT(*) n
            FROM private_messages
            WHERE receiver_user_id=%s
            AND read_at IS NULL
            AND deleted_by_receiver=0
            """,
            (viewer["id"],),
        ).fetchone()["n"]
    total_unread = notifications_unread + messages_unread
    return {
        "notifications_unread": notifications_unread,
        "messages_unread": messages_unread,
        "total_unread": total_unread,
    }


@app.get("/api/pages")
@logged_operation
def list_organization_pages(
    page_type: Optional[str] = None,
    mine: bool = False,
    q: str = "",
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    sql = "SELECT * FROM organization_pages WHERE 1=1"
    args = []
    if mine:
        sql += ' AND owner_user_id=%s'
        args.append(viewer["id"])
    if page_type and page_type != "All":
        sql += ' AND page_type=%s'
        args.append(page_type)
    if q.strip():
        sql += ' AND (name ILIKE %s OR category ILIKE %s OR about ILIKE %s)'
        term = "%" + q.strip() + "%"
        args += [term, term, term]
    sql += " ORDER BY id DESC LIMIT 300"
    with db() as conn:
        rows = conn.execute(sql, args).fetchall()
    pages = [serialize_org_page(r, viewer["id"]) for r in rows]
    return {"pages": pages}


@app.get("/api/pages/{page_id}")
@logged_operation
def get_organization_page(
    page_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Page not found")
        roles = conn.execute(
            'SELECT * FROM organization_page_roles WHERE page_id=%s ORDER BY id DESC',
            (page_id,),
        ).fetchall()
    result = {
        "page": serialize_org_page(row, viewer["id"]),
        "roles": [dict(r) for r in roles],
    }
    return result


@app.post("/api/pages", status_code=201)
@logged_operation(level=logging.INFO)
def create_organization_page(
    payload: OrganizationPagePayload,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO organization_pages( owner_user_id,name,page_type,category, location,about,website,contact,services, created_at,updated_at )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                viewer["id"],
                payload.name.strip(),
                payload.page_type.strip(),
                payload.category.strip(),
                payload.location.strip(),
                payload.about.strip(),
                payload.website.strip(),
                payload.contact.strip(),
                payload.services.strip(),
                now,
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM organization_pages WHERE id=%s', (cur_id,)
        ).fetchone()
    audit(viewer["id"], "page.create", f"page:{row['id']}")
    return serialize_org_page(row, viewer["id"])


@app.put("/api/pages/{page_id}")
@logged_operation(level=logging.INFO)
def update_organization_page(
    page_id: int,
    payload: OrganizationPagePayload,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Page not found")
        if row["owner_user_id"] != viewer["id"] and (
            not conn.execute(
                """
            SELECT 1
            FROM organization_page_members
            WHERE page_id=%s
            AND user_id=%s
            AND role IN ('Admin','Editor')
            """,
                (page_id, viewer["id"]),
            ).fetchone()
        ):
            raise HTTPException(
                403, "Only the Page owner, Admin or Editor can edit this Page"
            )
        conn.execute(
            """
            UPDATE organization_pages
            SET name=%s,page_type=%s,category=%s, location=%s,about=%s,website=%s, contact=%s,services=%s,updated_at=%s
            WHERE id=%s
            """,
            (
                payload.name.strip(),
                payload.page_type.strip(),
                payload.category.strip(),
                payload.location.strip(),
                payload.about.strip(),
                payload.website.strip(),
                payload.contact.strip(),
                payload.services.strip(),
                datetime.now(timezone.utc).isoformat(),
                page_id,
            ),
        )
        row = conn.execute(
            'SELECT * FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone()
    return serialize_org_page(row, viewer["id"])


@app.delete("/api/pages/{page_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_organization_page(
    page_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Page not found")
        if row["owner_user_id"] != viewer["id"]:
            raise HTTPException(403, "Only the Page owner can delete this Page")
        conn.execute('DELETE FROM organization_page_roles WHERE page_id=%s', (page_id,))
        conn.execute('DELETE FROM organization_pages WHERE id=%s', (page_id,))
    audit(viewer["id"], "page.delete", f"page:{page_id}")


@app.get("/api/pages/{page_id}/roles")
@logged_operation
def list_organization_roles(
    page_id: int, authorization: Optional[str] = Header(default=None)
):
    current_user(authorization)
    with db() as conn:
        if not conn.execute(
            'SELECT 1 FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone():
            raise HTTPException(404, "Page not found")
        rows = conn.execute(
            'SELECT * FROM organization_page_roles WHERE page_id=%s ORDER BY id DESC',
            (page_id,),
        ).fetchall()
    return {"roles": [dict(r) for r in rows]}


@app.post("/api/pages/{page_id}/roles", status_code=201)
@logged_operation(level=logging.INFO)
def add_organization_role(
    page_id: int,
    payload: OrganizationRolePayload,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        page = conn.execute(
            'SELECT * FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone()
        if not page:
            raise HTTPException(404, "Page not found")
        if page["owner_user_id"] != viewer["id"]:
            raise HTTPException(403, "Only the Page owner can manage role entries")
        cur = conn.execute(
            """
            INSERT INTO organization_page_roles( page_id,created_by_user_id,role, person,designation,contact,about,created_at )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                page_id,
                viewer["id"],
                payload.role.strip(),
                payload.person.strip(),
                payload.designation.strip(),
                payload.contact.strip(),
                payload.about.strip(),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM organization_page_roles WHERE id=%s', (cur_id,)
        ).fetchone()
    return dict(row)


@app.delete("/api/pages/{page_id}/roles/{role_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_organization_role(
    page_id: int, role_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        page = conn.execute(
            'SELECT * FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone()
        if not page:
            raise HTTPException(404, "Page not found")
        if page["owner_user_id"] != viewer["id"]:
            raise HTTPException(403, "Only the Page owner can manage role entries")
        row = conn.execute(
            'SELECT 1 FROM organization_page_roles WHERE id=%s AND page_id=%s',
            (role_id, page_id),
        ).fetchone()
        if not row:
            raise HTTPException(404, "Role entry not found")
        conn.execute('DELETE FROM organization_page_roles WHERE id=%s', (role_id,))


@app.post("/api/pages/{page_id}/follow", status_code=201)
@logged_operation(level=logging.INFO)
def follow_page(page_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        if not conn.execute(
            'SELECT 1 FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone():
            raise HTTPException(404, "Page not found")
        conn.execute(
            'INSERT INTO organization_page_followers( page_id,user_id,created_at ) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
            (page_id, viewer["id"], datetime.now(timezone.utc).isoformat()),
        )
    return {"ok": True}


@app.delete("/api/pages/{page_id}/follow", status_code=204)
@logged_operation(level=logging.INFO)
def unfollow_page(page_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        conn.execute(
            'DELETE FROM organization_page_followers WHERE page_id=%s AND user_id=%s',
            (page_id, viewer["id"]),
        )


@app.get("/api/pages/{page_id}/followers")
@logged_operation
def page_followers(page_id: int, authorization: Optional[str] = Header(default=None)):
    current_user(authorization)
    with db() as conn:
        if not conn.execute(
            'SELECT 1 FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone():
            raise HTTPException(404, "Page not found")
        rows = conn.execute(
            """
            SELECT u.id,u.username,u.full_name, f.created_at
            FROM organization_page_followers f
            JOIN users u ON u.id=f.user_id
            WHERE f.page_id=%s
            ORDER BY f.created_at DESC
            LIMIT 500
            """,
            (page_id,),
        ).fetchall()
    return {"followers": [dict(r) for r in rows]}


@app.get("/api/pages/{page_id}/members")
@logged_operation
def page_members(page_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        page, role = _page_and_permission(conn, page_id, viewer["id"])
        rows = conn.execute(
            """
            SELECT m.user_id,u.username,u.full_name, m.role,m.created_at
            FROM organization_page_members m
            JOIN users u ON u.id=m.user_id
            WHERE m.page_id=%s
            ORDER BY CASE m.role WHEN 'Admin' THEN 1 WHEN 'Editor' THEN 2 ELSE 3 END, u.username
            """,
            (page_id,),
        ).fetchall()
        owner = conn.execute(
            'SELECT id user_id,username,full_name FROM users WHERE id=%s',
            (page["owner_user_id"],),
        ).fetchone()
    return {
        "owner": dict(owner) if owner else None,
        "members": [dict(r) for r in rows],
        "viewer_role": role,
        "can_manage": role in ("Owner", "Admin"),
    }


@app.post("/api/pages/{page_id}/members", status_code=201)
@logged_operation(level=logging.INFO)
def add_page_member(
    page_id: int,
    payload: PageMemberPayload,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    allowed = {"Admin", "Editor", "Member"}
    if payload.role not in allowed:
        raise HTTPException(400, "Role must be Admin, Editor or Member")
    with db() as conn:
        page, role = _page_and_permission(conn, page_id, viewer["id"])
        if role not in ("Owner", "Admin"):
            raise HTTPException(403, "Only the Page owner or Admin can add members")
        user = conn.execute(
            'SELECT id,username,full_name FROM users WHERE lower(username)=lower(%s)',
            (payload.username.strip(),),
        ).fetchone()
        if not user:
            raise HTTPException(404, "User not found")
        if user["id"] == page["owner_user_id"]:
            raise HTTPException(400, "The Page owner already has full access")
        if role == "Admin" and payload.role == "Admin":
            raise HTTPException(403, "Only the Page owner can appoint another Admin")
        conn.execute(
            """
            INSERT INTO organization_page_members( page_id,user_id,role, added_by_user_id,created_at )
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT(page_id,user_id) DO UPDATE
            SET role=excluded.role, added_by_user_id=excluded.added_by_user_id
            """,
            (
                page_id,
                user["id"],
                payload.role,
                viewer["id"],
                datetime.now(timezone.utc).isoformat(),
            ),
        )
    audit(
        viewer["id"],
        "page.member.add",
        f"page:{page_id}:user:{user['id']}:{payload.role}",
    )
    return {
        "user_id": user["id"],
        "username": user["username"],
        "full_name": user["full_name"],
        "role": payload.role,
    }


@app.put("/api/pages/{page_id}/members/{user_id}")
@logged_operation(level=logging.INFO)
def change_page_member_role(
    page_id: int,
    user_id: int,
    payload: PageMemberRolePayload,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    allowed = {"Admin", "Editor", "Member"}
    if payload.role not in allowed:
        raise HTTPException(400, "Role must be Admin, Editor or Member")
    with db() as conn:
        page, role = _page_and_permission(conn, page_id, viewer["id"])
        if role not in ("Owner", "Admin"):
            raise HTTPException(403, "Only the Page owner or Admin can manage members")
        existing = conn.execute(
            'SELECT role FROM organization_page_members WHERE page_id=%s AND user_id=%s',
            (page_id, user_id),
        ).fetchone()
        if not existing:
            raise HTTPException(404, "Page member not found")
        if role == "Admin" and (existing["role"] == "Admin" or payload.role == "Admin"):
            raise HTTPException(403, "Only the Page owner can manage Admin roles")
        conn.execute(
            'UPDATE organization_page_members SET role=%s WHERE page_id=%s AND user_id=%s',
            (payload.role, page_id, user_id),
        )
    return {"ok": True, "role": payload.role}


@app.delete("/api/pages/{page_id}/members/{user_id}", status_code=204)
@logged_operation(level=logging.INFO)
def remove_page_member(
    page_id: int, user_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        page, role = _page_and_permission(conn, page_id, viewer["id"])
        target = conn.execute(
            'SELECT role FROM organization_page_members WHERE page_id=%s AND user_id=%s',
            (page_id, user_id),
        ).fetchone()
        if not target:
            raise HTTPException(404, "Page member not found")
        if viewer["id"] != user_id and role not in ("Owner", "Admin"):
            raise HTTPException(403, "Not allowed")
        if role == "Admin" and viewer["id"] != user_id and (target["role"] == "Admin"):
            raise HTTPException(403, "Only the Page owner can remove another Admin")
        conn.execute(
            'DELETE FROM organization_page_members WHERE page_id=%s AND user_id=%s',
            (page_id, user_id),
        )
    audit(viewer["id"], "page.member.remove", f"page:{page_id}:user:{user_id}")


@app.post("/api/pages/{page_id}/posts/media", status_code=201)
@logged_operation(level=logging.INFO)
def upload_page_post_media(
    page_id: int,
    upload: UploadFile = File(...),
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        _, _, can_post = _page_post_permission(conn, page_id, viewer["id"])
        if not can_post:
            raise HTTPException(
                403, "Only the Page owner, Admin or Editor can post as this Page"
            )
    ctype = (upload.content_type or "").lower()
    if ctype not in SOCIAL_IMAGE_TYPES:
        raise HTTPException(415, "Page post images must be PNG, JPG or WEBP.")
    data = upload.file.read(MAX_UPLOAD_BYTES + 1)
    if not data or len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            413 if data else 400, "Image must be non-empty and at most 10 MB."
        )
    stored = secrets.token_hex(24) + ALLOWED_UPLOAD_TYPES[ctype]
    _validate_upload(data, ctype)
    try:
        _stored_path(stored).write_bytes(data)
        now = datetime.now(timezone.utc).isoformat()
        with db() as conn:
            cur = conn.execute(
                """
                INSERT INTO uploaded_files( owner_user_id,category,original_name, stored_name,content_type,size_bytes,created_at )
                VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    viewer["id"],
                    "page_post_image",
                    Path(upload.filename or "page-post-image").name[:255],
                    stored,
                    ctype,
                    len(data),
                    now,
                ),
            )
            cur_id = cur.fetchone()["id"]
            audit(viewer["id"], "upload", f"file:{cur_id}")
        return {"id": cur_id}
    except BaseException:
        cleanup_failed_upload(stored)
        raise


@app.get("/api/page-posts")
@logged_operation
def page_feed(authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM page_posts ORDER BY id DESC LIMIT 200"
        ).fetchall()
    posts = [serialize_page_post(r, viewer["id"]) for r in rows]
    return {"posts": posts}


@app.get("/api/pages/{page_id}/posts")
@logged_operation
def list_page_posts(page_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        if not conn.execute(
            'SELECT 1 FROM organization_pages WHERE id=%s', (page_id,)
        ).fetchone():
            raise HTTPException(404, "Page not found")
        rows = conn.execute(
            'SELECT * FROM page_posts WHERE page_id=%s ORDER BY id DESC LIMIT 200',
            (page_id,),
        ).fetchall()
        _, role, can_post = _page_post_permission(conn, page_id, viewer["id"])
    posts = [serialize_page_post(r, viewer["id"]) for r in rows]
    return {"posts": posts, "viewer_role": role, "can_post": can_post}


@app.post("/api/pages/{page_id}/posts", status_code=201)
@logged_operation(level=logging.INFO)
def create_page_post(
    page_id: int,
    payload: PagePostCreate,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _, _, can_post = _page_post_permission(conn, page_id, viewer["id"])
        if not can_post:
            raise HTTPException(
                403, "Only the Page owner, Admin or Editor can post as this Page"
            )
        if payload.media_file_id:
            f = conn.execute(
                "SELECT 1 FROM uploaded_files WHERE id=%s AND owner_user_id=%s AND category='page_post_image'",
                (payload.media_file_id, viewer["id"]),
            ).fetchone()
            if not f:
                raise HTTPException(400, "Invalid Page post image")
        cur = conn.execute(
            """
            INSERT INTO page_posts( page_id,author_user_id,text, media_file_id,created_at,updated_at )
            VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                page_id,
                viewer["id"],
                payload.text.strip(),
                payload.media_file_id,
                now,
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM page_posts WHERE id=%s', (cur_id,)
        ).fetchone()
    audit(viewer["id"], "page.post.create", f"page:{page_id}:post:{row['id']}")
    return serialize_page_post(row, viewer["id"])


@app.put("/api/page-posts/{post_id}")
@logged_operation(level=logging.INFO)
def update_page_post(
    post_id: int,
    payload: PagePostUpdate,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute('SELECT * FROM page_posts WHERE id=%s', (post_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Page post not found")
        _, _, can_post = _page_post_permission(conn, row["page_id"], viewer["id"])
        if not can_post:
            raise HTTPException(403, "You cannot edit posts for this Page")
        conn.execute(
            'UPDATE page_posts SET text=%s,updated_at=%s WHERE id=%s',
            (payload.text.strip(), datetime.now(timezone.utc).isoformat(), post_id),
        )
        row = conn.execute('SELECT * FROM page_posts WHERE id=%s', (post_id,)).fetchone()
    return serialize_page_post(row, viewer["id"])


@app.delete("/api/page-posts/{post_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_page_post(post_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute('SELECT * FROM page_posts WHERE id=%s', (post_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Page post not found")
        _, _, can_post = _page_post_permission(conn, row["page_id"], viewer["id"])
        if not can_post:
            raise HTTPException(403, "You cannot delete posts for this Page")
        conn.execute('DELETE FROM page_posts WHERE id=%s', (post_id,))


@app.post("/api/page-posts/{post_id}/like", status_code=201)
@logged_operation(level=logging.INFO)
def like_page_post(post_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute('SELECT * FROM page_posts WHERE id=%s', (post_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Page post not found")
        conn.execute(
            'INSERT INTO page_post_likes( post_id,user_id,created_at ) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
            (post_id, viewer["id"], datetime.now(timezone.utc).isoformat()),
        )
    return {"ok": True}


@app.delete("/api/page-posts/{post_id}/like", status_code=204)
@logged_operation(level=logging.INFO)
def unlike_page_post(post_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        conn.execute(
            'DELETE FROM page_post_likes WHERE post_id=%s AND user_id=%s',
            (post_id, viewer["id"]),
        )


@app.get("/api/page-posts/{post_id}/comments")
@logged_operation
def page_post_comments(
    post_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        if not conn.execute(
            'SELECT 1 FROM page_posts WHERE id=%s', (post_id,)
        ).fetchone():
            raise HTTPException(404, "Page post not found")
        rows = conn.execute(
            """
            SELECT c.*,u.username,u.full_name,p.display_name
            FROM page_post_comments c
            JOIN users u ON u.id=c.author_user_id
            LEFT JOIN profiles p ON p.user_id=u.id
            WHERE c.post_id=%s
            ORDER BY c.id
            """,
            (post_id,),
        ).fetchall()
    comments = [
        {
            **dict(r),
            "author_name": r["display_name"] or r["full_name"],
            "is_owner": r["author_user_id"] == viewer["id"],
        }
        for r in rows
    ]
    return {"comments": comments}


@app.post("/api/page-posts/{post_id}/comments", status_code=201)
@logged_operation(level=logging.INFO)
def add_page_post_comment(
    post_id: int,
    payload: PagePostCommentCreate,
    authorization: Optional[str] = Header(default=None),
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        if not conn.execute(
            'SELECT 1 FROM page_posts WHERE id=%s', (post_id,)
        ).fetchone():
            raise HTTPException(404, "Page post not found")
        cur = conn.execute(
            'INSERT INTO page_post_comments( post_id,author_user_id,text,created_at ) VALUES(%s,%s,%s,%s) RETURNING id',
            (
                post_id,
                viewer["id"],
                payload.text.strip(),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        cur_id = cur.fetchone()["id"]
    return {"id": cur_id, "ok": True}


@app.delete("/api/page-post-comments/{comment_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_page_post_comment(
    comment_id: int, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM page_post_comments WHERE id=%s', (comment_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Comment not found")
        post = conn.execute(
            'SELECT * FROM page_posts WHERE id=%s', (row["post_id"],)
        ).fetchone()
        _, _, can_post = _page_post_permission(conn, post["page_id"], viewer["id"])
        if row["author_user_id"] != viewer["id"] and (not can_post):
            raise HTTPException(403, "You cannot delete this comment")
        conn.execute('DELETE FROM page_post_comments WHERE id=%s', (comment_id,))


@app.get("/api/groups")
@logged_operation
def list_groups(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM community_groups ORDER BY id DESC"
        ).fetchall()
        return {
            "groups": [
                _group_json(conn, g, u["id"])
                for g in rows
                if g["privacy"] == "public" or _group_role(conn, g["id"], u["id"])
            ]
        }


@app.post("/api/groups", status_code=201)
@logged_operation(level=logging.INFO)
def create_group(
    payload: CommunityGroupCreate, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    privacy = payload.privacy.lower()
    if privacy not in ("public", "private"):
        raise HTTPException(400, "Privacy must be public or private")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            'INSERT INTO community_groups(owner_user_id,name,description,privacy,created_at) VALUES(%s,%s,%s,%s,%s) RETURNING id',
            (u["id"], payload.name.strip(), payload.description.strip(), privacy, now),
        )
        cur_id = cur.fetchone()["id"]
        gid = cur_id
        conn.execute(
            """
            INSERT INTO community_group_members(group_id,user_id,role,status,joined_at)
            VALUES(%s,%s, 'owner','active',%s)
            """,
            (gid, u["id"], now),
        )
        g = conn.execute('SELECT * FROM community_groups WHERE id=%s', (gid,)).fetchone()
        return _group_json(conn, g, u["id"])


@app.post("/api/groups/{gid}/join")
@logged_operation(level=logging.INFO)
def join_group(gid: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        g, role = _group_access(conn, gid, u["id"])
        if role:
            return {"status": "active"}
        if g["privacy"] == "public":
            conn.execute(
                """
                INSERT INTO community_group_members(group_id,user_id,role,status,joined_at)
                VALUES(%s,%s,'member','active',%s) ON CONFLICT(group_id,user_id) DO UPDATE SET role=excluded.role,status=excluded.status,joined_at=excluded.joined_at""",
                (gid, u["id"], now),
            )
            return {"status": "active"}
        conn.execute(
            """
            INSERT INTO community_group_join_requests(group_id,user_id,status,created_at)
            VALUES(%s,%s,'pending',%s) ON CONFLICT(group_id,user_id) DO UPDATE SET status=excluded.status,created_at=excluded.created_at""",
            (gid, u["id"], now),
        )
        return {"status": "pending"}


@app.delete("/api/groups/{gid}/membership", status_code=204)
@logged_operation(level=logging.INFO)
def leave_group(gid: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        g, role = _group_access(conn, gid, u["id"])
        if role == "owner":
            raise HTTPException(400, "Group owner cannot leave the group")
        conn.execute(
            'DELETE FROM community_group_members WHERE group_id=%s AND user_id=%s',
            (gid, u["id"]),
        )
        conn.execute(
            'DELETE FROM community_group_join_requests WHERE group_id=%s AND user_id=%s',
            (gid, u["id"]),
        )


@app.get("/api/groups/{gid}/members")
@logged_operation
def group_members(gid: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        g, role = _group_access(conn, gid, u["id"])
        if g["privacy"] == "private" and (not role):
            raise HTTPException(403, "Private group membership required")
        rows = conn.execute(
            """
            SELECT m.user_id,m.role,u.username,u.full_name
            FROM community_group_members m
            JOIN users u ON u.id=m.user_id
            WHERE m.group_id=%s
            AND m.status='active'
            ORDER BY CASE m.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 WHEN 'moderator' THEN 2 ELSE 3 END,u.full_name
            """,
            (gid,),
        ).fetchall()
        return {"members": [dict(r) for r in rows]}


@app.get("/api/groups/{gid}/join-requests")
@logged_operation
def group_join_requests(gid: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        _, role = _group_access(conn, gid, u["id"])
        if role not in ("owner", "admin", "moderator"):
            raise HTTPException(403, "Moderator permission required")
        rows = conn.execute(
            """
            SELECT r.user_id,r.status,r.created_at,u.username,u.full_name
            FROM community_group_join_requests r
            JOIN users u ON u.id=r.user_id
            WHERE r.group_id=%s
            AND r.status='pending'
            """,
            (gid,),
        ).fetchall()
        return {"requests": [dict(r) for r in rows]}


@app.post("/api/groups/{gid}/join-requests/{uid}/accept")
@logged_operation(level=logging.INFO)
def accept_group_request(
    gid: int, uid: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _, role = _group_access(conn, gid, u["id"])
        if role not in ("owner", "admin", "moderator"):
            raise HTTPException(403, "Moderator permission required")
        if not conn.execute(
            "SELECT 1 FROM community_group_join_requests WHERE group_id=%s AND user_id=%s AND status='pending'",
            (gid, uid),
        ).fetchone():
            raise HTTPException(404, "Join request not found")
        conn.execute(
            """
            INSERT INTO community_group_members(group_id,user_id,role,status,joined_at)
            VALUES(%s,%s,'member','active',%s) ON CONFLICT(group_id,user_id) DO UPDATE SET role=excluded.role,status=excluded.status,joined_at=excluded.joined_at""",
            (gid, uid, now),
        )
        conn.execute(
            "UPDATE community_group_join_requests SET status='accepted' WHERE group_id=%s AND user_id=%s",
            (gid, uid),
        )
        return {"status": "accepted"}


@app.delete("/api/groups/{gid}/join-requests/{uid}", status_code=204)
@logged_operation(level=logging.INFO)
def decline_group_request(
    gid: int, uid: int, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    with db() as conn:
        _, role = _group_access(conn, gid, u["id"])
        if role not in ("owner", "admin", "moderator"):
            raise HTTPException(403, "Moderator permission required")
        conn.execute(
            'DELETE FROM community_group_join_requests WHERE group_id=%s AND user_id=%s',
            (gid, uid),
        )


@app.put("/api/groups/{gid}/members/{uid}/role")
@logged_operation(level=logging.INFO)
def group_member_role(
    gid: int,
    uid: int,
    payload: CommunityGroupRoleUpdate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    role2 = payload.role.lower()
    if role2 not in ("admin", "moderator", "member"):
        raise HTTPException(400, "Role must be admin, moderator or member")
    with db() as conn:
        _, role = _group_access(conn, gid, u["id"])
        if role not in ("owner", "admin"):
            raise HTTPException(403, "Admin permission required")
        target = _group_role(conn, gid, uid)
        if not target:
            raise HTTPException(404, "Member not found")
        if role != "owner" and (target == "admin" or role2 == "admin"):
            raise HTTPException(403, "Only the owner can manage admin roles")
        if target == "owner":
            raise HTTPException(403, "Owner role cannot be changed")
        conn.execute(
            'UPDATE community_group_members SET role=%s WHERE group_id=%s AND user_id=%s',
            (role2, gid, uid),
        )
        return {"role": role2}


@app.get("/api/groups/{gid}/posts")
@logged_operation
def group_posts(gid: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        g, role = _group_access(conn, gid, u["id"])
        if g["privacy"] == "private" and (not role):
            raise HTTPException(403, "Private group membership required")
        rows = conn.execute(
            """
            SELECT p.*,u.username,u.full_name,(SELECT COUNT(*)
            FROM community_group_comments c
            WHERE c.post_id=p.id) comment_count
            FROM community_group_posts p
            JOIN users u ON u.id=p.author_user_id
            WHERE p.group_id=%s
            ORDER BY p.id DESC
            LIMIT 200
            """,
            (gid,),
        ).fetchall()
        return {
            "posts": [
                dict(r)
                | {
                    "can_manage": r["author_user_id"] == u["id"]
                    or role in ("owner", "admin", "moderator")
                }
                for r in rows
            ]
        }


@app.post("/api/groups/{gid}/posts", status_code=201)
@logged_operation(level=logging.INFO)
def create_group_post(
    gid: int,
    payload: CommunityGroupPostCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _, role = _group_access(conn, gid, u["id"])
        if not role:
            raise HTTPException(403, "Join the group before posting")
        cur = conn.execute(
            """
            INSERT INTO community_group_posts(group_id,author_user_id,text,created_at,updated_at)
            VALUES(%s,%s,%s,%s,%s) RETURNING id""",
            (gid, u["id"], payload.text.strip(), now, now),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id}


@app.delete("/api/group-posts/{pid}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_group_post(pid: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        p = conn.execute(
            'SELECT * FROM community_group_posts WHERE id=%s', (pid,)
        ).fetchone()
        if not p:
            raise HTTPException(404, "Group post not found")
        role = _group_role(conn, p["group_id"], u["id"])
        if p["author_user_id"] != u["id"] and role not in (
            "owner",
            "admin",
            "moderator",
        ):
            raise HTTPException(403, "Not allowed")
        conn.execute('DELETE FROM community_group_posts WHERE id=%s', (pid,))


@app.get("/api/group-posts/{pid}/comments")
@logged_operation
def group_comments(pid: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        p = conn.execute(
            'SELECT * FROM community_group_posts WHERE id=%s', (pid,)
        ).fetchone()
        if not p:
            raise HTTPException(404, "Group post not found")
        g, role = _group_access(conn, p["group_id"], u["id"])
        if g["privacy"] == "private" and (not role):
            raise HTTPException(403, "Private group membership required")
        rows = conn.execute(
            """
            SELECT c.*,u.full_name,u.username
            FROM community_group_comments c
            JOIN users u ON u.id=c.author_user_id
            WHERE c.post_id=%s
            ORDER BY c.id
            """,
            (pid,),
        ).fetchall()
        return {"comments": [dict(r) for r in rows]}


@app.post("/api/group-posts/{pid}/comments", status_code=201)
@logged_operation(level=logging.INFO)
def add_group_comment(
    pid: int,
    payload: CommunityGroupCommentCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        p = conn.execute(
            'SELECT * FROM community_group_posts WHERE id=%s', (pid,)
        ).fetchone()
        if not p:
            raise HTTPException(404, "Group post not found")
        if not _group_role(conn, p["group_id"], u["id"]):
            raise HTTPException(403, "Join the group before commenting")
        cur = conn.execute(
            'INSERT INTO community_group_comments(post_id,author_user_id,text,created_at) VALUES(%s,%s,%s,%s) RETURNING id',
            (pid, u["id"], payload.text.strip(), now),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id}


@app.get("/api/events")
@logged_operation
def list_events(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM events
            WHERE visibility='public'
            OR creator_user_id=%s
            OR id IN (SELECT event_id
            FROM event_attendees
            WHERE user_id=%s)
            ORDER BY start_at
            """,
            (u["id"], u["id"]),
        ).fetchall()
        return {"events": [_event_json(conn, e, u["id"]) for e in rows]}


@app.post("/api/events", status_code=201)
@logged_operation(level=logging.INFO)
def create_event(
    payload: EventCreate, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    vis = (
        payload.visibility if payload.visibility in ("public", "private") else "public"
    )
    ht = payload.host_type if payload.host_type in ("user", "page", "group") else "user"
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO events(creator_user_id,host_type,host_id,title,description,start_at,end_at,location,meeting_url,visibility,created_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                u["id"],
                ht,
                payload.host_id,
                payload.title.strip(),
                payload.description.strip(),
                payload.start_at,
                payload.end_at,
                payload.location.strip(),
                payload.meeting_url.strip(),
                vis,
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        eid = cur_id
        conn.execute(
            'INSERT INTO event_attendees(event_id,user_id,rsvp,invited_by,updated_at) VALUES(%s,%s,%s,%s,%s)',
            (eid, u["id"], "going", u["id"], now),
        )
        return {"id": eid}


@app.post("/api/events/{eid}/invite", status_code=201)
@logged_operation(level=logging.INFO)
def invite_event(
    eid: int, payload: EventInvite, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        e = conn.execute('SELECT * FROM events WHERE id=%s', (eid,)).fetchone()
        if not e:
            raise HTTPException(404, "Event not found")
        if e["creator_user_id"] != u["id"]:
            raise HTTPException(403, "Only the event creator can invite people")
        target = conn.execute(
            'SELECT id FROM users WHERE lower(username)=lower(%s)',
            (payload.username.strip(),),
        ).fetchone()
        if not target:
            raise HTTPException(404, "User not found")
        conn.execute(
            """
            INSERT INTO event_attendees(event_id,user_id,rsvp,invited_by,updated_at)
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT(event_id,user_id) DO UPDATE
            SET rsvp='invited',invited_by=excluded.invited_by,updated_at=excluded.updated_at
            """,
            (eid, target["id"], "invited", u["id"], now),
        )
        return {"ok": True}


@app.put("/api/events/{eid}/rsvp")
@logged_operation(level=logging.INFO)
def rsvp_event(
    eid: int, payload: EventRSVP, authorization: Optional[str] = Header(default=None)
):
    u, _ = current_user(authorization)
    st = payload.status.lower()
    if st not in ("going", "maybe", "declined"):
        raise HTTPException(400, "RSVP must be going, maybe, or declined")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        event = conn.execute('SELECT * FROM events WHERE id=%s', (eid,)).fetchone()
        if (
            event
            and event["visibility"] != "public"
            and (event["creator_user_id"] != u["id"])
            and (
                not conn.execute(
                    'SELECT 1 FROM event_attendees WHERE event_id=%s AND user_id=%s',
                    (eid, u["id"]),
                ).fetchone()
            )
        ):
            raise HTTPException(403, "Private event invitation required")
        if not conn.execute('SELECT 1 FROM events WHERE id=%s', (eid,)).fetchone():
            raise HTTPException(404, "Event not found")
        conn.execute(
            """
            INSERT INTO event_attendees(event_id,user_id,rsvp,updated_at)
            VALUES(%s,%s,%s,%s)
            ON CONFLICT(event_id,user_id) DO UPDATE
            SET rsvp=excluded.rsvp,updated_at=excluded.updated_at
            """,
            (eid, u["id"], st, now),
        )
        return {"ok": True, "rsvp": st}


@app.delete("/api/events/{eid}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_event(eid: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        e = conn.execute('SELECT * FROM events WHERE id=%s', (eid,)).fetchone()
        if not e:
            raise HTTPException(404, "Event not found")
        if e["creator_user_id"] != u["id"]:
            raise HTTPException(403, "Only the event creator can delete it")
        conn.execute('DELETE FROM events WHERE id=%s', (eid,))


@app.get("/api/jobs")
@logged_operation
def list_jobs(
    q: str = "",
    opportunity_type: str = "",
    mine: bool = False,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    where = []
    args = []
    if mine:
        where.append("j.creator_user_id=?")
        args.append(u["id"])
    else:
        where.append("j.status='open'")
    if q.strip():
        where.append(
            '(j.title ILIKE %s OR j.description ILIKE %s OR j.skills ILIKE %s OR j.location ILIKE %s)'
        )
        like = "%" + q.strip() + "%"
        args += [like] * 4
    if opportunity_type.strip():
        where.append("j.opportunity_type=?")
        args.append(opportunity_type.strip())
    sql = (
        "SELECT j.* FROM jobs j"
        + (" WHERE " + " AND ".join(where) if where else "")
        + " ORDER BY j.id DESC LIMIT 200"
    )
    with db() as conn:
        return {
            "jobs": [
                _job_json(conn, r, u["id"]) for r in conn.execute(sql, args).fetchall()
            ]
        }


@app.post("/api/jobs", status_code=201)
@logged_operation(level=logging.INFO)
def create_job(payload: JobCreate, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    allowed = {
        "Job",
        "Freelance",
        "Internship",
        "Contract",
        "Volunteer",
        "Collaboration",
    }
    modes = {"On-site", "Remote", "Hybrid"}
    if payload.opportunity_type not in allowed:
        raise HTTPException(400, "Invalid opportunity type")
    if payload.work_mode not in modes:
        raise HTTPException(400, "Invalid work mode")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        if payload.page_id:
            _, role = _page_and_permission(conn, payload.page_id, u["id"])
            if role not in ("Owner", "Admin", "Editor"):
                raise HTTPException(403, "You cannot post opportunities for this Page")
        cur = conn.execute(
            """
            INSERT INTO jobs(creator_user_id,page_id,title,opportunity_type,work_mode,location,description,skills,compensation,application_deadline,status,created_at,updated_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open',%s,%s) RETURNING id""",
            (
                u["id"],
                payload.page_id,
                payload.title.strip(),
                payload.opportunity_type,
                payload.work_mode,
                payload.location.strip(),
                payload.description.strip(),
                payload.skills.strip(),
                payload.compensation.strip(),
                payload.application_deadline.strip(),
                now,
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        audit(u["id"], "job:create", f"job:{cur_id}")
        return {"id": cur_id}


@app.put("/api/jobs/{job_id}")
@logged_operation(level=logging.INFO)
def update_job(
    job_id: int, payload: JobCreate, authorization: Optional[str] = Header(default=None)
):
    if payload.opportunity_type not in {
        "Job",
        "Freelance",
        "Internship",
        "Contract",
        "Volunteer",
        "Collaboration",
    } or payload.work_mode not in {"On-site", "Remote", "Hybrid"}:
        raise HTTPException(400, "Invalid opportunity type or work mode")
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        j = conn.execute('SELECT * FROM jobs WHERE id=%s', (job_id,)).fetchone()
        if not j:
            raise HTTPException(404, "Opportunity not found")
        if not _job_manage_ok(conn, j, u["id"]):
            raise HTTPException(403, "Not allowed")
        conn.execute(
            """
            UPDATE jobs
            SET title=%s,opportunity_type=%s,work_mode=%s,location=%s,description=%s,skills=%s,compensation=%s,application_deadline=%s,updated_at=%s
            WHERE id=%s
            """,
            (
                payload.title.strip(),
                payload.opportunity_type,
                payload.work_mode,
                payload.location.strip(),
                payload.description.strip(),
                payload.skills.strip(),
                payload.compensation.strip(),
                payload.application_deadline.strip(),
                now,
                job_id,
            ),
        )
        return {"ok": True}


@app.delete("/api/jobs/{job_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_job(job_id: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        j = conn.execute('SELECT * FROM jobs WHERE id=%s', (job_id,)).fetchone()
        if not j:
            raise HTTPException(404, "Opportunity not found")
        if not _job_manage_ok(conn, j, u["id"]):
            raise HTTPException(403, "Not allowed")
        conn.execute('DELETE FROM jobs WHERE id=%s', (job_id,))


@app.post("/api/jobs/{job_id}/apply", status_code=201)
@logged_operation(level=logging.INFO)
def apply_job(
    job_id: int,
    payload: JobApplicationCreate,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _valid_owned_file(conn, payload.resume_file_id, u["id"])
        j = conn.execute('SELECT * FROM jobs WHERE id=%s', (job_id,)).fetchone()
        if not j or j["status"] != "open":
            raise HTTPException(404, "Open opportunity not found")
        if j["creator_user_id"] == u["id"]:
            raise HTTPException(400, "You cannot apply to your own opportunity")
        try:
            conn.execute(
                """
                INSERT INTO job_applications(job_id,applicant_user_id,cover_note,portfolio_url,resume_file_id,status,created_at,updated_at)
                VALUES(%s,%s,%s,%s,%s,'submitted',%s,%s)
                """,
                (
                    job_id,
                    u["id"],
                    payload.cover_note.strip(),
                    payload.portfolio_url.strip(),
                    payload.resume_file_id,
                    now,
                    now,
                ),
            )
        except psycopg.IntegrityError:
            raise HTTPException(409, "You already applied")
        return {"ok": True}


@app.get("/api/jobs/{job_id}/applications")
@logged_operation
def job_applications(job_id: int, authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        j = conn.execute('SELECT * FROM jobs WHERE id=%s', (job_id,)).fetchone()
        if not j:
            raise HTTPException(404, "Opportunity not found")
        if not _job_manage_ok(conn, j, u["id"]):
            raise HTTPException(403, "Not allowed")
        rows = conn.execute(
            """
            SELECT a.*,u.username,u.full_name,COALESCE(p.headline,'') headline
            FROM job_applications a
            JOIN users u ON u.id=a.applicant_user_id
            LEFT JOIN profiles p ON p.user_id=u.id
            WHERE a.job_id=%s
            ORDER BY a.id DESC
            """,
            (job_id,),
        ).fetchall()
        return {"applications": [dict(r) for r in rows]}


@app.put("/api/job-applications/{application_id}/status")
@logged_operation(level=logging.INFO)
def update_application_status(
    application_id: int,
    payload: JobApplicationStatus,
    authorization: Optional[str] = Header(default=None),
):
    u, _ = current_user(authorization)
    allowed = {
        "submitted",
        "reviewing",
        "shortlisted",
        "accepted",
        "rejected",
        "withdrawn",
    }
    if payload.status not in allowed:
        raise HTTPException(400, "Invalid application status")
    with db() as conn:
        a = conn.execute(
            """
            SELECT a.*,j.creator_user_id,j.page_id
            FROM job_applications a
            JOIN jobs j ON j.id=a.job_id
            WHERE a.id=%s
            """,
            (application_id,),
        ).fetchone()
        if not a:
            raise HTTPException(404, "Application not found")
        if payload.status == "withdrawn":
            if a["applicant_user_id"] != u["id"]:
                raise HTTPException(403, "Only applicant can withdraw")
        else:
            j = conn.execute('SELECT * FROM jobs WHERE id=%s', (a["job_id"],)).fetchone()
            if not _job_manage_ok(conn, j, u["id"]):
                raise HTTPException(403, "Not allowed")
        conn.execute(
            'UPDATE job_applications SET status=%s,updated_at=%s WHERE id=%s',
            (payload.status, datetime.now(timezone.utc).isoformat(), application_id),
        )
        return {"ok": True}


@app.get("/api/job-applications/me")
@logged_operation
def my_job_applications(authorization: Optional[str] = Header(default=None)):
    u, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT a.*,j.title,j.opportunity_type,j.work_mode,j.location,op.name page_name
            FROM job_applications a
            JOIN jobs j ON j.id=a.job_id
            LEFT JOIN organization_pages op ON op.id=j.page_id
            WHERE a.applicant_user_id=%s
            ORDER BY a.id DESC
            """,
            (u["id"],),
        ).fetchall()
        return {"applications": [dict(r) for r in rows]}


@app.get("/api/portfolio/projects")
@logged_operation
def portfolio_projects(
    username: str = "",
    mine: bool = False,
    authorization: Optional[str] = Header(default=None),
):
    u = require_user(authorization)
    with db() as conn:
        if mine:
            rows = conn.execute(
                'SELECT * FROM portfolio_projects WHERE user_id=%s ORDER BY id DESC',
                (u["id"],),
            ).fetchall()
        elif username:
            owner = conn.execute(
                'SELECT id FROM users WHERE username=%s', (username,)
            ).fetchone()
            if not owner:
                raise HTTPException(404, "User not found")
            if owner["id"] == u["id"]:
                rows = conn.execute(
                    'SELECT * FROM portfolio_projects WHERE user_id=%s ORDER BY id DESC',
                    (owner["id"],),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM portfolio_projects WHERE user_id=%s AND visibility='Public' ORDER BY id DESC",
                    (owner["id"],),
                ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM portfolio_projects WHERE visibility='Public' OR user_id=%s ORDER BY id DESC LIMIT 100",
                (u["id"],),
            ).fetchall()
        return {"projects": [_portfolio_json(conn, r, u["id"]) for r in rows]}


@app.post("/api/portfolio/projects", status_code=201)
@logged_operation(level=logging.INFO)
def create_portfolio_project(
    payload: PortfolioProjectCreate, authorization: Optional[str] = Header(default=None)
):
    u = require_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    if payload.visibility not in {"Public", "Private"}:
        raise HTTPException(400, "Invalid visibility")
    with db() as conn:
        if payload.media_file_id:
            f = conn.execute(
                'SELECT id FROM uploaded_files WHERE id=%s AND owner_user_id=%s',
                (payload.media_file_id, u["id"]),
            ).fetchone()
            if not f:
                raise HTTPException(400, "Invalid media file")
        cur = conn.execute(
            """
            INSERT INTO portfolio_projects(user_id,title,category,description,skills,tools,project_url,repository_url,media_file_id,visibility,created_at,updated_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                u["id"],
                payload.title.strip(),
                payload.category.strip(),
                payload.description.strip(),
                payload.skills.strip(),
                payload.tools.strip(),
                payload.project_url.strip(),
                payload.repository_url.strip(),
                payload.media_file_id,
                payload.visibility,
                now,
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id, "ok": True}


@app.put("/api/portfolio/projects/{project_id}")
@logged_operation(level=logging.INFO)
def update_portfolio_project(
    project_id: int,
    payload: PortfolioProjectCreate,
    authorization: Optional[str] = Header(default=None),
):
    u = require_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    if payload.visibility not in {"Public", "Private"}:
        raise HTTPException(400, "Invalid visibility")
    with db() as conn:
        _valid_owned_file(conn, payload.media_file_id, u["id"])
        r = conn.execute(
            'SELECT * FROM portfolio_projects WHERE id=%s', (project_id,)
        ).fetchone()
        if not r:
            raise HTTPException(404, "Project not found")
        if r["user_id"] != u["id"]:
            raise HTTPException(403, "Not allowed")
        conn.execute(
            """
            UPDATE portfolio_projects
            SET title=%s,category=%s,description=%s,skills=%s,tools=%s,project_url=%s,repository_url=%s,media_file_id=%s,visibility=%s,updated_at=%s
            WHERE id=%s
            """,
            (
                payload.title.strip(),
                payload.category.strip(),
                payload.description.strip(),
                payload.skills.strip(),
                payload.tools.strip(),
                payload.project_url.strip(),
                payload.repository_url.strip(),
                payload.media_file_id,
                payload.visibility,
                now,
                project_id,
            ),
        )
        return {"ok": True}


@app.delete("/api/portfolio/projects/{project_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_portfolio_project(
    project_id: int, authorization: Optional[str] = Header(default=None)
):
    u = require_user(authorization)
    with db() as conn:
        r = conn.execute(
            'SELECT user_id FROM portfolio_projects WHERE id=%s', (project_id,)
        ).fetchone()
        if not r:
            raise HTTPException(404, "Project not found")
        if r["user_id"] != u["id"]:
            raise HTTPException(403, "Not allowed")
        conn.execute('DELETE FROM portfolio_projects WHERE id=%s', (project_id,))


@app.get("/api/learning/courses")
@logged_operation
def list_courses(authorization: Optional[str] = Header(default=None)):
    u = require_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.*,COALESCE(e.progress,0) progress,COALESCE(e.status,'') enrollment_status
            FROM learning_courses c
            LEFT JOIN course_enrollments e ON e.course_id=c.id
            AND e.user_id=%s
            WHERE c.visibility='Public'
            OR c.creator_user_id=%s
            ORDER BY c.id DESC
            """,
            (u["id"], u["id"]),
        ).fetchall()
        return {"courses": [dict(r) for r in rows]}


@app.post("/api/learning/courses", status_code=201)
@logged_operation(level=logging.INFO)
def create_course(
    payload: CourseCreate, authorization: Optional[str] = Header(default=None)
):
    u = require_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    if payload.visibility not in {"Public", "Private"}:
        raise HTTPException(400, "Invalid visibility")
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO learning_courses(creator_user_id,title,provider,instructor,description,skills,course_url,visibility,created_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                u["id"],
                payload.title.strip(),
                payload.provider.strip(),
                payload.instructor.strip(),
                payload.description.strip(),
                payload.skills.strip(),
                payload.course_url.strip(),
                payload.visibility,
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id, "ok": True}


@app.post("/api/learning/courses/{course_id}/enroll", status_code=201)
@logged_operation(level=logging.INFO)
def enroll_course(course_id: int, authorization: Optional[str] = Header(default=None)):
    u = require_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        if not conn.execute(
            "SELECT id FROM learning_courses WHERE id=%s AND (visibility='Public' OR creator_user_id=%s)",
            (course_id, u["id"]),
        ).fetchone():
            raise HTTPException(404, "Course not found")
        conn.execute(
            """
            INSERT INTO course_enrollments(course_id,user_id,progress,status,created_at)
            VALUES(%s,%s,0,'Enrolled',%s) ON CONFLICT DO NOTHING""",
            (course_id, u["id"], now),
        )
        return {"ok": True}


@app.put("/api/learning/courses/{course_id}/progress")
@logged_operation(level=logging.INFO)
def course_progress(
    course_id: int,
    payload: EnrollmentProgress,
    authorization: Optional[str] = Header(default=None),
):
    u = require_user(authorization)
    with db() as conn:
        cur = conn.execute(
            'UPDATE course_enrollments SET progress=%s,status=%s WHERE course_id=%s AND user_id=%s',
            (payload.progress, payload.status, course_id, u["id"]),
        )
        if not cur.rowcount:
            raise HTTPException(404, "Enrollment not found")
        return {"ok": True}


@app.get("/api/learning/certifications")
@logged_operation
def list_certifications(authorization: Optional[str] = Header(default=None)):
    u = require_user(authorization)
    with db() as conn:
        return {
            "certifications": [
                dict(r)
                for r in conn.execute(
                    'SELECT * FROM certifications WHERE user_id=%s ORDER BY id DESC',
                    (u["id"],),
                ).fetchall()
            ]
        }


@app.post("/api/learning/certifications", status_code=201)
@logged_operation(level=logging.INFO)
def create_certification(
    payload: CertificationCreate, authorization: Optional[str] = Header(default=None)
):
    u = require_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO certifications(user_id,name,issuer,credential_id,credential_url,issue_date,skills,created_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                u["id"],
                payload.name.strip(),
                payload.issuer.strip(),
                payload.credential_id.strip(),
                payload.credential_url.strip(),
                payload.issue_date.strip(),
                payload.skills.strip(),
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        return {"id": cur_id, "ok": True}


@app.delete("/api/learning/certifications/{cert_id}", status_code=204)
@logged_operation(level=logging.INFO)
def delete_certification(
    cert_id: int, authorization: Optional[str] = Header(default=None)
):
    u = require_user(authorization)
    with db() as conn:
        conn.execute(
            'DELETE FROM certifications WHERE id=%s AND user_id=%s', (cert_id, u["id"])
        )


@app.get("/api/explore/interests")
@logged_operation
def get_explore_interests(authorization: Optional[str] = Header(default=None)):
    u = require_user(authorization)
    with db() as conn:
        rows = conn.execute(
            'SELECT interest FROM user_interests WHERE user_id=%s ORDER BY interest',
            (u["id"],),
        ).fetchall()
    interests = [r["interest"] for r in rows]
    return {"interests": interests}


@app.put("/api/explore/interests")
@logged_operation(level=logging.INFO)
def put_explore_interests(
    payload: ExploreInterests, authorization: Optional[str] = Header(default=None)
):
    u = require_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    clean = []
    for x in payload.interests[:30]:
        x = str(x).strip()[:80]
        if x and x.lower() not in {v.lower() for v in clean}:
            clean.append(x)
    with db() as conn:
        conn.execute('DELETE FROM user_interests WHERE user_id=%s', (u["id"],))
        with conn.cursor() as cursor:
            cursor.executemany(
                'INSERT INTO user_interests( user_id, interest, created_at ) VALUES(%s,%s,%s)',
                [(u["id"], x, now) for x in clean],
            )
    return {"ok": True, "interests": clean}


@app.get("/api/explore")
@logged_operation
def explore_discovery(authorization: Optional[str] = Header(default=None)):
    u = require_user(authorization)
    with db() as conn:
        interests = [
            r["interest"]
            for r in conn.execute(
                'SELECT interest FROM user_interests WHERE user_id=%s', (u["id"],)
            ).fetchall()
        ]
        people = [
            dict(r)
            for r in conn.execute(
                """
            SELECT u.id, u.username, u.full_name, u.role, COALESCE( p.display_name, u.full_name ) display_name, COALESCE(p.headline,'') headline, COALESCE(p.photo,'') photo, ( SELECT COUNT(*)
            FROM follows f
            WHERE f.followed_user_id=u.id ) follower_count
            FROM users u
            LEFT JOIN profiles p ON p.user_id=u.id
            WHERE u.id<>%s
            AND u.id NOT IN ( SELECT followed_user_id
            FROM follows
            WHERE follower_user_id=%s )
            ORDER BY follower_count DESC,u.id DESC
            LIMIT 12
            """,
                (u["id"], u["id"]),
            ).fetchall()
        ]
        pages = [
            dict(r)
            for r in conn.execute(
                """
            SELECT p.id, p.name, p.page_type, p.category, p.location, p.about, ( SELECT COUNT(*)
            FROM organization_page_followers f
            WHERE f.page_id=p.id ) follower_count
            FROM organization_pages p
            WHERE p.id NOT IN ( SELECT page_id
            FROM organization_page_followers
            WHERE user_id=%s )
            ORDER BY follower_count DESC,p.id DESC
            LIMIT 12
            """,
                (u["id"],),
            ).fetchall()
        ]
        groups = [
            dict(r)
            for r in conn.execute(
                """
            SELECT g.id, g.name, g.description, g.privacy, ( SELECT COUNT(*)
            FROM community_group_members m
            WHERE m.group_id=g.id
            AND m.status='active' ) member_count
            FROM community_groups g
            WHERE g.privacy='public'
            OR g.owner_user_id=%s
            OR g.id IN ( SELECT group_id
            FROM community_group_members
            WHERE user_id=%s
            AND status='active' )
            ORDER BY member_count DESC,g.id DESC
            LIMIT 12
            """,
                (u["id"], u["id"]),
            ).fetchall()
        ]
        posts = [
            dict(r)
            for r in conn.execute(
                """
            SELECT p.id, p.text, p.category, p.created_at, u.username, u.full_name, ( SELECT COUNT(*)
            FROM post_likes l
            WHERE l.post_id=p.id ) like_count
            FROM posts p
            JOIN users u ON u.id=p.author_user_id
            WHERE upper(p.visibility)='PUBLIC'
            OR p.author_user_id=%s
            ORDER BY like_count DESC,p.id DESC
            LIMIT 12
            """,
                (u["id"],),
            ).fetchall()
        ]
        events = [
            dict(r)
            for r in conn.execute(
                """
            SELECT id, title, description, start_at, location, visibility
            FROM events
            WHERE visibility='public'
            OR creator_user_id=%s
            OR id IN ( SELECT event_id
            FROM event_attendees
            WHERE user_id=%s )
            ORDER BY start_at
            LIMIT 12
            """,
                (u["id"], u["id"]),
            ).fetchall()
        ]
        jobs = [dict(r) for r in conn.execute("""
            SELECT id, title, opportunity_type, work_mode, location, skills, created_at
            FROM jobs
            WHERE status='open'
            ORDER BY id DESC
            LIMIT 12
            """).fetchall()]
        courses = [
            dict(r)
            for r in conn.execute(
                """
            SELECT id, title, provider, instructor, skills, created_at
            FROM learning_courses
            WHERE visibility='Public'
            OR creator_user_id=%s
            ORDER BY id DESC
            LIMIT 12
            """,
                (u["id"],),
            ).fetchall()
        ]
        projects = [
            dict(r)
            for r in conn.execute(
                """
            SELECT pp.id, pp.title, pp.category, pp.description, pp.skills, pp.tools, u.username, u.full_name
            FROM portfolio_projects pp
            JOIN users u ON u.id=pp.user_id
            WHERE pp.visibility='Public'
            OR pp.user_id=%s
            ORDER BY pp.id DESC
            LIMIT 12
            """,
                (u["id"],),
            ).fetchall()
        ]
    return {
        "interests": interests,
        "people": people,
        "pages": pages,
        "groups": groups,
        "posts": posts,
        "events": events,
        "jobs": jobs,
        "courses": courses,
        "projects": projects,
    }


@app.websocket("/ws/communication")
@logged_operation
async def communication_websocket(ws: WebSocket):
    token = ws.headers.get("authorization", "")
    if token.startswith("Bearer "):
        token = token[7:].strip()
    else:
        token = ws.query_params.get("token", "").strip()
    uid = None
    connected = False

    def authenticate():
        return current_user("Bearer " + token)[0]["id"]

    def peer_policy(peer_id, typing=False):
        with db() as conn:
            if uid != peer_id and (
                not are_connected(conn, uid, peer_id)
                or not _not_blocked(conn, uid, peer_id)
            ):
                return False
            row = _communication_settings_row(conn, uid if typing else peer_id)
            return bool(row["typing_indicators"] if typing else row["online_status"])

    try:
        uid = await run_in_threadpool(authenticate)
        await realtime_hub.connect(uid, ws)
        connected = True
        await ws.send_json({"type": "ready", "user_id": uid})
        while True:
            await run_in_threadpool(authenticate)
            try:
                data = await asyncio.wait_for(ws.receive_json(), timeout=30)
            except asyncio.TimeoutError:
                continue
            if not isinstance(data, dict):
                await ws.close(code=4400)
                return
            typ = data.get("type")
            try:
                peer_id = int(data.get("peer_id") or 0)
            except (TypeError, ValueError):
                await ws.close(code=4400)
                return
            if typ in {"typing_start", "typing_stop"} and peer_id:
                if await run_in_threadpool(peer_policy, peer_id, True):
                    await realtime_hub.send(peer_id, {"type": typ, "user_id": uid})
            elif typ == "presence_query" and peer_id:
                visible = await run_in_threadpool(peer_policy, peer_id)
                await ws.send_json(
                    {
                        "type": "presence",
                        "user_id": peer_id,
                        "online": bool(visible and realtime_hub.online(peer_id)),
                    }
                )
    except HTTPException:
        safe_log(logging.WARNING, "websocket", "authentication_rejected")
        await ws.close(code=4401)
    except WebSocketDisconnect:
        safe_log(logging.DEBUG, "websocket", "disconnected")
    except Exception:
        safe_log(logging.ERROR, "websocket", "failed")
        try:
            await ws.close(code=1011)
        except Exception:
            pass
    finally:
        if connected:
            realtime_hub.disconnect(uid, ws)


@app.middleware("http")
@logged_operation
async def noveam_production_guard(request: Request, call_next):
    started = time.perf_counter()
    operation = "http_api" if request.url.path.startswith("/api/") else "http_frontend"

    def complete(response):
        response.headers.update(
            {
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "Referrer-Policy": "strict-origin-when-cross-origin",
                "Permissions-Policy": "camera=(self), microphone=(self), geolocation=(self)",
                "Cross-Origin-Opener-Policy": "same-origin",
            }
        )
        if NOVEAM_ENV == "production" and request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        if NOVEAM_REQUIRE_HTTPS and request.url.scheme == "https":
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )
        severity = (
            logging.ERROR
            if response.status_code >= 500
            else (
                logging.WARNING
                if response.status_code in {401, 403, 429}
                else logging.DEBUG
            )
        )
        safe_log(
            severity,
            operation,
            "response",
            (time.perf_counter() - started) * 1000,
            response.status_code,
        )
        return response

    if NOVEAM_REQUIRE_HTTPS:
        forwarded = request.headers.get("x-forwarded-proto", "").lower()
        secure = request.url.scheme == "https" or (
            NOVEAM_TRUST_PROXY_HTTPS and forwarded == "https"
        )
        local = bool(
            request.client
            and request.client.host in {"127.0.0.1", "::1", "testclient"}
            and (request.url.hostname in {"localhost", "127.0.0.1"})
        )
        if not secure and (not local):
            return complete(JSONResponse({"detail": "HTTPS required"}, status_code=426))
    now = time.monotonic()
    while _rate_buckets and next(iter(_rate_buckets.values()))[-1] < now - 60:
        _rate_buckets.popitem(last=False)
    key = _client_key(request)
    bucket = _rate_buckets.setdefault(key, deque())
    while bucket and bucket[0] < now - 60:
        bucket.popleft()
    if len(bucket) >= NOVEAM_RATE_LIMIT_PER_MINUTE:
        return complete(
            JSONResponse(
                {"detail": "Too many requests"},
                status_code=429,
                headers={"Retry-After": "60"},
            )
        )
    bucket.append(now)
    _rate_buckets.move_to_end(key)
    while len(_rate_buckets) > 10000:
        _rate_buckets.popitem(last=False)
    try:
        return complete(await call_next(request))
    except Exception:
        safe_log(
            logging.ERROR, operation, "failed", (time.perf_counter() - started) * 1000
        )
        raise


@app.get("/api/system/health")
@logged_operation
def system_health():
    try:
        with db() as conn:
            conn.execute("SELECT 1 FROM users LIMIT 1").fetchone()
    except psycopg.Error:
        raise HTTPException(503, "Database unavailable")
    return {
        "status": "ok",
        "environment": NOVEAM_ENV,
        "database": 'postgresql',
        "livekit_configured": bool(
            LIVEKIT_URL and LIVEKIT_API_KEY and LIVEKIT_API_SECRET and livekit_api
        ),
        "https_required": NOVEAM_REQUIRE_HTTPS,
        "realtime": "in_process_websocket",
    }


@app.get("/api/health")
@logged_operation
def health():
    system_health()
    return {"service": "Noveagamheam REST API", "status": "ok", "database": 'postgresql'}


@app.get("/api/roles")
@logged_operation
def roles():
    return {"roles": ROLES}


@app.get("/api/permissions/me")
@logged_operation
def my_permissions(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    return {"role": user["role"], "permissions": permissions_for(user["role"])}


@app.get("/api/care/me")
@logged_operation
def care_self(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "care:self")
    audit(user["id"], "read", "care:self")
    return {
        "ok": True,
        "scope": "self",
        "message": "Authorized for your own care workspace.",
    }


@app.get("/api/care/clinical")
@logged_operation
def care_clinical(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "care:clinical")
    audit(user["id"], "read", "care:clinical")
    return {
        "ok": True,
        "scope": "clinical",
        "message": "Authorized for clinician-only care functions.",
    }


@app.get("/api/records/me")
@logged_operation
def records_self(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "records:self")
    audit(user["id"], "read", "records:self")
    return {
        "ok": True,
        "scope": "self",
        "message": "Authorized for your own records workspace.",
    }


@app.get("/api/records/clinical")
@logged_operation
def records_clinical(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "records:clinical")
    audit(user["id"], "read", "records:clinical")
    return {
        "ok": True,
        "scope": "clinical",
        "message": "Authorized for clinician-only records functions.",
    }


@app.post("/api/auth/register", status_code=status.HTTP_201_CREATED)
@logged_operation(level=logging.INFO)
def register(payload: RegisterRequest):
    display_name = (payload.display_name or payload.full_name).strip()
    email = payload.email.strip().lower()
    phone = payload.phone.strip()
    if not display_name or not email or (not phone):
        raise HTTPException(
            status_code=400,
            detail="Display name, email ID and phone number are required",
        )
    if "@" not in email:
        raise HTTPException(status_code=400, detail="Enter a valid email ID")
    base = (payload.username.strip().lower() or email.split("@", 1)[0]).replace(" ", "")
    username = (
        "".join((ch for ch in base if ch.isalnum() or ch in "._-"))[:40] or "user"
    )
    if len(username) < 3:
        username = (username + "user")[:6]
    if payload.role not in SELF_SERVICE_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    password_hash = hash_password(payload.password)
    created_at = datetime.now(timezone.utc).isoformat()
    try:
        with db() as conn:
            candidate = username
            n = 1
            while conn.execute(
                'SELECT 1 FROM users WHERE username=%s', (candidate,)
            ).fetchone():
                n += 1
                candidate = username[:35] + str(n)
            username = candidate
            cur = conn.execute(
                """
                INSERT INTO users(username,full_name,role,password_hash,created_at,email,phone)
                VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    username,
                    display_name,
                    payload.role,
                    password_hash,
                    created_at,
                    email,
                    phone,
                ),
            )
            cur_id = cur.fetchone()["id"]
            user_id = cur_id
            conn.execute(
                'INSERT INTO profiles(user_id,display_name,bio,location) VALUES(%s,%s,%s,%s)',
                (user_id, display_name, "", ""),
            )
            row = conn.execute('SELECT * FROM users WHERE id=%s', (user_id,)).fetchone()
    except psycopg.IntegrityError:
        raise HTTPException(
            status_code=409, detail="Email ID or phone number is already registered"
        )
    token = create_session(user_id)
    return {"token": token, "user": public_user(row)}


@app.post("/api/auth/login")
@logged_operation(level=logging.INFO)
def login(payload: LoginRequest):
    _check_auth_attempt(
        (
            "login",
            hashlib.sha256(
                (payload.identifier or payload.username).strip().lower().encode()
            ).hexdigest(),
        )
    )
    raw_identifier = (payload.identifier or payload.username).strip()
    if not raw_identifier:
        raise HTTPException(
            status_code=400, detail="Enter your email ID, phone number, or username"
        )
    normalized = raw_identifier.lower()
    normalized_phone = "".join(
        (ch for ch in raw_identifier if ch.isdigit() or ch == "+")
    )
    with db() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM users
            WHERE lower(username)=%s
            OR lower(email)=%s
            OR phone=%s
            OR REPLACE(REPLACE(REPLACE(phone, ' ', ''), '-', ''), '(', '')=%s
            LIMIT 1
            """,
            (normalized, normalized, raw_identifier, normalized_phone),
        ).fetchone()
    if not row or not verify_password(payload.password, row["password_hash"]):
        raise HTTPException(
            status_code=401,
            detail="Invalid email ID, phone number, username, or password",
        )
    with db() as conn:
        two = conn.execute(
            'SELECT enabled FROM user_2fa WHERE user_id=%s', (row["id"],)
        ).fetchone()
    if two and int(two["enabled"]) == 1:
        challenge = secrets.token_urlsafe(32)
        expires = datetime.now(timezone.utc) + timedelta(minutes=5)
        with db() as conn:
            conn.execute(
                'DELETE FROM pending_2fa_logins WHERE expires_at < %s',
                (datetime.now(timezone.utc).isoformat(),),
            )
            conn.execute(
                'INSERT INTO pending_2fa_logins(challenge,user_id,expires_at) VALUES(%s,%s,%s)',
                (challenge, row["id"], expires.isoformat()),
            )
        return {"requires_2fa": True, "challenge": challenge}
    token = create_session(row["id"])
    return {"token": token, "user": public_user(row)}


@app.post("/api/auth/2fa/login")
@logged_operation(level=logging.INFO)
def two_factor_login(payload: TwoFactorLoginRequest):
    _check_auth_attempt(("2fa", hashlib.sha256(payload.challenge.encode()).hexdigest()))
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        row = conn.execute(
            """
            SELECT p.user_id,t.secret,t.enabled,t.last_counter
            FROM pending_2fa_logins p
            JOIN user_2fa t ON t.user_id=p.user_id
            WHERE p.challenge=%s
            AND p.expires_at>%s
            FOR UPDATE OF p,t
            """,
            (payload.challenge, now),
        ).fetchone()
        if (
            not row
            or not row["enabled"]
            or (not verify_totp(row["secret"], payload.code))
        ):
            raise HTTPException(401, "Invalid or expired verification code")
        counter = int(datetime.now(timezone.utc).timestamp()) // 30
        used = next(
            (
                counter + d
                for d in (-1, 0, 1)
                if hmac.compare_digest(
                    _totp_code(
                        row["secret"],
                        datetime.fromtimestamp((counter + d) * 30, timezone.utc),
                    ),
                    "".join((ch for ch in payload.code if ch.isdigit())),
                )
            ),
            None,
        )
        if used is None or used <= row["last_counter"]:
            raise HTTPException(401, "Verification code has already been used")
        conn.execute(
            'UPDATE user_2fa SET last_counter=%s WHERE user_id=%s', (used, row["user_id"])
        )
        conn.execute(
            'DELETE FROM pending_2fa_logins WHERE challenge=%s', (payload.challenge,)
        )
        user = conn.execute(
            'SELECT * FROM users WHERE id=%s', (row["user_id"],)
        ).fetchone()
        token = create_session(row["user_id"])
        audit(row["user_id"], "two_factor_login", "account:security")
    return {"token": token, "user": public_user(user)}


@app.get("/api/auth/2fa/status")
@logged_operation
def two_factor_status(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT enabled FROM user_2fa WHERE user_id=%s', (user["id"],)
        ).fetchone()
    return {"enabled": bool(row and int(row["enabled"]) == 1)}


@app.post("/api/auth/2fa/setup")
@logged_operation(level=logging.INFO)
def two_factor_setup(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    secret = _totp_secret()
    with db() as conn:
        enabled = conn.execute(
            'SELECT enabled FROM user_2fa WHERE user_id=%s', (user["id"],)
        ).fetchone()
        if enabled and enabled["enabled"]:
            raise HTTPException(
                409, "Disable existing two-factor authentication before resetting it"
            )
        conn.execute(
            """
            INSERT INTO user_2fa(user_id,secret,enabled,created_at)
            VALUES(%s,%s,0,%s)
            ON CONFLICT(user_id) DO UPDATE
            SET secret=excluded.secret,enabled=0,created_at=excluded.created_at
            """,
            (user["id"], secret, datetime.now(timezone.utc).isoformat()),
        )
    return {"secret": secret}


@app.post("/api/auth/2fa/enable")
@logged_operation(level=logging.INFO)
def two_factor_enable(
    payload: TwoFactorCodeRequest, authorization: Optional[str] = Header(default=None)
):
    user, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT secret FROM user_2fa WHERE user_id=%s', (user["id"],)
        ).fetchone()
    if not row or not verify_totp(row["secret"], payload.code):
        raise HTTPException(status_code=400, detail="Invalid verification code")
    with db() as conn:
        conn.execute('UPDATE user_2fa SET enabled=1 WHERE user_id=%s', (user["id"],))
    audit(user["id"], "enable_2fa", "account:security")
    return {"ok": True, "enabled": True}


@app.post("/api/auth/2fa/disable")
@logged_operation(level=logging.INFO)
def two_factor_disable(
    payload: TwoFactorDisableRequest,
    authorization: Optional[str] = Header(default=None),
):
    user, current_token = current_user(authorization)
    if not verify_password(payload.current_password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    with db() as conn:
        row = conn.execute(
            'SELECT secret,enabled FROM user_2fa WHERE user_id=%s', (user["id"],)
        ).fetchone()
    if (
        not row
        or int(row["enabled"]) != 1
        or (not verify_totp(row["secret"], payload.code))
    ):
        raise HTTPException(status_code=400, detail="Invalid verification code")
    with db() as conn:
        conn.execute('DELETE FROM user_2fa WHERE user_id=%s', (user["id"],))
        conn.execute(
            'DELETE FROM sessions WHERE user_id=%s AND token<>%s',
            (user["id"], current_token),
        )
    audit(user["id"], "disable_2fa", "account:security")
    return {"ok": True, "enabled": False}


@app.post("/api/auth/logout")
@logged_operation(level=logging.INFO)
def logout(authorization: Optional[str] = Header(default=None)):
    _, token = current_user(authorization)
    with db() as conn:
        conn.execute('DELETE FROM sessions WHERE token=%s', (token,))
    return {"ok": True}


@app.get("/api/auth/sessions")
@logged_operation
def list_sessions(authorization: Optional[str] = Header(default=None)):
    user, current_token = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        conn.execute('DELETE FROM sessions WHERE expires_at < %s', (now,))
        rows = conn.execute(
            """
            SELECT token, expires_at, created_at
            FROM sessions
            WHERE user_id=%s
            ORDER BY CASE WHEN token=%s THEN 0 ELSE 1 END, created_at DESC
            """,
            (user["id"], current_token),
        ).fetchall()
    return {
        "sessions": [
            {
                "id": r["token"][:12],
                "current": r["token"] == current_token,
                "created_at": r["created_at"] or None,
                "expires_at": r["expires_at"],
            }
            for r in rows
        ]
    }


@app.post("/api/auth/logout-others")
@logged_operation(level=logging.INFO)
def logout_other_sessions(authorization: Optional[str] = Header(default=None)):
    user, current_token = current_user(authorization)
    with db() as conn:
        cur = conn.execute(
            'DELETE FROM sessions WHERE user_id=%s AND token<>%s',
            (user["id"], current_token),
        )
        removed = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
    audit(user["id"], "logout_other_sessions", "account:security")
    return {"ok": True, "removed": removed, "message": "Other sessions logged out"}


@app.post("/api/auth/logout-all")
@logged_operation(level=logging.INFO)
def logout_all_sessions(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        cur = conn.execute('DELETE FROM sessions WHERE user_id=%s', (user["id"],))
        removed = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
    audit(user["id"], "logout_all_sessions", "account:security")
    return {"ok": True, "removed": removed, "message": "All sessions logged out"}


@app.post("/api/auth/change-password")
@logged_operation(level=logging.INFO)
def change_password(
    payload: ChangePasswordRequest, authorization: Optional[str] = Header(default=None)
):
    user, current_token = current_user(authorization)
    if not verify_password(payload.current_password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if payload.current_password == payload.new_password:
        raise HTTPException(
            status_code=400,
            detail="New password must be different from the current password",
        )
    new_hash = hash_password(payload.new_password)
    with db() as conn:
        conn.execute(
            'UPDATE users SET password_hash=%s WHERE id=%s', (new_hash, user["id"])
        )
        conn.execute(
            'DELETE FROM sessions WHERE user_id=%s AND token<>%s',
            (user["id"], current_token),
        )
    audit(user["id"], "change_password", "account:security")
    return {"ok": True, "message": "Password changed successfully"}


@app.get("/api/settings/me")
@logged_operation
def get_user_settings(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM user_settings WHERE user_id=%s', (user["id"],)
        ).fetchone()
    if not row:
        return {
            "theme": "system",
            "language": "en",
            "notifications": True,
            "profile_visibility": "connections",
            "activity_status": True,
            "connection_requests": True,
        }
    return {
        "theme": row["theme"],
        "language": row["language"],
        "notifications": bool(row["notifications"]),
        "profile_visibility": row["profile_visibility"],
        "activity_status": bool(row["activity_status"]),
        "connection_requests": bool(row["connection_requests"]),
    }


@app.put("/api/settings/me")
@logged_operation(level=logging.INFO)
def update_user_settings(
    payload: UserSettingsUpdate, authorization: Optional[str] = Header(default=None)
):
    user, _ = current_user(authorization)
    if payload.theme not in {"system", "light", "dark"}:
        raise HTTPException(status_code=400, detail="Invalid theme")
    if payload.profile_visibility not in {"public", "connections", "private"}:
        raise HTTPException(status_code=400, detail="Invalid profile visibility")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        conn.execute(
            """
            INSERT INTO user_settings(user_id,theme,language,notifications,profile_visibility,activity_status,connection_requests,updated_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(user_id) DO UPDATE
            SET theme=excluded.theme, language=excluded.language, notifications=excluded.notifications, profile_visibility=excluded.profile_visibility, activity_status=excluded.activity_status, connection_requests=excluded.connection_requests, updated_at=excluded.updated_at
            """,
            (
                user["id"],
                payload.theme,
                payload.language,
                int(payload.notifications),
                payload.profile_visibility,
                int(payload.activity_status),
                int(payload.connection_requests),
                now,
            ),
        )
    audit(user["id"], "update", "settings:self")
    return {"ok": True, "message": "Settings saved to your NOVEAM account"}


@app.get("/api/users/me")
@logged_operation
def me(authorization: Optional[str] = Header(default=None)):
    row, _ = current_user(authorization)
    data = public_user(row)
    data["email"] = row["email"] if "email" in row.keys() else ""
    data["phone"] = row["phone"] if "phone" in row.keys() else ""
    with db() as conn:
        profile = conn.execute(
            'SELECT display_name FROM profiles WHERE user_id=%s', (row["id"],)
        ).fetchone()
    data["display_name"] = profile["display_name"] if profile else row["full_name"]
    return data


@app.put("/api/account/contact")
@logged_operation(level=logging.INFO)
def update_account_contact(
    payload: AccountContactUpdate, authorization: Optional[str] = Header(default=None)
):
    user, _ = current_user(authorization)
    if not verify_password(payload.current_password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    email = payload.email.strip().lower()
    phone = payload.phone.strip()
    if not email or "@" not in email or email.startswith("@") or email.endswith("@"):
        raise HTTPException(status_code=400, detail="Enter a valid email address")
    if not phone:
        raise HTTPException(status_code=400, detail="Enter a phone number")
    normalized_phone = "".join((ch for ch in phone if ch.isdigit() or ch == "+"))
    if len("".join((ch for ch in normalized_phone if ch.isdigit()))) < 7:
        raise HTTPException(status_code=400, detail="Enter a valid phone number")
    with db() as conn:
        duplicate_email = conn.execute(
            'SELECT id FROM users WHERE lower(email)=%s AND id<>%s', (email, user["id"])
        ).fetchone()
        if duplicate_email:
            raise HTTPException(
                status_code=409, detail="That email address is already registered"
            )
        duplicate_phone = conn.execute(
            "SELECT id FROM users WHERE REPLACE(REPLACE(REPLACE(phone,' ',''),'-',''),'(','')=%s AND id<>%s",
            (normalized_phone, user["id"]),
        ).fetchone()
        if duplicate_phone:
            raise HTTPException(
                status_code=409, detail="That phone number is already registered"
            )
        try:
            conn.execute(
                'UPDATE users SET email=%s, phone=%s WHERE id=%s',
                (email, phone, user["id"]),
            )
        except psycopg.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="Email address or phone number is already registered",
            )
    audit(user["id"], "update_contact", "account:self")
    return {
        "ok": True,
        "email": email,
        "phone": phone,
        "message": "Account contact details updated",
    }


@app.delete("/api/account/me")
@logged_operation(level=logging.INFO)
def delete_account(
    payload: DeleteAccountRequest, authorization: Optional[str] = Header(default=None)
):
    user, _ = current_user(authorization)
    if payload.confirmation.strip().upper() != "DELETE":
        raise HTTPException(400, "Type DELETE to confirm permanent account deletion")
    if not verify_password(payload.current_password, user["password_hash"]):
        raise HTTPException(400, "Current password is incorrect")
    with db() as conn:
        files = [
            r["stored_name"]
            for r in conn.execute(
                'SELECT stored_name FROM uploaded_files WHERE owner_user_id=%s OR patient_user_id=%s',
                (user["id"], user["id"]),
            )
        ]
        with conn.cursor() as cursor:
            cursor.executemany(
                'INSERT INTO pending_file_cleanup(stored_name) VALUES(%s) ON CONFLICT DO NOTHING',
                [(name,) for name in files],
            )
        conn.execute(
            "UPDATE audit_log SET user_id=NULL,resource='account:deleted' WHERE user_id=%s",
            (user["id"],),
        )
        tables = [
            r["name"]
            for r in conn.execute(
                "SELECT table_name AS name FROM information_schema.tables WHERE table_schema=current_schema() AND table_type='BASE TABLE'"
            )
        ]
        for table in tables:
            if table in {"users", "audit_log"}:
                continue
            cols = {
                r["name"] for r in conn.execute(
                    "SELECT column_name AS name FROM information_schema.columns "
                    "WHERE table_schema=current_schema() AND table_name=%s",
                    (table,),
                )
            }
            if "user_id" in cols:
                conn.execute(pg_sql.SQL("DELETE FROM {} WHERE user_id=%s").format(pg_sql.Identifier(table)), (user["id"],))
        conn.execute('DELETE FROM users WHERE id=%s', (user["id"],))
        audit(None, "account_deleted", "account:deleted")
    flush_file_cleanup()
    return {"ok": True, "message": "Account permanently deleted"}


@app.get("/api/profiles/me")
@logged_operation
def my_profile(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "profile:read")
    audit(user["id"], "read", "profile:self")
    with db() as conn:
        profile = conn.execute(
            'SELECT * FROM profiles WHERE user_id=%s', (user["id"],)
        ).fetchone()
    return {
        "user": public_user(user),
        "account": {
            "email": user["email"] if "email" in user.keys() else "",
            "phone": user["phone"] if "phone" in user.keys() else "",
        },
        "profile": dict(profile) if profile else None,
    }


@app.put("/api/profiles/me")
@logged_operation(level=logging.INFO)
def update_profile(
    payload: ProfileUpdate, authorization: Optional[str] = Header(default=None)
):
    user, _ = current_user(authorization)
    require_permission(user, "profile:write")
    if payload.role not in ROLES or (
        payload.role != user["role"] and payload.role not in SELF_SERVICE_ROLES
    ):
        raise HTTPException(status_code=400, detail="Invalid role")
    email = payload.email.strip().lower()
    phone = payload.phone.strip()
    if email and "@" not in email:
        raise HTTPException(status_code=400, detail="Enter a valid email ID")
    values = [
        payload.display_name.strip(),
        payload.headline.strip(),
        payload.bio.strip(),
        payload.photo.strip(),
        payload.cover.strip(),
        payload.pronouns.strip(),
        payload.location.strip(),
        payload.languages.strip(),
        payload.occupation.strip(),
        payload.organization.strip(),
        payload.education.strip(),
        payload.skills.strip(),
        payload.interests.strip(),
        payload.website.strip(),
        payload.linkedin.strip(),
        payload.youtube.strip(),
        payload.instagram.strip(),
        payload.xlink.strip(),
        payload.achievements.strip(),
        user["id"],
    ]
    with db() as conn:
        if email:
            duplicate_email = conn.execute(
                'SELECT id FROM users WHERE lower(email)=%s AND id<>%s',
                (email, user["id"]),
            ).fetchone()
            if duplicate_email:
                raise HTTPException(
                    status_code=409,
                    detail="Email ID is already registered to another account",
                )
        if phone:
            duplicate_phone = conn.execute(
                'SELECT id FROM users WHERE phone=%s AND id<>%s', (phone, user["id"])
            ).fetchone()
            if duplicate_phone:
                raise HTTPException(
                    status_code=409,
                    detail="Phone number is already registered to another account",
                )
        conn.execute(
            'UPDATE users SET full_name=%s, role=%s, email=%s, phone=%s WHERE id=%s',
            (payload.display_name.strip(), payload.role, email, phone, user["id"]),
        )
        conn.execute(
            """
            UPDATE profiles
            SET display_name=%s, headline=%s, bio=%s, photo=%s, cover=%s, pronouns=%s, location=%s, languages=%s, occupation=%s, organization=%s, education=%s, skills=%s, interests=%s, website=%s, linkedin=%s, youtube=%s, instagram=%s, xlink=%s, achievements=%s
            WHERE user_id=%s
            """,
            values,
        )
        profile = conn.execute(
            'SELECT * FROM profiles WHERE user_id=%s', (user["id"],)
        ).fetchone()
        updated_user = conn.execute(
            'SELECT * FROM users WHERE id=%s', (user["id"],)
        ).fetchone()
    audit(user["id"], "update", "profile:self")
    return {
        "user": public_user(updated_user),
        "account": {"email": updated_user["email"], "phone": updated_user["phone"]},
        "profile": dict(profile),
    }


@app.get("/api/tasks")
@logged_operation
def list_tasks(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            'SELECT * FROM tasks WHERE user_id=%s ORDER BY task_date,time,id',
            (user["id"],),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/tasks", status_code=201)
@logged_operation(level=logging.INFO)
def add_task(task: Task, authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO tasks(user_id,task_date,time,title,details,status,responsible_person,comment)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                user["id"],
                str(task.task_date),
                task.time,
                task.title,
                task.details,
                task.status,
                task.responsible_person,
                task.comment,
            ),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM tasks WHERE id=%s AND user_id=%s', (cur_id, user["id"])
        ).fetchone()
    audit(user["id"], "create", "task")
    return dict(row)


@app.put("/api/tasks/{task_id}")
@logged_operation(level=logging.INFO)
def update_task(
    task_id: int, task: Task, authorization: Optional[str] = Header(default=None)
):
    user, _ = current_user(authorization)
    with db() as conn:
        cur = conn.execute(
            """
            UPDATE tasks
            SET task_date=%s,time=%s,title=%s,details=%s,status=%s,responsible_person=%s,comment=%s
            WHERE id=%s
            AND user_id=%s
            """,
            (
                str(task.task_date),
                task.time,
                task.title,
                task.details,
                task.status,
                task.responsible_person,
                task.comment,
                task_id,
                user["id"],
            ),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Task not found")
        row = conn.execute(
            'SELECT * FROM tasks WHERE id=%s AND user_id=%s', (task_id, user["id"])
        ).fetchone()
    audit(user["id"], "update", "task")
    return dict(row)


@app.get("/api/care/directory")
@logged_operation
def care_directory(q: str = "", authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    term = f"%{q.strip().lower()}%"
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id,username,full_name,role
            FROM users
            WHERE id<>%s
            AND role IN ('Doctor','Parent','Sibling','Guardian','Friend')
            AND (LOWER(username) ILIKE %s
            OR LOWER(full_name) ILIKE %s)
            ORDER BY full_name
            LIMIT 30
            """,
            (user["id"], term, term),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/care/access-requests", status_code=201)
@logged_operation(level=logging.INFO)
def create_access_request(
    payload: AccessRequestCreate, authorization: Optional[str] = Header(default=None)
):
    requester, _ = current_user(authorization)
    allowed_requesters = {"Doctor", "Parent", "Sibling", "Guardian", "Friend"}
    if requester["role"] not in allowed_requesters:
        raise HTTPException(
            status_code=403, detail="This role cannot request patient access."
        )
    if requester["role"] != "Doctor" and payload.permission == "edit":
        raise HTTPException(
            status_code=403, detail="Support roles may request view access only."
        )
    with db() as conn:
        patient = conn.execute(
            "SELECT * FROM users WHERE username=%s AND role='Patient'",
            (payload.patient_username.strip().lower(),),
        ).fetchone()
        if not patient:
            raise HTTPException(status_code=404, detail="Patient account not found.")
        existing = conn.execute(
            """
            SELECT id
            FROM care_access_requests
            WHERE patient_user_id=%s
            AND requester_user_id=%s
            AND status IN ('pending','approved')
            """,
            (patient["id"], requester["id"]),
        ).fetchone()
        if existing:
            raise HTTPException(
                status_code=409,
                detail="An active or pending relationship already exists.",
            )
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            """
            INSERT INTO care_access_requests (patient_user_id,requester_user_id,requester_role,scope,permission,note,status,created_at)
            VALUES(%s,%s,%s,%s,%s,%s, 'pending', %s) RETURNING id""",
            (
                patient["id"],
                requester["id"],
                requester["role"],
                payload.scope,
                payload.permission,
                payload.note.strip(),
                now,
            ),
        )
        cur_id = cur.fetchone()["id"]
        row = conn.execute(
            'SELECT * FROM care_access_requests WHERE id=%s', (cur_id,)
        ).fetchone()
    audit(requester["id"], "create", f"care_access_request:{row['id']}")
    return access_row_dict(row)


@app.get("/api/care/access-requests/me")
@logged_operation
def my_access_requests(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT r.*, p.username AS patient_username,p.full_name AS patient_name, v.username AS requester_username,v.full_name AS requester_name
            FROM care_access_requests r
            JOIN users p ON p.id=r.patient_user_id
            JOIN users v ON v.id=r.requester_user_id
            WHERE r.patient_user_id=%s
            OR r.requester_user_id=%s
            ORDER BY r.id DESC
            """,
            (user["id"], user["id"]),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/care/access-requests/{request_id}/approve")
@logged_operation(level=logging.INFO)
def approve_access_request(
    request_id: int, authorization: Optional[str] = Header(default=None)
):
    return patient_decide_request(request_id, "approved", authorization)


@app.post("/api/care/access-requests/{request_id}/deny")
@logged_operation(level=logging.INFO)
def deny_access_request(
    request_id: int, authorization: Optional[str] = Header(default=None)
):
    return patient_decide_request(request_id, "denied", authorization)


@app.post("/api/care/access-requests/{request_id}/revoke")
@logged_operation(level=logging.INFO)
def revoke_access_request(
    request_id: int, authorization: Optional[str] = Header(default=None)
):
    user, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute(
            'SELECT * FROM care_access_requests WHERE id=%s', (request_id,)
        ).fetchone()
        if not row:
            raise HTTPException(
                status_code=404, detail="Access relationship not found."
            )
        if user["id"] not in (row["patient_user_id"], row["requester_user_id"]):
            raise HTTPException(
                status_code=403, detail="You cannot revoke this relationship."
            )
        if row["status"] not in ("pending", "approved"):
            raise HTTPException(
                status_code=409, detail="This relationship is not active."
            )
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE care_access_requests SET status='revoked',revoked_at=%s WHERE id=%s",
            (now, request_id),
        )
        updated = conn.execute(
            'SELECT * FROM care_access_requests WHERE id=%s', (request_id,)
        ).fetchone()
    audit(user["id"], "revoke", f"care_access_request:{request_id}")
    return access_row_dict(updated)


@app.get("/api/care/patients/{patient_username}")
@logged_operation
def authorized_patient_care(
    patient_username: str, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        patient = conn.execute(
            "SELECT id,username,full_name,role FROM users WHERE username=%s AND role='Patient'",
            (patient_username.strip().lower(),),
        ).fetchone()
    if not patient:
        raise HTTPException(status_code=404, detail="Patient account not found.")
    require_patient_access(viewer, patient["id"], "care", "view")
    audit(viewer["id"], "read", f"patient_care:{patient['id']}")
    return {
        "authorized": True,
        "patient": dict(patient),
        "scope": "care",
        "note": "Step 12 authorization check passed; no clinical data is stored here yet.",
    }


@app.get("/api/records/patients/{patient_username}")
@logged_operation
def authorized_patient_records(
    patient_username: str, authorization: Optional[str] = Header(default=None)
):
    viewer, _ = current_user(authorization)
    with db() as conn:
        patient = conn.execute(
            "SELECT id,username,full_name,role FROM users WHERE username=%s AND role='Patient'",
            (patient_username.strip().lower(),),
        ).fetchone()
    if not patient:
        raise HTTPException(status_code=404, detail="Patient account not found.")
    require_patient_access(viewer, patient["id"], "records", "view")
    audit(viewer["id"], "read", f"patient_records:{patient['id']}")
    return {
        "authorized": True,
        "patient": dict(patient),
        "scope": "records",
        "note": "Step 12 authorization check passed; no medical records are stored here yet.",
    }


# 6. Frontend fallback (after API routes)

if FRONTEND_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:

    @app.get("/")
    @logged_operation
    def frontend_missing():
        raise HTTPException(503, "Frontend unavailable; configure NOVEAM_FRONTEND_DIR")
