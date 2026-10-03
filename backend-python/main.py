from collections import defaultdict, deque
import time
from fastapi import FastAPI, HTTPException, Header, status, UploadFile, File, Form, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime, timedelta, timezone
import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import uuid
from pathlib import Path
import firebase_admin
from firebase_admin import credentials, firestore

try:
    from livekit import api as livekit_api
except ImportError:
    livekit_api = None

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("NOVEAGAMHEAM_DB_PATH", str(BASE_DIR / "noveagamheam.db")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
# ---------------------------------------------------------
# Firebase / Firestore
# ---------------------------------------------------------

FIREBASE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID", "").strip()
FIREBASE_CLIENT_EMAIL = os.environ.get("FIREBASE_CLIENT_EMAIL", "").strip()
FIREBASE_PRIVATE_KEY = os.environ.get("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n").strip()

firestore_db = None

if FIREBASE_PROJECT_ID and FIREBASE_CLIENT_EMAIL and FIREBASE_PRIVATE_KEY:
    try:
        firebase_credentials = credentials.Certificate({
            "type": "service_account",
            "project_id": FIREBASE_PROJECT_ID,
            "client_email": FIREBASE_CLIENT_EMAIL,
            "private_key": FIREBASE_PRIVATE_KEY,
            "token_uri": "https://oauth2.googleapis.com/token",
        })

        if not firebase_admin._apps:
            firebase_admin.initialize_app(firebase_credentials)

        firestore_db = firestore.client()

        print("Firebase Firestore initialized successfully")

    except Exception as e:
        print("Firebase initialization failed:", str(e))
else:
    print("Firebase environment variables are not configured")

# Serve the existing frontend and REST API from the same FastAPI application.
# Normal source layout:
#   noveagamheam_build/
#     backend-python/main.py
#     frontend-web/index.html
# A packaged build can override this with NOVEAGAMHEAM_FRONTEND_DIR.
FRONTEND_DIR = Path(
    os.environ.get("NOVEAGAMHEAM_FRONTEND_DIR", str(BASE_DIR.parent / "frontend-web"))
).resolve()

LIVEKIT_URL = os.environ.get("LIVEKIT_URL", "").strip()
LIVEKIT_API_KEY = os.environ.get("LIVEKIT_API_KEY", "").strip()
LIVEKIT_API_SECRET = os.environ.get("LIVEKIT_API_SECRET", "").strip()

NOVEAM_ENV = os.environ.get("NOVEAM_ENV","development").strip().lower()
NOVEAM_REQUIRE_HTTPS = os.environ.get("NOVEAM_REQUIRE_HTTPS","0").strip() in {"1","true","yes"}
NOVEAM_MAX_REQUEST_BYTES = int(os.environ.get("NOVEAM_MAX_REQUEST_BYTES","10485760"))
NOVEAM_RATE_LIMIT_PER_MINUTE = int(os.environ.get("NOVEAM_RATE_LIMIT_PER_MINUTE","180"))
NOVEAM_TRUST_PROXY_HTTPS = os.environ.get("NOVEAM_TRUST_PROXY_HTTPS","0").strip() in {"1","true","yes"}

_rate_buckets=defaultdict(deque)

def _client_key(request):
    return request.client.host if request.client else "unknown"


class NoveamRealtimeHub:
    def __init__(self): self.clients={}
    async def connect(self,uid,ws):
        await ws.accept(); self.clients.setdefault(uid,set()).add(ws)
    def disconnect(self,uid,ws):
        g=self.clients.get(uid,set()); g.discard(ws)
        if not g: self.clients.pop(uid,None)
    async def send(self,uid,event):
        for ws in list(self.clients.get(uid,set())):
            try: await ws.send_json(event)
            except Exception: self.disconnect(uid,ws)
    def online(self,uid): return bool(self.clients.get(uid))
realtime_hub=NoveamRealtimeHub()


app = FastAPI(title="Noveagamheam REST API", version="1.7.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://noveam.in",
        "https://www.noveam.in",
        "http://localhost:8000",
        "http://127.0.0.1:8000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],


)

ROLES = [
    "Patient", "Doctor", "Parent", "Sibling", "Guardian", "Friend",
    "Gym Trainer", "Coach/Instructor", "Founder", "Co-Founder", "Owner",
    "Director", "Management", "Principal", "Dean", "Head/HOD", "Teacher",
    "Faculty", "Staff", "Student/Learner", "Member", "Volunteer",
    "Partner/Collaborator", "Other"
]

# Step 11: centralized role-based authorization.
# Permissions control protected backend actions; they do not remove the general
# Noveagamheam modules from the user's dashboard.
PERMISSIONS = {
    "Patient": {"profile:read", "profile:write", "care:self", "records:self", "wellness:use"},
    "Doctor": {"profile:read", "profile:write", "care:self", "care:clinical", "records:self", "records:clinical", "wellness:use"},
    "Parent": {"profile:read", "profile:write", "care:self", "care:support", "records:self", "wellness:use"},
    "Sibling": {"profile:read", "profile:write", "care:self", "care:support", "records:self", "wellness:use"},
    "Guardian": {"profile:read", "profile:write", "care:self", "care:support", "records:self", "wellness:use"},
    "Friend": {"profile:read", "profile:write", "care:self", "care:support", "records:self", "wellness:use"},
    "Gym Trainer": {"profile:read", "profile:write", "wellness:use", "fitness:coach"},
    "Coach/Instructor": {"profile:read", "profile:write", "wellness:use", "fitness:coach"},
}
DEFAULT_PERMISSIONS = {"profile:read", "profile:write", "wellness:use"}


def permissions_for(role: str):
    return sorted(PERMISSIONS.get(role, DEFAULT_PERMISSIONS))


def require_permission(user, permission: str):
    if permission not in set(permissions_for(user["role"])):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Your role does not have permission: {permission}",
        )


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS profiles (
            user_id INTEGER PRIMARY KEY,
            display_name TEXT NOT NULL,
            headline TEXT NOT NULL DEFAULT '',
            bio TEXT NOT NULL DEFAULT '',
            photo TEXT NOT NULL DEFAULT '',
            cover TEXT NOT NULL DEFAULT '',
            pronouns TEXT NOT NULL DEFAULT '',
            location TEXT NOT NULL DEFAULT '',
            languages TEXT NOT NULL DEFAULT '',
            occupation TEXT NOT NULL DEFAULT '',
            organization TEXT NOT NULL DEFAULT '',
            education TEXT NOT NULL DEFAULT '',
            skills TEXT NOT NULL DEFAULT '',
            interests TEXT NOT NULL DEFAULT '',
            website TEXT NOT NULL DEFAULT '',
            linkedin TEXT NOT NULL DEFAULT '',
            youtube TEXT NOT NULL DEFAULT '',
            instagram TEXT NOT NULL DEFAULT '',
            xlink TEXT NOT NULL DEFAULT '',
            achievements TEXT NOT NULL DEFAULT '',
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT '',
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS user_2fa (
            user_id INTEGER PRIMARY KEY, secret TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS pending_2fa_logins (
            challenge TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            theme TEXT NOT NULL DEFAULT 'system',
            language TEXT NOT NULL DEFAULT 'en',
            notifications INTEGER NOT NULL DEFAULT 1,
            profile_visibility TEXT NOT NULL DEFAULT 'connections',
            activity_status INTEGER NOT NULL DEFAULT 1,
            connection_requests INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT NOT NULL,
            resource TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            task_date TEXT NOT NULL,
            time TEXT NOT NULL,
            title TEXT NOT NULL,
            details TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'To-Do',
            responsible_person TEXT NOT NULL DEFAULT 'Self',
            comment TEXT NOT NULL DEFAULT '',
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)
        # Step 10: safely add expanded profile columns to existing Step 9 databases.
        existing = {row["name"] for row in conn.execute("PRAGMA table_info(profiles)").fetchall()}
        profile_columns = {
            "headline": "TEXT NOT NULL DEFAULT ''", "photo": "TEXT NOT NULL DEFAULT ''",
            "cover": "TEXT NOT NULL DEFAULT ''", "pronouns": "TEXT NOT NULL DEFAULT ''",
            "languages": "TEXT NOT NULL DEFAULT ''", "occupation": "TEXT NOT NULL DEFAULT ''",
            "organization": "TEXT NOT NULL DEFAULT ''", "education": "TEXT NOT NULL DEFAULT ''",
            "skills": "TEXT NOT NULL DEFAULT ''", "interests": "TEXT NOT NULL DEFAULT ''",
            "website": "TEXT NOT NULL DEFAULT ''", "linkedin": "TEXT NOT NULL DEFAULT ''",
            "youtube": "TEXT NOT NULL DEFAULT ''", "instagram": "TEXT NOT NULL DEFAULT ''",
            "xlink": "TEXT NOT NULL DEFAULT ''", "achievements": "TEXT NOT NULL DEFAULT ''",
        }
        for name, definition in profile_columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE profiles ADD COLUMN {name} {definition}")
        session_existing = {row["name"] for row in conn.execute("PRAGMA table_info(sessions)").fetchall()}
        if "created_at" not in session_existing:
            conn.execute("ALTER TABLE sessions ADD COLUMN created_at TEXT NOT NULL DEFAULT ''")
        task_existing = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        if "user_id" not in task_existing:
            conn.execute("ALTER TABLE tasks ADD COLUMN user_id INTEGER")


@app.on_event("startup")
def startup():
    init_db()


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 210_000)
    return "pbkdf2_sha256$210000$%s$%s" % (
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, iterations, salt_b64, digest_b64 = encoded.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(digest_b64)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iterations))
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def _totp_secret() -> str:
    return base64.b32encode(os.urandom(20)).decode("ascii").rstrip("=")

def _totp_code(secret: str, when: Optional[datetime] = None) -> str:
    now = when or datetime.now(timezone.utc)
    counter = int(now.timestamp()) // 30
    padded = secret + "=" * ((8 - len(secret) % 8) % 8)
    key = base64.b32decode(padded, casefold=True)
    digest = hmac.new(key, counter.to_bytes(8, "big"), hashlib.sha1).digest()
    offset = digest[-1] & 15
    value = ((digest[offset] & 127) << 24 | digest[offset+1] << 16 | digest[offset+2] << 8 | digest[offset+3])
    return str(value % 1000000).zfill(6)

def verify_totp(secret: str, code: str) -> bool:
    clean = "".join(c for c in str(code) if c.isdigit())
    if len(clean) != 6: return False
    now = datetime.now(timezone.utc)
    return any(hmac.compare_digest(_totp_code(secret, now + timedelta(seconds=30*d)), clean) for d in (-1,0,1))

def public_user(row):
    return {
        "id": row["id"],
        "username": row["username"],
        "full_name": row["full_name"],
        "role": row["role"],
        "created_at": row["created_at"],
    }


def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(40)
    expires = datetime.now(timezone.utc) + timedelta(days=7)
    with db() as conn:
        conn.execute("DELETE FROM sessions WHERE expires_at < ?", (datetime.now(timezone.utc).isoformat(),))
        conn.execute(
            "INSERT INTO sessions(token,user_id,expires_at,created_at) VALUES(?,?,?,?)",
            (token, user_id, expires.isoformat(), datetime.now(timezone.utc).isoformat()),
        )
    return token


def current_user(authorization: Optional[str]):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authentication required")
    token = authorization[7:].strip()
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        row = conn.execute(
            """SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id
               WHERE s.token=? AND s.expires_at>?""",
            (token, now),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    return row, token


def audit(user_id: Optional[int], action: str, resource: str):
    with db() as conn:
        conn.execute(
            "INSERT INTO audit_log(user_id,action,resource,created_at) VALUES(?,?,?,?)",
            (user_id, action, resource, datetime.now(timezone.utc).isoformat()),
        )


class RegisterRequest(BaseModel):
    username: str = Field(default="", max_length=40)
    full_name: str = Field(default="", max_length=100)
    display_name: str = Field(default="", max_length=100)
    email: str = Field(default="", max_length=254)
    phone: str = Field(default="", max_length=30)
    role: str
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    # Keep "username" for existing frontend compatibility.
    # "identifier" also allows newer clients to explicitly send email/phone/username.
    username: str = ""
    identifier: str = ""
    password: str


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(min_length=1, max_length=128)
    new_password: str = Field(min_length=8, max_length=128)


class TwoFactorCodeRequest(BaseModel):
    code: str = Field(min_length=6, max_length=12)

class TwoFactorDisableRequest(BaseModel):
    current_password: str = Field(min_length=1, max_length=128)
    code: str = Field(min_length=6, max_length=12)

class TwoFactorLoginRequest(BaseModel):
    challenge: str = Field(min_length=20, max_length=200)
    code: str = Field(min_length=6, max_length=12)

class AccountContactUpdate(BaseModel):
    email: str = Field(default="", max_length=254)
    phone: str = Field(default="", max_length=30)
    current_password: str = Field(min_length=1, max_length=128)


class DeleteAccountRequest(BaseModel):
    current_password: str = Field(min_length=1, max_length=128)
    confirmation: str = Field(min_length=1, max_length=30)


class UserSettingsUpdate(BaseModel):
    theme: str = Field(default="system", max_length=20)
    language: str = Field(default="en", max_length=20)
    notifications: bool = True
    profile_visibility: str = Field(default="connections", max_length=30)
    activity_status: bool = True
    connection_requests: bool = True


class ProfileUpdate(BaseModel):
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


class Task(BaseModel):
    id: Optional[int] = None
    task_date: date
    time: str
    title: str
    details: str = ""
    status: str = "To-Do"
    responsible_person: str = "Self"
    comment: str = ""


@app.get("/api/health")
def health():
    return {"service": "Noveagamheam REST API", "status": "ok", "database": "sqlite"}


@app.get("/api/roles")
def roles():
    return {"roles": ROLES}


@app.get("/api/permissions/me")
def my_permissions(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    return {"role": user["role"], "permissions": permissions_for(user["role"])}


@app.get("/api/care/me")
def care_self(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "care:self")
    audit(user["id"], "read", "care:self")
    return {"ok": True, "scope": "self", "message": "Authorized for your own care workspace."}


@app.get("/api/care/clinical")
def care_clinical(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "care:clinical")
    audit(user["id"], "read", "care:clinical")
    return {"ok": True, "scope": "clinical", "message": "Authorized for clinician-only care functions."}


@app.get("/api/records/me")
def records_self(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "records:self")
    audit(user["id"], "read", "records:self")
    return {"ok": True, "scope": "self", "message": "Authorized for your own records workspace."}


@app.get("/api/records/clinical")
def records_clinical(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "records:clinical")
    audit(user["id"], "read", "records:clinical")
    return {"ok": True, "scope": "clinical", "message": "Authorized for clinician-only records functions."}


@app.post("/api/auth/register", status_code=status.HTTP_201_CREATED)
def register(payload: RegisterRequest):
    display_name = (payload.display_name or payload.full_name).strip()
    email = payload.email.strip().lower()
    phone = payload.phone.strip()
    if not display_name or not email or not phone:
        raise HTTPException(status_code=400, detail="Display name, email ID and phone number are required")
    if "@" not in email:
        raise HTTPException(status_code=400, detail="Enter a valid email ID")
    base = (payload.username.strip().lower() or email.split("@",1)[0]).replace(" ","")
    username = ''.join(ch for ch in base if ch.isalnum() or ch in '._-')[:40] or 'user'
    if len(username) < 3: username = (username + 'user')[:6]
    if payload.role not in ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    password_hash = hash_password(payload.password)
    created_at = datetime.now(timezone.utc).isoformat()
    try:
        with db() as conn:
            candidate=username; n=1
            while conn.execute("SELECT 1 FROM users WHERE username=?",(candidate,)).fetchone():
                n+=1; candidate=(username[:35]+str(n))
            username=candidate
            cur = conn.execute(
                "INSERT INTO users(username,full_name,role,password_hash,created_at,email,phone) VALUES(?,?,?,?,?,?,?)",
                (username, display_name, payload.role, password_hash, created_at, email, phone),
            )
            user_id = cur.lastrowid
            conn.execute(
                "INSERT INTO profiles(user_id,display_name,bio,location) VALUES(?,?,?,?)",
                (user_id, display_name, "", ""),
            )
            row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Email ID or phone number is already registered")
    token = create_session(user_id)
    return {"token": token, "user": public_user(row)}


@app.post("/api/auth/login")
def login(payload: LoginRequest):
    # Accept username, email ID, or phone number without changing existing clients.
    raw_identifier = (payload.identifier or payload.username).strip()
    if not raw_identifier:
        raise HTTPException(status_code=400, detail="Enter your email ID, phone number, or username")

    normalized = raw_identifier.lower()
    normalized_phone = "".join(ch for ch in raw_identifier if ch.isdigit() or ch == "+")
    with db() as conn:
        row = conn.execute(
            """SELECT * FROM users
               WHERE lower(username)=?
                  OR lower(email)=?
                  OR phone=?
                  OR REPLACE(REPLACE(REPLACE(phone, ' ', ''), '-', ''), '(', '')=? 
               LIMIT 1""",
            (normalized, normalized, raw_identifier, normalized_phone),
        ).fetchone()

    # Passwords created by this backend use PBKDF2 and are verified with verify_password().
    if not row or not verify_password(payload.password, row["password_hash"]):
        raise HTTPException(
            status_code=401,
            detail="Invalid email ID, phone number, username, or password",
        )

    with db() as conn:
        two = conn.execute("SELECT enabled FROM user_2fa WHERE user_id=?", (row["id"],)).fetchone()
    if two and int(two["enabled"]) == 1:
        challenge = secrets.token_urlsafe(32)
        expires = datetime.now(timezone.utc) + timedelta(minutes=5)
        with db() as conn:
            conn.execute("DELETE FROM pending_2fa_logins WHERE expires_at < ?", (datetime.now(timezone.utc).isoformat(),))
            conn.execute("INSERT INTO pending_2fa_logins(challenge,user_id,expires_at) VALUES(?,?,?)",(challenge,row["id"],expires.isoformat()))
        return {"requires_2fa": True, "challenge": challenge}
    token = create_session(row["id"])
    return {"token": token, "user": public_user(row)}


@app.post("/api/auth/2fa/login")
def two_factor_login(payload: TwoFactorLoginRequest):
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        row=conn.execute("""SELECT p.user_id,t.secret,t.enabled FROM pending_2fa_logins p
            JOIN user_2fa t ON t.user_id=p.user_id WHERE p.challenge=? AND p.expires_at>?""",(payload.challenge,now)).fetchone()
    if not row or int(row["enabled"])!=1 or not verify_totp(row["secret"],payload.code):
        raise HTTPException(status_code=401,detail="Invalid or expired verification code")
    with db() as conn:
        conn.execute("DELETE FROM pending_2fa_logins WHERE challenge=?",(payload.challenge,))
        user=conn.execute("SELECT * FROM users WHERE id=?",(row["user_id"],)).fetchone()
    token=create_session(row["user_id"]); audit(row["user_id"],"two_factor_login","account:security")
    return {"token":token,"user":public_user(user)}

@app.get("/api/auth/2fa/status")
def two_factor_status(authorization: Optional[str]=Header(default=None)):
    user,_=current_user(authorization)
    with db() as conn: row=conn.execute("SELECT enabled FROM user_2fa WHERE user_id=?",(user["id"],)).fetchone()
    return {"enabled":bool(row and int(row["enabled"])==1)}

@app.post("/api/auth/2fa/setup")
def two_factor_setup(authorization: Optional[str]=Header(default=None)):
    user,_=current_user(authorization); secret=_totp_secret()
    with db() as conn:
        conn.execute("""INSERT INTO user_2fa(user_id,secret,enabled,created_at) VALUES(?,?,0,?)
          ON CONFLICT(user_id) DO UPDATE SET secret=excluded.secret,enabled=0,created_at=excluded.created_at""",
          (user["id"],secret,datetime.now(timezone.utc).isoformat()))
    return {"secret":secret}

@app.post("/api/auth/2fa/enable")
def two_factor_enable(payload: TwoFactorCodeRequest,authorization: Optional[str]=Header(default=None)):
    user,_=current_user(authorization)
    with db() as conn: row=conn.execute("SELECT secret FROM user_2fa WHERE user_id=?",(user["id"],)).fetchone()
    if not row or not verify_totp(row["secret"],payload.code): raise HTTPException(status_code=400,detail="Invalid verification code")
    with db() as conn: conn.execute("UPDATE user_2fa SET enabled=1 WHERE user_id=?",(user["id"],))
    audit(user["id"],"enable_2fa","account:security"); return {"ok":True,"enabled":True}

@app.post("/api/auth/2fa/disable")
def two_factor_disable(payload: TwoFactorDisableRequest,authorization: Optional[str]=Header(default=None)):
    user,current_token=current_user(authorization)
    if not verify_password(payload.current_password,user["password_hash"]): raise HTTPException(status_code=400,detail="Current password is incorrect")
    with db() as conn: row=conn.execute("SELECT secret,enabled FROM user_2fa WHERE user_id=?",(user["id"],)).fetchone()
    if not row or int(row["enabled"])!=1 or not verify_totp(row["secret"],payload.code): raise HTTPException(status_code=400,detail="Invalid verification code")
    with db() as conn:
        conn.execute("DELETE FROM user_2fa WHERE user_id=?",(user["id"],))
        conn.execute("DELETE FROM sessions WHERE user_id=? AND token<>?",(user["id"],current_token))
    audit(user["id"],"disable_2fa","account:security"); return {"ok":True,"enabled":False}

@app.post("/api/auth/logout")
def logout(authorization: Optional[str] = Header(default=None)):
    _, token = current_user(authorization)
    with db() as conn:
        conn.execute("DELETE FROM sessions WHERE token=?", (token,))
    return {"ok": True}


@app.get("/api/auth/sessions")
def list_sessions(authorization: Optional[str] = Header(default=None)):
    user, current_token = current_user(authorization)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        conn.execute("DELETE FROM sessions WHERE expires_at < ?", (now,))
        rows = conn.execute(
            """SELECT token, expires_at, created_at
               FROM sessions WHERE user_id=?
               ORDER BY CASE WHEN token=? THEN 0 ELSE 1 END, created_at DESC""",
            (user["id"], current_token),
        ).fetchall()
    return {"sessions": [
        {
            "id": r["token"][:12],
            "current": r["token"] == current_token,
            "created_at": r["created_at"] or None,
            "expires_at": r["expires_at"],
        } for r in rows
    ]}


@app.post("/api/auth/logout-others")
def logout_other_sessions(authorization: Optional[str] = Header(default=None)):
    user, current_token = current_user(authorization)
    with db() as conn:
        cur = conn.execute(
            "DELETE FROM sessions WHERE user_id=? AND token<>?",
            (user["id"], current_token),
        )
        removed = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
    audit(user["id"], "logout_other_sessions", "account:security")
    return {"ok": True, "removed": removed, "message": "Other sessions logged out"}


@app.post("/api/auth/logout-all")
def logout_all_sessions(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        cur = conn.execute("DELETE FROM sessions WHERE user_id=?", (user["id"],))
        removed = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
    audit(user["id"], "logout_all_sessions", "account:security")
    return {"ok": True, "removed": removed, "message": "All sessions logged out"}


@app.post("/api/auth/change-password")
def change_password(payload: ChangePasswordRequest, authorization: Optional[str] = Header(default=None)):
    user, current_token = current_user(authorization)
    if not verify_password(payload.current_password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if payload.current_password == payload.new_password:
        raise HTTPException(status_code=400, detail="New password must be different from the current password")
    new_hash = hash_password(payload.new_password)
    with db() as conn:
        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (new_hash, user["id"]))
        # Invalidate every other login session after a password change while
        # keeping the device that performed the verified change signed in.
        conn.execute("DELETE FROM sessions WHERE user_id=? AND token<>?", (user["id"], current_token))
    audit(user["id"], "change_password", "account:security")
    return {"ok": True, "message": "Password changed successfully"}


@app.get("/api/settings/me")
def get_user_settings(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute("SELECT * FROM user_settings WHERE user_id=?", (user["id"],)).fetchone()
    if not row:
        return {
            "theme": "system", "language": "en", "notifications": True,
            "profile_visibility": "connections", "activity_status": True,
            "connection_requests": True,
        }
    return {
        "theme": row["theme"], "language": row["language"],
        "notifications": bool(row["notifications"]),
        "profile_visibility": row["profile_visibility"],
        "activity_status": bool(row["activity_status"]),
        "connection_requests": bool(row["connection_requests"]),
    }


@app.put("/api/settings/me")
def update_user_settings(payload: UserSettingsUpdate, authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    if payload.theme not in {"system", "light", "dark"}:
        raise HTTPException(status_code=400, detail="Invalid theme")
    if payload.profile_visibility not in {"public", "connections", "private"}:
        raise HTTPException(status_code=400, detail="Invalid profile visibility")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        conn.execute(
            """INSERT INTO user_settings(user_id,theme,language,notifications,profile_visibility,activity_status,connection_requests,updated_at)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 theme=excluded.theme, language=excluded.language, notifications=excluded.notifications,
                 profile_visibility=excluded.profile_visibility, activity_status=excluded.activity_status,
                 connection_requests=excluded.connection_requests, updated_at=excluded.updated_at""",
            (user["id"], payload.theme, payload.language, int(payload.notifications),
             payload.profile_visibility, int(payload.activity_status), int(payload.connection_requests), now),
        )
    audit(user["id"], "update", "settings:self")
    return {"ok": True, "message": "Settings saved to your NOVEAM account"}


@app.get("/api/users/me")
def me(authorization: Optional[str] = Header(default=None)):
    row, _ = current_user(authorization)
    data = public_user(row)
    data["email"] = row["email"] if "email" in row.keys() else ""
    data["phone"] = row["phone"] if "phone" in row.keys() else ""
    with db() as conn:
        profile = conn.execute("SELECT display_name FROM profiles WHERE user_id=?", (row["id"],)).fetchone()
    data["display_name"] = profile["display_name"] if profile else row["full_name"]
    return data


@app.put("/api/account/contact")
def update_account_contact(payload: AccountContactUpdate, authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    if not verify_password(payload.current_password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    email = payload.email.strip().lower()
    phone = payload.phone.strip()
    if not email or "@" not in email or email.startswith("@") or email.endswith("@"):
        raise HTTPException(status_code=400, detail="Enter a valid email address")
    if not phone:
        raise HTTPException(status_code=400, detail="Enter a phone number")
    normalized_phone = "".join(ch for ch in phone if ch.isdigit() or ch == "+")
    if len("".join(ch for ch in normalized_phone if ch.isdigit())) < 7:
        raise HTTPException(status_code=400, detail="Enter a valid phone number")
    with db() as conn:
        duplicate_email = conn.execute("SELECT id FROM users WHERE lower(email)=? AND id<>?", (email, user["id"])).fetchone()
        if duplicate_email:
            raise HTTPException(status_code=409, detail="That email address is already registered")
        duplicate_phone = conn.execute(
            """SELECT id FROM users
               WHERE REPLACE(REPLACE(REPLACE(phone,' ',''),'-',''),'(','')=? AND id<>?""",
            (normalized_phone, user["id"]),
        ).fetchone()
        if duplicate_phone:
            raise HTTPException(status_code=409, detail="That phone number is already registered")
        try:
            conn.execute("UPDATE users SET email=?, phone=? WHERE id=?", (email, phone, user["id"]))
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="Email address or phone number is already registered")
    audit(user["id"], "update_contact", "account:self")
    return {"ok": True, "email": email, "phone": phone, "message": "Account contact details updated"}


@app.delete("/api/account/me")
def delete_account(payload: DeleteAccountRequest, authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    if payload.confirmation.strip().upper() != "DELETE":
        raise HTTPException(status_code=400, detail="Type DELETE to confirm permanent account deletion")
    if not verify_password(payload.current_password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    user_id = user["id"]
    # Delete user-owned records that may belong to older tables without FK cascade,
    # while preserving application schema and all other users' data.
    with db() as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        # Tables with user_id are cleaned defensively. Skip audit_log until the end
        # because its FK is SET NULL and the final user deletion remains auditable.
        tables = [r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()]
        for table in tables:
            if table in {"users", "audit_log"}:
                continue
            safe = table.replace('"', '""')
            cols = {r["name"] for r in conn.execute(f'PRAGMA table_info("{safe}")').fetchall()}
            if "user_id" in cols:
                conn.execute(f'DELETE FROM "{safe}" WHERE user_id=?', (user_id,))
        conn.execute("DELETE FROM audit_log WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    return {"ok": True, "message": "Account permanently deleted"}


@app.get("/api/profiles/me")
def my_profile(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "profile:read")
    audit(user["id"], "read", "profile:self")
    with db() as conn:
        profile = conn.execute("SELECT * FROM profiles WHERE user_id=?", (user["id"],)).fetchone()
    return {
        "user": public_user(user),
        "account": {"email": user["email"] if "email" in user.keys() else "", "phone": user["phone"] if "phone" in user.keys() else ""},
        "profile": dict(profile) if profile else None,
    }


@app.put("/api/profiles/me")
def update_profile(payload: ProfileUpdate, authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    require_permission(user, "profile:write")
    if payload.role not in ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    email = payload.email.strip().lower()
    phone = payload.phone.strip()
    if email and "@" not in email:
        raise HTTPException(status_code=400, detail="Enter a valid email ID")
    values = [
        payload.display_name.strip(), payload.headline.strip(), payload.bio.strip(),
        payload.photo.strip(), payload.cover.strip(), payload.pronouns.strip(),
        payload.location.strip(), payload.languages.strip(), payload.occupation.strip(),
        payload.organization.strip(), payload.education.strip(), payload.skills.strip(),
        payload.interests.strip(), payload.website.strip(), payload.linkedin.strip(),
        payload.youtube.strip(), payload.instagram.strip(), payload.xlink.strip(),
        payload.achievements.strip(), user["id"]
    ]
    with db() as conn:
        if email:
            duplicate_email = conn.execute("SELECT id FROM users WHERE lower(email)=? AND id<>?", (email, user["id"])).fetchone()
            if duplicate_email:
                raise HTTPException(status_code=409, detail="Email ID is already registered to another account")
        if phone:
            duplicate_phone = conn.execute("SELECT id FROM users WHERE phone=? AND id<>?", (phone, user["id"])).fetchone()
            if duplicate_phone:
                raise HTTPException(status_code=409, detail="Phone number is already registered to another account")
        conn.execute("UPDATE users SET full_name=?, role=?, email=?, phone=? WHERE id=?",
                     (payload.display_name.strip(), payload.role, email, phone, user["id"]))
        conn.execute(
            """UPDATE profiles SET display_name=?, headline=?, bio=?, photo=?, cover=?, pronouns=?,
               location=?, languages=?, occupation=?, organization=?, education=?, skills=?, interests=?,
               website=?, linkedin=?, youtube=?, instagram=?, xlink=?, achievements=? WHERE user_id=?""",
            values,
        )
        profile = conn.execute("SELECT * FROM profiles WHERE user_id=?", (user["id"],)).fetchone()
        updated_user = conn.execute("SELECT * FROM users WHERE id=?", (user["id"],)).fetchone()
    audit(user["id"], "update", "profile:self")
    return {"user": public_user(updated_user), "account": {"email": updated_user["email"], "phone": updated_user["phone"]}, "profile": dict(profile)}


@app.get("/api/tasks")
def list_tasks(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE user_id=? ORDER BY task_date,time,id", (user["id"],)
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/tasks", status_code=201)
def add_task(task: Task, authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        cur = conn.execute(
            """INSERT INTO tasks(user_id,task_date,time,title,details,status,responsible_person,comment)
               VALUES(?,?,?,?,?,?,?,?)""",
            (user["id"], str(task.task_date), task.time, task.title, task.details, task.status, task.responsible_person, task.comment),
        )
        row = conn.execute("SELECT * FROM tasks WHERE id=? AND user_id=?", (cur.lastrowid, user["id"])).fetchone()
    audit(user["id"], "create", "task")
    return dict(row)


@app.put("/api/tasks/{task_id}")
def update_task(task_id: int, task: Task, authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        cur = conn.execute(
            """UPDATE tasks SET task_date=?,time=?,title=?,details=?,status=?,responsible_person=?,comment=?
               WHERE id=? AND user_id=?""",
            (str(task.task_date), task.time, task.title, task.details, task.status, task.responsible_person, task.comment, task_id, user["id"]),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Task not found")
        row = conn.execute("SELECT * FROM tasks WHERE id=? AND user_id=?", (task_id, user["id"])).fetchone()
    audit(user["id"], "update", "task")
    return dict(row)

# ========================================
# STEP 12 - PATIENT CONSENT & CARE RELATIONSHIPS
# ========================================

class AccessRequestCreate(BaseModel):
    patient_username: str = Field(min_length=3, max_length=40)
    scope: str = Field(default="care", pattern="^(care|records|care_and_records)$")
    permission: str = Field(default="view", pattern="^(view|edit)$")
    note: str = Field(default="", max_length=500)


def step12_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS care_access_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_user_id INTEGER NOT NULL,
            requester_user_id INTEGER NOT NULL,
            requester_role TEXT NOT NULL,
            scope TEXT NOT NULL,
            permission TEXT NOT NULL DEFAULT 'view',
            note TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            decided_at TEXT,
            revoked_at TEXT,
            FOREIGN KEY(patient_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(requester_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_care_access_patient ON care_access_requests(patient_user_id,status);
        CREATE INDEX IF NOT EXISTS idx_care_access_requester ON care_access_requests(requester_user_id,status);
        """)


# Run the Step 12 migration at import time as well as the normal startup migration.
step12_init_db()


def access_row_dict(row):
    return dict(row) if row else None


def scope_allows(granted_scope: str, required_scope: str) -> bool:
    return granted_scope == required_scope or granted_scope == "care_and_records"


def require_patient_access(viewer, patient_user_id: int, required_scope: str, required_permission: str = "view"):
    if viewer["id"] == patient_user_id:
        return
    with db() as conn:
        grants = conn.execute(
            """SELECT * FROM care_access_requests
               WHERE patient_user_id=? AND requester_user_id=? AND status='approved'""",
            (patient_user_id, viewer["id"]),
        ).fetchall()
    for grant in grants:
        if scope_allows(grant["scope"], required_scope):
            if required_permission == "view" or grant["permission"] == "edit":
                return
    raise HTTPException(status_code=403, detail="The patient has not granted the required access.")


@app.get("/api/care/directory")
def care_directory(q: str = "", authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    term = f"%{q.strip().lower()}%"
    with db() as conn:
        rows = conn.execute(
            """SELECT id,username,full_name,role FROM users
               WHERE id<>? AND role IN ('Doctor','Parent','Sibling','Guardian','Friend')
               AND (LOWER(username) LIKE ? OR LOWER(full_name) LIKE ?)
               ORDER BY full_name LIMIT 30""",
            (user["id"], term, term),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/care/access-requests", status_code=201)
def create_access_request(payload: AccessRequestCreate, authorization: Optional[str] = Header(default=None)):
    requester, _ = current_user(authorization)
    allowed_requesters = {"Doctor", "Parent", "Sibling", "Guardian", "Friend"}
    if requester["role"] not in allowed_requesters:
        raise HTTPException(status_code=403, detail="This role cannot request patient access.")
    # Support roles are view-only in this foundation. Doctors may request view or edit.
    if requester["role"] != "Doctor" and payload.permission == "edit":
        raise HTTPException(status_code=403, detail="Support roles may request view access only.")
    with db() as conn:
        patient = conn.execute(
            "SELECT * FROM users WHERE username=? AND role='Patient'",
            (payload.patient_username.strip().lower(),),
        ).fetchone()
        if not patient:
            raise HTTPException(status_code=404, detail="Patient account not found.")
        existing = conn.execute(
            """SELECT id FROM care_access_requests WHERE patient_user_id=? AND requester_user_id=?
               AND status IN ('pending','approved')""",
            (patient["id"], requester["id"]),
        ).fetchone()
        if existing:
            raise HTTPException(status_code=409, detail="An active or pending relationship already exists.")
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            """INSERT INTO care_access_requests
               (patient_user_id,requester_user_id,requester_role,scope,permission,note,status,created_at)
               VALUES(?,?,?,?,?,?, 'pending', ?)""",
            (patient["id"], requester["id"], requester["role"], payload.scope, payload.permission, payload.note.strip(), now),
        )
        row = conn.execute("SELECT * FROM care_access_requests WHERE id=?", (cur.lastrowid,)).fetchone()
    audit(requester["id"], "create", f"care_access_request:{row['id']}")
    return access_row_dict(row)


@app.get("/api/care/access-requests/me")
def my_access_requests(authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        rows = conn.execute(
            """SELECT r.*,
                      p.username AS patient_username,p.full_name AS patient_name,
                      v.username AS requester_username,v.full_name AS requester_name
               FROM care_access_requests r
               JOIN users p ON p.id=r.patient_user_id
               JOIN users v ON v.id=r.requester_user_id
               WHERE r.patient_user_id=? OR r.requester_user_id=?
               ORDER BY r.id DESC""",
            (user["id"], user["id"]),
        ).fetchall()
    return [dict(r) for r in rows]


def patient_decide_request(request_id: int, decision: str, authorization: Optional[str]):
    patient, _ = current_user(authorization)
    if patient["role"] != "Patient":
        raise HTTPException(status_code=403, detail="Only the patient can approve or deny this request.")
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM care_access_requests WHERE id=? AND patient_user_id=?",
            (request_id, patient["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Access request not found.")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail="This request has already been decided.")
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE care_access_requests SET status=?,decided_at=? WHERE id=?",
            (decision, now, request_id),
        )
        updated = conn.execute("SELECT * FROM care_access_requests WHERE id=?", (request_id,)).fetchone()
    audit(patient["id"], decision, f"care_access_request:{request_id}")
    return access_row_dict(updated)


@app.post("/api/care/access-requests/{request_id}/approve")
def approve_access_request(request_id: int, authorization: Optional[str] = Header(default=None)):
    return patient_decide_request(request_id, "approved", authorization)


@app.post("/api/care/access-requests/{request_id}/deny")
def deny_access_request(request_id: int, authorization: Optional[str] = Header(default=None)):
    return patient_decide_request(request_id, "denied", authorization)


@app.post("/api/care/access-requests/{request_id}/revoke")
def revoke_access_request(request_id: int, authorization: Optional[str] = Header(default=None)):
    user, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute("SELECT * FROM care_access_requests WHERE id=?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Access relationship not found.")
        if user["id"] not in (row["patient_user_id"], row["requester_user_id"]):
            raise HTTPException(status_code=403, detail="You cannot revoke this relationship.")
        if row["status"] not in ("pending", "approved"):
            raise HTTPException(status_code=409, detail="This relationship is not active.")
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE care_access_requests SET status='revoked',revoked_at=? WHERE id=?",
            (now, request_id),
        )
        updated = conn.execute("SELECT * FROM care_access_requests WHERE id=?", (request_id,)).fetchone()
    audit(user["id"], "revoke", f"care_access_request:{request_id}")
    return access_row_dict(updated)


@app.get("/api/care/patients/{patient_username}")
def authorized_patient_care(patient_username: str, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        patient = conn.execute(
            "SELECT id,username,full_name,role FROM users WHERE username=? AND role='Patient'",
            (patient_username.strip().lower(),),
        ).fetchone()
    if not patient:
        raise HTTPException(status_code=404, detail="Patient account not found.")
    require_patient_access(viewer, patient["id"], "care", "view")
    audit(viewer["id"], "read", f"patient_care:{patient['id']}")
    return {"authorized": True, "patient": dict(patient), "scope": "care", "note": "Step 12 authorization check passed; no clinical data is stored here yet."}


@app.get("/api/records/patients/{patient_username}")
def authorized_patient_records(patient_username: str, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        patient = conn.execute(
            "SELECT id,username,full_name,role FROM users WHERE username=? AND role='Patient'",
            (patient_username.strip().lower(),),
        ).fetchone()
    if not patient:
        raise HTTPException(status_code=404, detail="Patient account not found.")
    require_patient_access(viewer, patient["id"], "records", "view")
    audit(viewer["id"], "read", f"patient_records:{patient['id']}")
    return {"authorized": True, "patient": dict(patient), "scope": "records", "note": "Step 12 authorization check passed; no medical records are stored here yet."}


# ========================================
# STEP 13 - CARE PORTAL & RECORDS API DATA
# ========================================

class CareEntryCreate(BaseModel):
    patient_username: Optional[str] = Field(default=None, max_length=40)
    title: str = Field(min_length=1, max_length=160)
    details: str = Field(default="", max_length=3000)
    status: str = Field(default="Active", max_length=40)

class RecordCreate(BaseModel):
    patient_username: Optional[str] = Field(default=None, max_length=40)
    record_type: str = Field(default="General", max_length=80)
    title: str = Field(min_length=1, max_length=160)
    details: str = Field(default="", max_length=5000)


def step13_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS care_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_user_id INTEGER NOT NULL,
            created_by_user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            details TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'Active',
            created_at TEXT NOT NULL,
            FOREIGN KEY(patient_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(created_by_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS patient_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_user_id INTEGER NOT NULL,
            created_by_user_id INTEGER NOT NULL,
            record_type TEXT NOT NULL DEFAULT 'General',
            title TEXT NOT NULL,
            details TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY(patient_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(created_by_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_care_entries_patient ON care_entries(patient_user_id,id);
        CREATE INDEX IF NOT EXISTS idx_patient_records_patient ON patient_records(patient_user_id,id);
        """)

step13_init_db()


def patient_for_viewer(viewer, patient_username: Optional[str], scope: str, permission: str = "view"):
    with db() as conn:
        if patient_username:
            patient = conn.execute("SELECT id,username,full_name,role FROM users WHERE username=? AND role='Patient'", (patient_username.strip().lower(),)).fetchone()
        elif viewer["role"] == "Patient":
            patient = conn.execute("SELECT id,username,full_name,role FROM users WHERE id=?", (viewer["id"],)).fetchone()
        else:
            raise HTTPException(status_code=400, detail="Choose an authorized patient username.")
    if not patient:
        raise HTTPException(status_code=404, detail="Patient account not found.")
    require_patient_access(viewer, patient["id"], scope, permission)
    return patient

@app.get("/api/care/entries")
def list_care_entries(patient_username: Optional[str] = None, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    patient = patient_for_viewer(viewer, patient_username, "care", "view")
    with db() as conn:
        rows = conn.execute("""SELECT c.*,u.full_name AS created_by_name,u.role AS created_by_role
                               FROM care_entries c JOIN users u ON u.id=c.created_by_user_id
                               WHERE c.patient_user_id=? ORDER BY c.id DESC""", (patient["id"],)).fetchall()
    audit(viewer["id"], "read", f"care_entries:{patient['id']}")
    return {"patient": dict(patient), "entries": [dict(r) for r in rows]}

@app.post("/api/care/entries", status_code=201)
def create_care_entry(payload: CareEntryCreate, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    patient = patient_for_viewer(viewer, payload.patient_username, "care", "edit" if viewer["role"] != "Patient" else "view")
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute("INSERT INTO care_entries(patient_user_id,created_by_user_id,title,details,status,created_at) VALUES(?,?,?,?,?,?)",
                         (patient["id"],viewer["id"],payload.title.strip(),payload.details.strip(),payload.status.strip(),now))
        row=conn.execute("SELECT * FROM care_entries WHERE id=?",(cur.lastrowid,)).fetchone()
    audit(viewer["id"], "create", f"care_entry:{row['id']}")
    return dict(row)

@app.get("/api/records/entries")
def list_record_entries(patient_username: Optional[str] = None, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    patient = patient_for_viewer(viewer, patient_username, "records", "view")
    with db() as conn:
        rows=conn.execute("""SELECT r.*,u.full_name AS created_by_name,u.role AS created_by_role
                              FROM patient_records r JOIN users u ON u.id=r.created_by_user_id
                              WHERE r.patient_user_id=? ORDER BY r.id DESC""",(patient["id"],)).fetchall()
    audit(viewer["id"], "read", f"record_entries:{patient['id']}")
    return {"patient":dict(patient),"records":[dict(r) for r in rows]}

@app.post("/api/records/entries", status_code=201)
def create_record_entry(payload: RecordCreate, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    patient = patient_for_viewer(viewer, payload.patient_username, "records", "edit" if viewer["role"] != "Patient" else "view")
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute("INSERT INTO patient_records(patient_user_id,created_by_user_id,record_type,title,details,created_at) VALUES(?,?,?,?,?,?)",
                         (patient["id"],viewer["id"],payload.record_type.strip(),payload.title.strip(),payload.details.strip(),now))
        row=conn.execute("SELECT * FROM patient_records WHERE id=?",(cur.lastrowid,)).fetchone()
    audit(viewer["id"], "create", f"patient_record:{row['id']}")
    return dict(row)


# ========================================
# STEP 14 - FILE & DOCUMENT UPLOADS
# ========================================
UPLOAD_DIR = Path(os.environ.get("NOVEAGAMHEAM_UPLOAD_DIR", str(BASE_DIR / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_UPLOAD_BYTES = 10 * 1024 * 1024
ALLOWED_UPLOAD_TYPES = {
    "application/pdf": ".pdf",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "text/plain": ".txt",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
}


def step14_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_user_id INTEGER NOT NULL,
            patient_user_id INTEGER,
            record_id INTEGER,
            category TEXT NOT NULL DEFAULT 'personal',
            original_name TEXT NOT NULL,
            stored_name TEXT NOT NULL UNIQUE,
            content_type TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(patient_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(record_id) REFERENCES patient_records(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_uploaded_files_owner ON uploaded_files(owner_user_id,id);
        CREATE INDEX IF NOT EXISTS idx_uploaded_files_patient ON uploaded_files(patient_user_id,id);
        """)

step14_init_db()


def file_row_for_access(file_id: int, viewer, write: bool = False):
    with db() as conn:
        row = conn.execute("SELECT * FROM uploaded_files WHERE id=?", (file_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="File not found.")
    if row["patient_user_id"] is not None:
        require_patient_access(viewer, row["patient_user_id"], "records", "edit" if write else "view")
    elif row["owner_user_id"] != viewer["id"]:
        raise HTTPException(status_code=403, detail="You do not have access to this file.")
    return row


@app.get("/api/files")
def list_files(patient_username: Optional[str] = None, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        if patient_username:
            patient = patient_for_viewer(viewer, patient_username, "records", "view")
            rows = conn.execute("SELECT * FROM uploaded_files WHERE patient_user_id=? ORDER BY id DESC", (patient["id"],)).fetchall()
        elif viewer["role"] == "Patient":
            rows = conn.execute("SELECT * FROM uploaded_files WHERE owner_user_id=? OR patient_user_id=? ORDER BY id DESC", (viewer["id"], viewer["id"])).fetchall()
        else:
            rows = conn.execute("SELECT * FROM uploaded_files WHERE owner_user_id=? AND patient_user_id IS NULL ORDER BY id DESC", (viewer["id"],)).fetchall()
    return {"files": [{k:r[k] for k in r.keys() if k != "stored_name"} for r in rows]}


@app.post("/api/files/upload", status_code=201)
async def upload_file(
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
        patient = patient_for_viewer(viewer, patient_username, "records", "edit" if viewer["role"] != "Patient" else "view")
        patient_id = patient["id"]
    elif category == "record" and viewer["role"] == "Patient":
        patient_id = viewer["id"]

    ctype = (upload.content_type or "").lower()
    if ctype not in ALLOWED_UPLOAD_TYPES:
        raise HTTPException(status_code=415, detail="Allowed files: PDF, PNG, JPG, WEBP, TXT, DOC and DOCX.")
    data = await upload.read(MAX_UPLOAD_BYTES + 1)
    if not data:
        raise HTTPException(status_code=400, detail="The selected file is empty.")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Maximum file size is 10 MB.")
    original = Path(upload.filename or "upload").name[:255]
    stored = secrets.token_hex(24) + ALLOWED_UPLOAD_TYPES[ctype]
    (UPLOAD_DIR / stored).write_bytes(data)
    now = datetime.now(timezone.utc).isoformat()
    try:
        with db() as conn:
            cur = conn.execute("""INSERT INTO uploaded_files(owner_user_id,patient_user_id,record_id,category,original_name,stored_name,content_type,size_bytes,created_at)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                               (viewer["id"], patient_id, record_id, category, original, stored, ctype, len(data), now))
            file_id = cur.lastrowid
    except Exception:
        (UPLOAD_DIR / stored).unlink(missing_ok=True)
        raise
    audit(viewer["id"], "upload", f"file:{file_id}")
    return {"id": file_id, "original_name": original, "content_type": ctype, "size_bytes": len(data), "category": category, "created_at": now}


@app.get("/api/files/{file_id}/download")
def download_file(file_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    row = file_row_for_access(file_id, viewer)
    path = UPLOAD_DIR / row["stored_name"]
    if not path.exists():
        raise HTTPException(status_code=404, detail="Stored file is missing.")
    audit(viewer["id"], "download", f"file:{file_id}")
    return FileResponse(path, media_type=row["content_type"], filename=row["original_name"])


@app.delete("/api/files/{file_id}", status_code=204)
def delete_file(file_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    row = file_row_for_access(file_id, viewer, write=True)
    if row["owner_user_id"] != viewer["id"] and viewer["role"] != "Patient":
        raise HTTPException(status_code=403, detail="Only the uploader or patient can delete this file.")
    with db() as conn:
        conn.execute("DELETE FROM uploaded_files WHERE id=?", (file_id,))
    (UPLOAD_DIR / row["stored_name"]).unlink(missing_ok=True)
    audit(viewer["id"], "delete", f"file:{file_id}")

# ========================================
# STEP 15 - SOCIAL FEED + PROFILE MEDIA
# ========================================
SOCIAL_IMAGE_TYPES = {"image/png", "image/jpeg", "image/webp"}
SOCIAL_VIDEO_TYPES = {"video/mp4", "video/webm", "video/quicktime"}
SOCIAL_AUDIO_TYPES = {"audio/mpeg", "audio/mp4", "audio/wav", "audio/x-wav", "audio/ogg"}
SOCIAL_DOCUMENT_TYPES = {
    "application/pdf", "text/plain",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
}
SOCIAL_POST_MEDIA_TYPES = SOCIAL_IMAGE_TYPES | SOCIAL_VIDEO_TYPES | SOCIAL_AUDIO_TYPES | SOCIAL_DOCUMENT_TYPES


def step15_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author_user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT 'Other',
            visibility TEXT NOT NULL DEFAULT 'PUBLIC',
            media_file_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(media_file_id) REFERENCES uploaded_files(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS post_likes (
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(post_id,user_id),
            FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS post_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE,
            FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(id DESC);
        CREATE INDEX IF NOT EXISTS idx_comments_post ON post_comments(post_id,id);
        """)

step15_init_db()

# Social composer extensions: media type/URL + article metadata.
with db() as conn:
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(posts)").fetchall()}
    if "post_type" not in cols:
        conn.execute("ALTER TABLE posts ADD COLUMN post_type TEXT NOT NULL DEFAULT 'POST'")
    if "media_url" not in cols:
        conn.execute("ALTER TABLE posts ADD COLUMN media_url TEXT")
    if "article_title" not in cols:
        conn.execute("ALTER TABLE posts ADD COLUMN article_title TEXT")
    if "article_cover_file_id" not in cols:
        conn.execute("ALTER TABLE posts ADD COLUMN article_cover_file_id INTEGER")

class PostCreate(BaseModel):
    text: str = Field(min_length=1, max_length=20000)
    category: str = Field(default="Other", max_length=60)
    visibility: str = Field(default="PUBLIC", max_length=30)
    media_file_id: Optional[int] = None
    post_type: str = Field(default="POST", max_length=20)
    media_url: Optional[str] = Field(default=None, max_length=2000)
    article_title: Optional[str] = Field(default=None, max_length=240)
    article_cover_file_id: Optional[int] = None

class PostUpdate(BaseModel):
    text: str = Field(min_length=1, max_length=5000)
    category: str = Field(default="Other", max_length=60)
    visibility: str = Field(default="PUBLIC", max_length=30)

class CommentCreate(BaseModel):
    text: str = Field(min_length=1, max_length=1000)


def post_visible_to(row, viewer):
    if row["author_user_id"] == viewer["id"]:
        return True
    if row["visibility"] == "PUBLIC":
        return True
    if row["visibility"] == "CONNECTIONS":
        with db() as conn:
            return are_connected(conn, viewer["id"], row["author_user_id"])
    # CARE_TEAM remains governed separately by explicit care consent.
    return False


def serialize_post(row, viewer):
    with db() as conn:
        profile = conn.execute("SELECT display_name,photo FROM profiles WHERE user_id=?", (row["author_user_id"],)).fetchone()
        author = conn.execute("SELECT username,full_name FROM users WHERE id=?", (row["author_user_id"],)).fetchone()
        likes = conn.execute("SELECT COUNT(*) n FROM post_likes WHERE post_id=?", (row["id"],)).fetchone()["n"]
        liked = conn.execute("SELECT 1 FROM post_likes WHERE post_id=? AND user_id=?", (row["id"],viewer["id"])).fetchone() is not None
        comments = conn.execute("SELECT COUNT(*) n FROM post_comments WHERE post_id=?", (row["id"],)).fetchone()["n"]
    attachment = None
    if row["media_file_id"]:
        with db() as conn:
            f = conn.execute("SELECT original_name,content_type,size_bytes FROM uploaded_files WHERE id=?", (row["media_file_id"],)).fetchone()
        if f:
            attachment = dict(f)
    return {**dict(row), "author_username": author["username"], "author_name": (profile["display_name"] if profile else author["full_name"]),
            "author_photo": (profile["photo"] if profile else ""), "like_count": likes, "liked_by_me": liked,
            "comment_count": comments, "is_owner": row["author_user_id"] == viewer["id"], "attachment": attachment}

@app.post("/api/profile-media/upload", status_code=201)
async def upload_profile_media(kind: str = Form(...), upload: UploadFile = File(...), authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    require_permission(viewer, "profile:write")
    if kind not in {"photo", "cover"}:
        raise HTTPException(status_code=400, detail="kind must be photo or cover")
    ctype = (upload.content_type or "").lower()
    if ctype not in SOCIAL_IMAGE_TYPES:
        raise HTTPException(status_code=415, detail="Profile images must be PNG, JPG or WEBP.")
    data = await upload.read(MAX_UPLOAD_BYTES + 1)
    if not data or len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413 if data else 400, detail="Profile image must be non-empty and at most 10 MB.")
    stored = secrets.token_hex(24) + ALLOWED_UPLOAD_TYPES[ctype]
    (UPLOAD_DIR / stored).write_bytes(data)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute("""INSERT INTO uploaded_files(owner_user_id,category,original_name,stored_name,content_type,size_bytes,created_at)
                            VALUES(?,?,?,?,?,?,?)""", (viewer["id"], "profile_"+kind, Path(upload.filename or kind).name[:255], stored, ctype, len(data), now))
        file_id = cur.lastrowid
        conn.execute(f"UPDATE profiles SET {kind}=? WHERE user_id=?", (f"file:{file_id}", viewer["id"]))
    audit(viewer["id"], "upload", f"profile_{kind}:{file_id}")
    return {"id": file_id, "kind": kind, "value": f"file:{file_id}"}

@app.get("/api/media/{file_id}")
def authenticated_media(file_id: int, authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        row = conn.execute("SELECT * FROM uploaded_files WHERE id=?", (file_id,)).fetchone()
        post = conn.execute("SELECT * FROM posts WHERE media_file_id=? OR article_cover_file_id=?", (file_id,file_id)).fetchone()
        page_post = conn.execute("SELECT * FROM page_posts WHERE media_file_id=?", (file_id,)).fetchone() if _table_exists(conn, "page_posts") else None
    if not row or row["content_type"] not in SOCIAL_POST_MEDIA_TYPES:
        raise HTTPException(status_code=404, detail="Media not found")
    allowed = row["owner_user_id"] == viewer["id"] or row["category"] in {"profile_photo", "profile_cover"}
    if post and post_visible_to(post, viewer): allowed = True
    if page_post: allowed = True
    if not allowed: raise HTTPException(status_code=403, detail="You do not have access to this image")
    path = UPLOAD_DIR / row["stored_name"]
    if not path.exists(): raise HTTPException(status_code=404, detail="Stored image is missing")
    return FileResponse(path, media_type=row["content_type"])

@app.post("/api/posts/media", status_code=201)
async def upload_post_media(upload: UploadFile = File(...), authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    ctype = (upload.content_type or "").lower()
    if ctype not in SOCIAL_POST_MEDIA_TYPES:
        raise HTTPException(status_code=415, detail="Supported post attachments: PNG/JPG/WEBP, MP4/WEBM/MOV, MP3/M4A/WAV/OGG, PDF/TXT/DOC/DOCX.")
    data = await upload.read(MAX_UPLOAD_BYTES + 1)
    if not data or len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413 if data else 400, detail="Attachment must be non-empty and at most 10 MB.")
    ext = Path(upload.filename or "").suffix.lower()
    if not ext:
        ext = ALLOWED_UPLOAD_TYPES.get(ctype, "")
    stored = secrets.token_hex(24) + ext
    (UPLOAD_DIR / stored).write_bytes(data)
    now = datetime.now(timezone.utc).isoformat()
    category = "post_image" if ctype in SOCIAL_IMAGE_TYPES else "post_media"
    with db() as conn:
        cur=conn.execute("INSERT INTO uploaded_files(owner_user_id,category,original_name,stored_name,content_type,size_bytes,created_at) VALUES(?,?,?,?,?,?,?)",
                         (viewer["id"],category,Path(upload.filename or "post-attachment").name[:255],stored,ctype,len(data),now))
    return {"id":cur.lastrowid, "content_type":ctype, "name":Path(upload.filename or "post-attachment").name[:255]}

@app.get("/api/posts")
def list_posts(authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn: rows=conn.execute("SELECT * FROM posts ORDER BY id DESC LIMIT 200").fetchall()
    return {"posts":[serialize_post(r,viewer) for r in rows if post_visible_to(r,viewer)]}

@app.get("/api/posts/{post_id}")
def get_post(post_id:int, authorization: Optional[str] = Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn: row=conn.execute("SELECT * FROM posts WHERE id=?",(post_id,)).fetchone()
    if not row: raise HTTPException(status_code=404,detail="Post not found")
    if not post_visible_to(row,viewer): raise HTTPException(status_code=403,detail="Post is not visible to you")
    return serialize_post(row,viewer)

@app.post("/api/posts", status_code=201)
def create_post(payload:PostCreate, authorization: Optional[str] = Header(default=None)):
    viewer,_=current_user(authorization); visibility=payload.visibility.upper()
    if visibility not in {"PUBLIC","CONNECTIONS","CARE_TEAM","PRIVATE"}: raise HTTPException(status_code=400,detail="Invalid visibility")
    post_type = payload.post_type.upper()
    if post_type not in {"POST","PHOTO","VIDEO","AUDIO","FILE","ARTICLE"}:
        raise HTTPException(status_code=400, detail="Invalid post type")
    if payload.media_file_id:
        with db() as conn:
            f=conn.execute("SELECT * FROM uploaded_files WHERE id=? AND owner_user_id=? AND category IN ('post_image','post_media')",(payload.media_file_id,viewer["id"])).fetchone()
        if not f: raise HTTPException(status_code=400,detail="Invalid post attachment")
    if payload.media_url:
        if not (payload.media_url.startswith("https://") or payload.media_url.startswith("http://")):
            raise HTTPException(status_code=400, detail="Media URL must start with http:// or https://")
    if post_type == "ARTICLE" and not (payload.article_title or "").strip():
        raise HTTPException(status_code=400, detail="Article title is required")
    if payload.article_cover_file_id:
        with db() as conn:
            cover=conn.execute("SELECT * FROM uploaded_files WHERE id=? AND owner_user_id=? AND category='post_image'",
                               (payload.article_cover_file_id,viewer["id"])).fetchone()
        if not cover or cover["content_type"] not in SOCIAL_IMAGE_TYPES:
            raise HTTPException(status_code=400, detail="Invalid article cover image")
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute("""INSERT INTO posts(author_user_id,text,category,visibility,media_file_id,created_at,updated_at,post_type,media_url,article_title,article_cover_file_id)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                         (viewer["id"],payload.text.strip(),payload.category.strip() or "Other",visibility,payload.media_file_id,now,now,
                          post_type,(payload.media_url or "").strip() or None,(payload.article_title or "").strip() or None,payload.article_cover_file_id))
        row=conn.execute("SELECT * FROM posts WHERE id=?",(cur.lastrowid,)).fetchone()
    audit(viewer["id"],"create",f"post:{row['id']}"); return serialize_post(row,viewer)

@app.put("/api/posts/{post_id}")
def update_post(post_id:int,payload:PostUpdate,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization); visibility=payload.visibility.upper()
    if visibility not in {"PUBLIC","CONNECTIONS","CARE_TEAM","PRIVATE"}: raise HTTPException(status_code=400,detail="Invalid visibility")
    with db() as conn:
        row=conn.execute("SELECT * FROM posts WHERE id=?",(post_id,)).fetchone()
        if not row: raise HTTPException(status_code=404,detail="Post not found")
        if row["author_user_id"]!=viewer["id"]: raise HTTPException(status_code=403,detail="Only the author can edit this post")
        conn.execute("UPDATE posts SET text=?,category=?,visibility=?,updated_at=? WHERE id=?",(payload.text.strip(),payload.category.strip(),visibility,datetime.now(timezone.utc).isoformat(),post_id))
        row=conn.execute("SELECT * FROM posts WHERE id=?",(post_id,)).fetchone()
    return serialize_post(row,viewer)

@app.delete("/api/posts/{post_id}", status_code=204)
def delete_post(post_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute("SELECT * FROM posts WHERE id=?",(post_id,)).fetchone()
        if not row: raise HTTPException(status_code=404,detail="Post not found")
        if row["author_user_id"]!=viewer["id"]: raise HTTPException(status_code=403,detail="Only the author can delete this post")
        conn.execute("DELETE FROM posts WHERE id=?",(post_id,))
    audit(viewer["id"],"delete",f"post:{post_id}")

@app.post("/api/posts/{post_id}/like", status_code=201)
def like_post(post_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute("SELECT * FROM posts WHERE id=?",(post_id,)).fetchone()
        if not row or not post_visible_to(row,viewer): raise HTTPException(status_code=404,detail="Post not found")
        conn.execute("INSERT OR IGNORE INTO post_likes(post_id,user_id,created_at) VALUES(?,?,?)",(post_id,viewer["id"],datetime.now(timezone.utc).isoformat()))
        if row['author_user_id'] != viewer['id']: create_notification(conn,row['author_user_id'],viewer['id'],'POST_LIKE','New like','liked your post','post',post_id)
    return {"ok":True}

@app.delete("/api/posts/{post_id}/like", status_code=204)
def unlike_post(post_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn: conn.execute("DELETE FROM post_likes WHERE post_id=? AND user_id=?",(post_id,viewer["id"]))

@app.get("/api/posts/{post_id}/comments")
def list_comments(post_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        post=conn.execute("SELECT * FROM posts WHERE id=?",(post_id,)).fetchone()
        if not post or not post_visible_to(post,viewer): raise HTTPException(status_code=404,detail="Post not found")
        rows=conn.execute("""SELECT c.*,u.username,u.full_name,p.display_name FROM post_comments c JOIN users u ON u.id=c.author_user_id
                            LEFT JOIN profiles p ON p.user_id=u.id WHERE c.post_id=? ORDER BY c.id""",(post_id,)).fetchall()
    return {"comments":[{**dict(r),"author_name":r["display_name"] or r["full_name"],"is_owner":r["author_user_id"]==viewer["id"]} for r in rows]}

@app.post("/api/posts/{post_id}/comments", status_code=201)
def add_comment(post_id:int,payload:CommentCreate,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        post=conn.execute("SELECT * FROM posts WHERE id=?",(post_id,)).fetchone()
        if not post or not post_visible_to(post,viewer): raise HTTPException(status_code=404,detail="Post not found")
        cur=conn.execute("INSERT INTO post_comments(post_id,author_user_id,text,created_at) VALUES(?,?,?,?)",(post_id,viewer["id"],payload.text.strip(),datetime.now(timezone.utc).isoformat()))
        if post['author_user_id'] != viewer['id']: create_notification(conn,post['author_user_id'],viewer['id'],'POST_COMMENT','New comment','commented on your post','post',post_id)
    return {"id":cur.lastrowid,"ok":True}

@app.delete("/api/comments/{comment_id}", status_code=204)
def delete_comment(comment_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute("SELECT * FROM post_comments WHERE id=?",(comment_id,)).fetchone()
        if not row: raise HTTPException(status_code=404,detail="Comment not found")
        if row["author_user_id"]!=viewer["id"]: raise HTTPException(status_code=403,detail="Only the author can delete this comment")
        conn.execute("DELETE FROM post_comments WHERE id=?",(comment_id,))

# ========================================
# STEP 18 - REAL BACKEND NOTIFICATIONS
# ========================================
def step18_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            actor_user_id INTEGER,
            type TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL DEFAULT '',
            target_type TEXT NOT NULL DEFAULT '',
            target_id INTEGER,
            created_at TEXT NOT NULL,
            read_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(actor_user_id) REFERENCES users(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id,id DESC);
        CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(user_id,read_at);
        """)
step18_init_db()

def create_notification(conn, user_id:int, actor_user_id:Optional[int], ntype:str, title:str, body:str='', target_type:str='', target_id:Optional[int]=None):
    if actor_user_id is not None and user_id == actor_user_id:
        return
    pref = conn.execute("SELECT notifications FROM user_settings WHERE user_id=?", (user_id,)).fetchone()
    if pref is not None and not bool(pref["notifications"]):
        return
    conn.execute("INSERT INTO notifications(user_id,actor_user_id,type,title,body,target_type,target_id,created_at) VALUES(?,?,?,?,?,?,?,?)",
                 (user_id,actor_user_id,ntype,title,body,target_type,target_id,datetime.now(timezone.utc).isoformat()))

def notification_actor(conn, actor_id):
    if actor_id is None: return None
    u=conn.execute('SELECT id,username,full_name,role FROM users WHERE id=?',(actor_id,)).fetchone()
    if not u: return None
    p=conn.execute('SELECT display_name,photo FROM profiles WHERE user_id=?',(actor_id,)).fetchone()
    return {'id':u['id'],'username':u['username'],'name':(p['display_name'] if p and p['display_name'] else u['full_name']),'photo':(p['photo'] if p else ''),'role':u['role']}

@app.get('/api/notifications')
def list_notifications(limit:int=100, unread_only:bool=False, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization); limit=max(1,min(limit,200))
    with db() as conn:
        sql='SELECT * FROM notifications WHERE user_id=?' + (' AND read_at IS NULL' if unread_only else '') + ' ORDER BY id DESC LIMIT ?'
        rows=conn.execute(sql,(viewer['id'],limit)).fetchall()
        unread=conn.execute('SELECT COUNT(*) n FROM notifications WHERE user_id=? AND read_at IS NULL',(viewer['id'],)).fetchone()['n']
        items=[]
        for r in rows:
            d=dict(r); d['actor']=notification_actor(conn,r['actor_user_id']); items.append(d)
        return {'notifications':items,'unread_count':unread}

@app.post('/api/notifications/{notification_id}/read')
def read_notification(notification_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        r=conn.execute('SELECT id FROM notifications WHERE id=? AND user_id=?',(notification_id,viewer['id'])).fetchone()
        if not r: raise HTTPException(404,'Notification not found')
        conn.execute('UPDATE notifications SET read_at=COALESCE(read_at,?) WHERE id=?',(datetime.now(timezone.utc).isoformat(),notification_id))
    return {'ok':True}

@app.post('/api/notifications/read-all')
def read_all_notifications(authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn: conn.execute('UPDATE notifications SET read_at=? WHERE user_id=? AND read_at IS NULL',(datetime.now(timezone.utc).isoformat(),viewer['id']))
    return {'ok':True}

@app.delete('/api/notifications/{notification_id}', status_code=204)
def delete_notification(notification_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        r=conn.execute('SELECT id FROM notifications WHERE id=? AND user_id=?',(notification_id,viewer['id'])).fetchone()
        if not r: raise HTTPException(404,'Notification not found')
        conn.execute('DELETE FROM notifications WHERE id=?',(notification_id,))

@app.get('/api/search')
def global_search(q: str = '', category: str = 'all', limit: int = 8, authorization: Optional[str] = Header(default=None)):
    viewer,_ = current_user(authorization)
    term = q.strip()
    if len(term) < 2:
        return {'query': term, 'results': [], 'counts': {}}
    limit = max(1, min(limit, 25))
    allowed = {'all','people','posts','pages','groups','jobs','portfolio','education','events'}
    if category not in allowed:
        raise HTTPException(400, 'Invalid search category')
    like = '%' + term + '%'
    results = []
    counts = {}
    with db() as conn:
        def add(kind, rows, mapper):
            counts[kind] = len(rows)
            results.extend(mapper(r) for r in rows)

        if category in ('all','people'):
            rows=conn.execute("""SELECT u.id,u.username,u.full_name,u.role,p.display_name,p.headline,p.photo
                FROM users u LEFT JOIN profiles p ON p.user_id=u.id
                WHERE u.id<>? AND (u.username LIKE ? OR u.full_name LIKE ? OR p.display_name LIKE ? OR p.headline LIKE ?
                OR p.skills LIKE ? OR p.occupation LIKE ? OR p.organization LIKE ?)
                ORDER BY COALESCE(p.display_name,u.full_name) LIMIT ?""",
                (viewer['id'],like,like,like,like,like,like,like,limit)).fetchall()
            add('people',rows,lambda r:{'type':'people','id':r['id'],'title':r['display_name'] or r['full_name'],
                'subtitle':('@'+r['username']+' · '+(r['headline'] or r['role'] or '')).strip(' ·'),
                'url':'profile.html?username='+r['username']})

        if category in ('all','posts'):
            rows=conn.execute("""SELECT p.id,p.text,p.category,p.created_at,u.username,u.full_name,pr.display_name
                FROM posts p JOIN users u ON u.id=p.author_user_id LEFT JOIN profiles pr ON pr.user_id=u.id
                WHERE (p.visibility='public' OR p.visibility='Public' OR p.author_user_id=?)
                AND (p.text LIKE ? OR p.category LIKE ?) ORDER BY p.id DESC LIMIT ?""",
                (viewer['id'],like,like,limit)).fetchall()
            add('posts',rows,lambda r:{'type':'posts','id':r['id'],'title':(r['text'][:120] or 'Post'),
                'subtitle':'Post by '+(r['display_name'] or r['full_name'])+((' · '+r['category']) if r['category'] else ''),
                'url':'feed.html?post='+str(r['id'])})

        if category in ('all','pages'):
            rows=conn.execute("""SELECT id,name,page_type,category,location FROM organization_pages
                WHERE name LIKE ? OR category LIKE ? OR about LIKE ? OR services LIKE ? OR location LIKE ?
                ORDER BY id DESC LIMIT ?""",(like,like,like,like,like,limit)).fetchall()
            add('pages',rows,lambda r:{'type':'pages','id':r['id'],'title':r['name'],
                'subtitle':' · '.join(x for x in [r['page_type'],r['category'],r['location']] if x),
                'url':'pages.html?page='+str(r['id'])})

        if category in ('all','groups'):
            rows=conn.execute("""SELECT id,name,description,privacy FROM community_groups
                WHERE privacy='public' AND (name LIKE ? OR description LIKE ?) ORDER BY id DESC LIMIT ?""",
                (like,like,limit)).fetchall()
            add('groups',rows,lambda r:{'type':'groups','id':r['id'],'title':r['name'],
                'subtitle':(r['description'][:140] or 'Public group'),'url':'groups.html?group='+str(r['id'])})

        if category in ('all','jobs'):
            rows=conn.execute("""SELECT id,title,opportunity_type,work_mode,location,skills FROM jobs
                WHERE status='open' AND (title LIKE ? OR description LIKE ? OR skills LIKE ? OR location LIKE ?)
                ORDER BY id DESC LIMIT ?""",(like,like,like,like,limit)).fetchall()
            add('jobs',rows,lambda r:{'type':'jobs','id':r['id'],'title':r['title'],
                'subtitle':' · '.join(x for x in [r['opportunity_type'],r['work_mode'],r['location']] if x),
                'url':'jobs.html?job='+str(r['id'])})

        if category in ('all','portfolio'):
            rows=conn.execute("""SELECT pp.id,pp.title,pp.category,pp.skills,u.username,u.full_name
                FROM portfolio_projects pp JOIN users u ON u.id=pp.user_id
                WHERE (pp.visibility='Public' OR pp.user_id=?) AND
                (pp.title LIKE ? OR pp.description LIKE ? OR pp.skills LIKE ? OR pp.tools LIKE ?)
                ORDER BY pp.id DESC LIMIT ?""",(viewer['id'],like,like,like,like,limit)).fetchall()
            add('portfolio',rows,lambda r:{'type':'portfolio','id':r['id'],'title':r['title'],
                'subtitle':'Portfolio · '+r['full_name']+((' · '+r['category']) if r['category'] else ''),
                'url':'portfolio.html?username='+r['username']})

        if category in ('all','education'):
            rows=conn.execute("""SELECT id,title,provider,instructor,skills FROM learning_courses
                WHERE visibility='Public' AND (title LIKE ? OR provider LIKE ? OR instructor LIKE ? OR description LIKE ? OR skills LIKE ?)
                ORDER BY id DESC LIMIT ?""",(like,like,like,like,like,limit)).fetchall()
            add('education',rows,lambda r:{'type':'education','id':r['id'],'title':r['title'],
                'subtitle':' · '.join(x for x in [r['provider'],r['instructor']] if x),'url':'education.html?course='+str(r['id'])})

        if category in ('all','events'):
            rows=conn.execute("""SELECT id,title,start_at,location,description FROM events
                WHERE visibility='public' AND (title LIKE ? OR description LIKE ? OR location LIKE ?)
                ORDER BY start_at DESC LIMIT ?""",(like,like,like,limit)).fetchall()
            add('events',rows,lambda r:{'type':'events','id':r['id'],'title':r['title'],
                'subtitle':' · '.join(x for x in [r['start_at'],r['location']] if x),'url':'events.html?event='+str(r['id'])})
    order={'people':0,'posts':1,'pages':2,'groups':3,'jobs':4,'portfolio':5,'education':6,'events':7}
    results.sort(key=lambda x: order.get(x['type'],99))
    return {'query':term,'category':category,'results':results,'counts':counts}


@app.get('/api/activity-summary')
def activity_summary(authorization: Optional[str] = Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        notifications_unread=conn.execute('SELECT COUNT(*) n FROM notifications WHERE user_id=? AND read_at IS NULL',(viewer['id'],)).fetchone()['n']
        messages_unread=conn.execute('SELECT COUNT(*) n FROM private_messages WHERE receiver_user_id=? AND read_at IS NULL AND deleted_by_receiver=0',(viewer['id'],)).fetchone()['n']
    return {'notifications_unread':notifications_unread,'messages_unread':messages_unread,'total_unread':notifications_unread+messages_unread}


# ========================================
# STEP 16 - FOLLOWERS, FOLLOWING, FRIENDS & CONNECTIONS
# ========================================
def step16_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS follows (
            follower_user_id INTEGER NOT NULL,
            followed_user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(follower_user_id, followed_user_id),
            FOREIGN KEY(follower_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(followed_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS connection_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id INTEGER NOT NULL,
            receiver_user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            created_at TEXT NOT NULL,
            responded_at TEXT,
            FOREIGN KEY(sender_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(receiver_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_follows_followed ON follows(followed_user_id);
        CREATE INDEX IF NOT EXISTS idx_connections_receiver ON connection_requests(receiver_user_id,status);
        """)
step16_init_db()

def are_connected(conn, a:int, b:int):
    return conn.execute("""SELECT 1 FROM connection_requests WHERE status='ACCEPTED' AND
        ((sender_user_id=? AND receiver_user_id=?) OR (sender_user_id=? AND receiver_user_id=?)) LIMIT 1""",(a,b,b,a)).fetchone() is not None

def social_user(conn,row,viewer_id):
    p=conn.execute("SELECT display_name,headline,photo FROM profiles WHERE user_id=?",(row['id'],)).fetchone()
    following=conn.execute("SELECT 1 FROM follows WHERE follower_user_id=? AND followed_user_id=?",(viewer_id,row['id'])).fetchone() is not None
    follower=conn.execute("SELECT 1 FROM follows WHERE follower_user_id=? AND followed_user_id=?",(row['id'],viewer_id)).fetchone() is not None
    pending_out=conn.execute("SELECT id FROM connection_requests WHERE sender_user_id=? AND receiver_user_id=? AND status='PENDING' ORDER BY id DESC LIMIT 1",(viewer_id,row['id'])).fetchone()
    pending_in=conn.execute("SELECT id FROM connection_requests WHERE sender_user_id=? AND receiver_user_id=? AND status='PENDING' ORDER BY id DESC LIMIT 1",(row['id'],viewer_id)).fetchone()
    return {'id':row['id'],'username':row['username'],'name':(p['display_name'] if p else row['full_name']),'headline':(p['headline'] if p else ''),'photo':(p['photo'] if p else ''),'role':row['role'],'following':following,'follows_me':follower,'connected':are_connected(conn,viewer_id,row['id']),'pending_out_id':pending_out['id'] if pending_out else None,'pending_in_id':pending_in['id'] if pending_in else None}

@app.get('/api/social/people')
def social_people(q:str='', authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization); term='%'+q.strip()+'%'
    with db() as conn:
        rows=conn.execute("SELECT * FROM users WHERE id<>? AND (username LIKE ? OR full_name LIKE ?) ORDER BY full_name LIMIT 100",(viewer['id'],term,term)).fetchall()
        return {'people':[social_user(conn,r,viewer['id']) for r in rows]}

@app.post('/api/social/follow/{user_id}', status_code=201)
def follow_user(user_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    if user_id==viewer['id']: raise HTTPException(400,'You cannot follow yourself')
    with db() as conn:
        if not conn.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone(): raise HTTPException(404,'User not found')
        conn.execute('INSERT OR IGNORE INTO follows(follower_user_id,followed_user_id,created_at) VALUES(?,?,?)',(viewer['id'],user_id,datetime.now(timezone.utc).isoformat()))
        create_notification(conn,user_id,viewer['id'],'NEW_FOLLOWER','New follower','started following you','user',viewer['id'])
    return {'ok':True}

@app.delete('/api/social/follow/{user_id}', status_code=204)
def unfollow_user(user_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn: conn.execute('DELETE FROM follows WHERE follower_user_id=? AND followed_user_id=?',(viewer['id'],user_id))

@app.post('/api/social/connections/{user_id}', status_code=201)
def request_connection(user_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    if user_id==viewer['id']: raise HTTPException(400,'You cannot connect with yourself')
    with db() as conn:
        if not conn.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone(): raise HTTPException(404,'User not found')
        if are_connected(conn,viewer['id'],user_id): return {'ok':True,'status':'ACCEPTED'}
        incoming=conn.execute("SELECT id FROM connection_requests WHERE sender_user_id=? AND receiver_user_id=? AND status='PENDING' ORDER BY id DESC LIMIT 1",(user_id,viewer['id'])).fetchone()
        if incoming:
            conn.execute("UPDATE connection_requests SET status='ACCEPTED',responded_at=? WHERE id=?",(datetime.now(timezone.utc).isoformat(),incoming['id']))
            return {'ok':True,'status':'ACCEPTED'}
        existing=conn.execute("SELECT id FROM connection_requests WHERE sender_user_id=? AND receiver_user_id=? AND status='PENDING'",(viewer['id'],user_id)).fetchone()
        if existing: return {'ok':True,'status':'PENDING','id':existing['id']}
        cur=conn.execute("INSERT INTO connection_requests(sender_user_id,receiver_user_id,status,created_at) VALUES(?,?,'PENDING',?)",(viewer['id'],user_id,datetime.now(timezone.utc).isoformat()))
        create_notification(conn,user_id,viewer['id'],'CONNECTION_REQUEST','Connection request','sent you a connection request','connection_request',cur.lastrowid)
    return {'ok':True,'status':'PENDING','id':cur.lastrowid}

@app.post('/api/social/connections/requests/{request_id}/accept')
def accept_connection(request_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        r=conn.execute("SELECT * FROM connection_requests WHERE id=? AND receiver_user_id=? AND status='PENDING'",(request_id,viewer['id'])).fetchone()
        if not r: raise HTTPException(404,'Pending request not found')
        conn.execute("UPDATE connection_requests SET status='ACCEPTED',responded_at=? WHERE id=?",(datetime.now(timezone.utc).isoformat(),request_id))
        create_notification(conn,r['sender_user_id'],viewer['id'],'CONNECTION_ACCEPTED','Connection accepted','accepted your connection request','user',viewer['id'])
    return {'ok':True}

@app.post('/api/social/connections/requests/{request_id}/decline')
def decline_connection(request_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        r=conn.execute("SELECT * FROM connection_requests WHERE id=? AND receiver_user_id=? AND status='PENDING'",(request_id,viewer['id'])).fetchone()
        if not r: raise HTTPException(404,'Pending request not found')
        conn.execute("UPDATE connection_requests SET status='DECLINED',responded_at=? WHERE id=?",(datetime.now(timezone.utc).isoformat(),request_id))
    return {'ok':True}

@app.delete('/api/social/connections/{user_id}', status_code=204)
def remove_connection(user_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        conn.execute("DELETE FROM connection_requests WHERE status='ACCEPTED' AND ((sender_user_id=? AND receiver_user_id=?) OR (sender_user_id=? AND receiver_user_id=?))",(viewer['id'],user_id,user_id,viewer['id']))

@app.get('/api/social/network')
def my_network(authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        followers=conn.execute('SELECT u.* FROM follows f JOIN users u ON u.id=f.follower_user_id WHERE f.followed_user_id=? ORDER BY f.created_at DESC',(viewer['id'],)).fetchall()
        following=conn.execute('SELECT u.* FROM follows f JOIN users u ON u.id=f.followed_user_id WHERE f.follower_user_id=? ORDER BY f.created_at DESC',(viewer['id'],)).fetchall()
        connections=conn.execute("""SELECT u.* FROM users u WHERE u.id<>? AND EXISTS(SELECT 1 FROM connection_requests c WHERE c.status='ACCEPTED' AND ((c.sender_user_id=? AND c.receiver_user_id=u.id) OR (c.receiver_user_id=? AND c.sender_user_id=u.id))) ORDER BY u.full_name""",(viewer['id'],viewer['id'],viewer['id'])).fetchall()
        requests=conn.execute("SELECT c.id request_id,u.* FROM connection_requests c JOIN users u ON u.id=c.sender_user_id WHERE c.receiver_user_id=? AND c.status='PENDING' ORDER BY c.id DESC",(viewer['id'],)).fetchall()
        return {'followers':[social_user(conn,r,viewer['id']) for r in followers],'following':[social_user(conn,r,viewer['id']) for r in following],'connections':[social_user(conn,r,viewer['id']) for r in connections],'requests':[dict(social_user(conn,r,viewer['id']),request_id=r['request_id']) for r in requests]}

# -----------------------------------------------------------------------------
# Step 17 - Private Messaging & Chat
# Messages are restricted to accepted connections. This preserves the Step 16
# relationship model and prevents arbitrary users from opening private chats.
# -----------------------------------------------------------------------------

def step17_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS private_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id INTEGER NOT NULL,
            receiver_user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            read_at TEXT,
            deleted_by_sender INTEGER NOT NULL DEFAULT 0,
            deleted_by_receiver INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(sender_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(receiver_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_private_messages_pair
            ON private_messages(sender_user_id,receiver_user_id,created_at);
        CREATE INDEX IF NOT EXISTS idx_private_messages_receiver_read
            ON private_messages(receiver_user_id,read_at);
        """)

step17_init_db()

def communication_phase1_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS communication_spaces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            privacy TEXT NOT NULL DEFAULT 'private',
            created_at TEXT NOT NULL,
            FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_space_members (
            space_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'member',
            joined_at TEXT NOT NULL,
            PRIMARY KEY(space_id,user_id),
            FOREIGN KEY(space_id) REFERENCES communication_spaces(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            space_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            channel_type TEXT NOT NULL DEFAULT 'text',
            topic TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY(space_id) REFERENCES communication_spaces(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_channel_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            reply_to_id INTEGER,
            created_at TEXT NOT NULL,
            edited_at TEXT,
            FOREIGN KEY(channel_id) REFERENCES communication_channels(id) ON DELETE CASCADE,
            FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(reply_to_id) REFERENCES communication_channel_messages(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS communication_message_reactions (
            message_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            emoji TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(message_id,user_id,emoji),
            FOREIGN KEY(message_id) REFERENCES communication_channel_messages(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_comm_channels_space ON communication_channels(space_id,id);
        CREATE INDEX IF NOT EXISTS idx_comm_messages_channel ON communication_channel_messages(channel_id,id);
        CREATE TABLE IF NOT EXISTS communication_presence (
            user_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'online',
            last_seen TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_call_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            recipient_user_id INTEGER,
            signal_type TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY(channel_id) REFERENCES communication_channels(id) ON DELETE CASCADE,
            FOREIGN KEY(sender_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(recipient_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_comm_signals_channel_id ON communication_call_signals(channel_id,id);
        CREATE TABLE IF NOT EXISTS communication_meetings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            host_user_id INTEGER NOT NULL,
            space_id INTEGER,
            channel_id INTEGER,
            title TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            meeting_code TEXT NOT NULL UNIQUE,
            scheduled_at TEXT NOT NULL DEFAULT '',
            duration_minutes INTEGER NOT NULL DEFAULT 60,
            waiting_room INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'scheduled',
            created_at TEXT NOT NULL,
            ended_at TEXT,
            FOREIGN KEY(host_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(space_id) REFERENCES communication_spaces(id) ON DELETE SET NULL,
            FOREIGN KEY(channel_id) REFERENCES communication_channels(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS communication_meeting_participants (
            meeting_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'participant',
            state TEXT NOT NULL DEFAULT 'waiting',
            hand_raised INTEGER NOT NULL DEFAULT 0,
            microphone_enabled INTEGER NOT NULL DEFAULT 1,
            camera_enabled INTEGER NOT NULL DEFAULT 0,
            joined_at TEXT NOT NULL,
            admitted_at TEXT,
            left_at TEXT,
            PRIMARY KEY(meeting_id,user_id),
            FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_meeting_chat (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meeting_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE,
            FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_comm_meetings_host ON communication_meetings(host_user_id,id DESC);
        CREATE INDEX IF NOT EXISTS idx_comm_meeting_chat ON communication_meeting_chat(meeting_id,id);
        CREATE TABLE IF NOT EXISTS communication_meeting_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meeting_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            recipient_user_id INTEGER,
            signal_type TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE,
            FOREIGN KEY(sender_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(recipient_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_comm_meeting_signals ON communication_meeting_signals(meeting_id,id);
        CREATE TABLE IF NOT EXISTS communication_settings (
            user_id INTEGER PRIMARY KEY,
            allow_direct_calls TEXT NOT NULL DEFAULT 'connections',
            allow_video_calls INTEGER NOT NULL DEFAULT 1,
            incoming_call_notifications INTEGER NOT NULL DEFAULT 1,
            message_notifications INTEGER NOT NULL DEFAULT 1,
            read_receipts INTEGER NOT NULL DEFAULT 1,
            typing_indicators INTEGER NOT NULL DEFAULT 1,
            online_status INTEGER NOT NULL DEFAULT 1,
            auto_download_media INTEGER NOT NULL DEFAULT 0,
            default_microphone TEXT NOT NULL DEFAULT '',
            default_camera TEXT NOT NULL DEFAULT '',
            default_speaker TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_direct_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            caller_user_id INTEGER NOT NULL,
            callee_user_id INTEGER NOT NULL,
            call_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ringing',
            meeting_id INTEGER,
            created_at TEXT NOT NULL,
            answered_at TEXT,
            ended_at TEXT,
            FOREIGN KEY(caller_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(callee_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_comm_direct_calls_users ON communication_direct_calls(caller_user_id,callee_user_id,id DESC);
        CREATE TABLE IF NOT EXISTS private_message_reactions (
            message_id INTEGER NOT NULL, user_id INTEGER NOT NULL, emoji TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(message_id,user_id,emoji),
            FOREIGN KEY(message_id) REFERENCES private_messages(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS private_message_saved (
            message_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(message_id,user_id),
            FOREIGN KEY(message_id) REFERENCES private_messages(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS conversation_controls (
            user_id INTEGER NOT NULL, peer_user_id INTEGER NOT NULL, muted INTEGER NOT NULL DEFAULT 0,
            blocked INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL,
            PRIMARY KEY(user_id,peer_user_id),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(peer_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT, reporter_user_id INTEGER NOT NULL, reported_user_id INTEGER NOT NULL,
            reason TEXT NOT NULL, created_at TEXT NOT NULL,
            FOREIGN KEY(reporter_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(reported_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_meeting_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, meeting_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            emoji TEXT NOT NULL, created_at TEXT NOT NULL,
            FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_meeting_polls (
            id INTEGER PRIMARY KEY AUTOINCREMENT, meeting_id INTEGER NOT NULL, creator_user_id INTEGER NOT NULL,
            question TEXT NOT NULL, options_json TEXT NOT NULL, created_at TEXT NOT NULL,
            FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE,
            FOREIGN KEY(creator_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_meeting_poll_votes (
            poll_id INTEGER NOT NULL, user_id INTEGER NOT NULL, option_index INTEGER NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(poll_id,user_id),
            FOREIGN KEY(poll_id) REFERENCES communication_meeting_polls(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS communication_meeting_invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT, meeting_id INTEGER NOT NULL, inviter_user_id INTEGER NOT NULL,
            invitee_user_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL,
            FOREIGN KEY(meeting_id) REFERENCES communication_meetings(id) ON DELETE CASCADE,
            FOREIGN KEY(inviter_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(invitee_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

communication_phase1_init_db()

class CommunicationSpaceCreate(BaseModel):
    name: str = Field(min_length=2,max_length=100)
    description: str = Field(default='',max_length=1000)
    privacy: str = Field(default='private',max_length=20)

class CommunicationChannelCreate(BaseModel):
    name: str = Field(min_length=1,max_length=80)
    channel_type: str = Field(default='text',max_length=20)
    topic: str = Field(default='',max_length=300)

class CommunicationMessageCreate(BaseModel):
    body: str = Field(min_length=1,max_length=5000)
    reply_to_id: Optional[int] = None

class CommunicationReactionCreate(BaseModel):
    emoji: str = Field(min_length=1,max_length=16)

class CommunicationMemberAdd(BaseModel):
    username: str = Field(min_length=1,max_length=100)

class CommunicationSignalCreate(BaseModel):
    recipient_user_id: Optional[int] = None
    signal_type: str = Field(min_length=1,max_length=30)
    payload: dict = Field(default_factory=dict)

class CommunicationPresenceUpdate(BaseModel):
    status: str = Field(default='online',max_length=20)

class CommunicationMeetingCreate(BaseModel):
    title: str = Field(min_length=2,max_length=160)
    description: str = Field(default='',max_length=2000)
    scheduled_at: str = Field(default='',max_length=50)
    duration_minutes: int = Field(default=60,ge=10,le=1440)
    waiting_room: bool = True
    space_id: Optional[int] = None
    channel_id: Optional[int] = None

class CommunicationMeetingChatCreate(BaseModel):
    body: str = Field(min_length=1,max_length=5000)

class CommunicationMeetingParticipantAction(BaseModel):
    action: str = Field(min_length=1,max_length=30)
    user_id: Optional[int] = None

class LiveKitTokenRequest(BaseModel):
    meeting_id: int

class CommunicationSettingsUpdate(BaseModel):
    allow_direct_calls: str = Field(default='connections',max_length=20)
    allow_video_calls: bool = True
    incoming_call_notifications: bool = True
    message_notifications: bool = True
    read_receipts: bool = True
    typing_indicators: bool = True
    online_status: bool = True
    auto_download_media: bool = False
    default_microphone: str = Field(default='',max_length=300)
    default_camera: str = Field(default='',max_length=300)
    default_speaker: str = Field(default='',max_length=300)

class DirectCallCreate(BaseModel):
    callee_user_id: int
    call_type: str = Field(default='audio',max_length=10)

class DirectCallAction(BaseModel):
    action: str = Field(min_length=1,max_length=20)

class MessageEditRequest(BaseModel):
    body: str = Field(min_length=1,max_length=5000)

class MessageReactionRequest(BaseModel):
    emoji: str = Field(min_length=1,max_length=16)

class ConversationControlRequest(BaseModel):
    action: str = Field(min_length=1,max_length=20)

class CommunicationReportRequest(BaseModel):
    reason: str = Field(min_length=3,max_length=500)

class PrivateMessageCreate(BaseModel):
    body: str = Field(min_length=1, max_length=5000)


def message_user(conn, user_id: int):
    row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        return None
    p = conn.execute("SELECT display_name,photo,headline FROM profiles WHERE user_id=?", (user_id,)).fetchone()
    return {
        "id": row["id"], "username": row["username"],
        "name": (p["display_name"] if p and p["display_name"] else row["full_name"]),
        "photo": (p["photo"] if p else ""), "headline": (p["headline"] if p else ""),
        "role": row["role"]
    }

@app.get('/api/messages/conversations')
def message_conversations(authorization: Optional[str] = Header(default=None)):
    viewer, _ = current_user(authorization)
    with db() as conn:
        peers = conn.execute("""SELECT u.* FROM users u WHERE u.id<>? AND EXISTS(
            SELECT 1 FROM connection_requests c WHERE c.status='ACCEPTED' AND
            ((c.sender_user_id=? AND c.receiver_user_id=u.id) OR
             (c.receiver_user_id=? AND c.sender_user_id=u.id))) ORDER BY u.full_name""",
            (viewer['id'], viewer['id'], viewer['id'])).fetchall()
        out=[]
        for peer in peers:
            last=conn.execute("""SELECT * FROM private_messages WHERE
                ((sender_user_id=? AND receiver_user_id=?) OR (sender_user_id=? AND receiver_user_id=?))
                AND NOT ((sender_user_id=? AND deleted_by_sender=1) OR (receiver_user_id=? AND deleted_by_receiver=1))
                ORDER BY id DESC LIMIT 1""",
                (viewer['id'],peer['id'],peer['id'],viewer['id'],viewer['id'],viewer['id'])).fetchone()
            unread=conn.execute("SELECT COUNT(*) n FROM private_messages WHERE sender_user_id=? AND receiver_user_id=? AND read_at IS NULL AND deleted_by_receiver=0",
                (peer['id'],viewer['id'])).fetchone()['n']
            out.append({"user":message_user(conn,peer['id']),"last_message":dict(last) if last else None,"unread":unread})
        out.sort(key=lambda x:(x['last_message']['created_at'] if x['last_message'] else ''), reverse=True)
        return {"conversations":out}

@app.get('/api/messages/{user_id}')
def get_private_messages(user_id:int, limit:int=100, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    limit=max(1,min(limit,200))
    with db() as conn:
        if not are_connected(conn,viewer['id'],user_id):
            raise HTTPException(403,'Private messages are available only between accepted connections')
        rows=conn.execute("""SELECT * FROM private_messages WHERE
            ((sender_user_id=? AND receiver_user_id=? AND deleted_by_sender=0) OR
             (sender_user_id=? AND receiver_user_id=? AND deleted_by_receiver=0))
            ORDER BY id DESC LIMIT ?""",(viewer['id'],user_id,user_id,viewer['id'],limit)).fetchall()
        conn.execute("UPDATE private_messages SET read_at=? WHERE sender_user_id=? AND receiver_user_id=? AND read_at IS NULL",
                     (datetime.now(timezone.utc).isoformat(),user_id,viewer['id']))
        return {"user":message_user(conn,user_id),"messages":[dict(r) for r in reversed(rows)]}

@app.post('/api/messages/{user_id}', status_code=201)
def send_private_message(user_id:int, payload:PrivateMessageCreate, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization); body=payload.body.strip()
    if not body: raise HTTPException(400,'Message cannot be empty')
    with db() as conn:
        if not are_connected(conn,viewer['id'],user_id):
            raise HTTPException(403,'You can message only accepted connections')
        if not conn.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone(): raise HTTPException(404,'User not found')
        now=datetime.now(timezone.utc).isoformat()
        cur=conn.execute("INSERT INTO private_messages(sender_user_id,receiver_user_id,body,created_at) VALUES(?,?,?,?)",
                         (viewer['id'],user_id,body,now))
        row=conn.execute('SELECT * FROM private_messages WHERE id=?',(cur.lastrowid,)).fetchone()
        create_notification(conn,user_id,viewer['id'],'NEW_MESSAGE','New message','sent you a private message','message',cur.lastrowid)
        audit(viewer['id'],'message.send',f'user:{user_id}')
        return dict(row)

@app.delete('/api/messages/message/{message_id}', status_code=204)
def delete_private_message(message_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute('SELECT * FROM private_messages WHERE id=?',(message_id,)).fetchone()
        if not row or viewer['id'] not in (row['sender_user_id'],row['receiver_user_id']): raise HTTPException(404,'Message not found')
        if viewer['id']==row['sender_user_id']: conn.execute('UPDATE private_messages SET deleted_by_sender=1 WHERE id=?',(message_id,))
        else: conn.execute('UPDATE private_messages SET deleted_by_receiver=1 WHERE id=?',(message_id,))

# ========================================
# NOVEAM COMMUNICATION PHASE 1
# ========================================
def _space_role(conn, space_id:int, user_id:int):
    r=conn.execute("SELECT role FROM communication_space_members WHERE space_id=? AND user_id=?",(space_id,user_id)).fetchone()
    return r["role"] if r else None

@app.get('/api/communication/spaces')
def communication_spaces(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        rows=conn.execute("""SELECT s.*,m.role FROM communication_spaces s
          JOIN communication_space_members m ON m.space_id=s.id WHERE m.user_id=? ORDER BY s.id DESC""",(u['id'],)).fetchall()
        out=[]
        for r in rows:
            d=dict(r)
            d['channels']=[dict(x) for x in conn.execute("SELECT * FROM communication_channels WHERE space_id=? ORDER BY id",(r['id'],)).fetchall()]
            out.append(d)
        return {'spaces':out}

@app.post('/api/communication/spaces',status_code=201)
def create_communication_space(payload:CommunicationSpaceCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    if payload.privacy not in {'private','public'}: raise HTTPException(400,'Invalid privacy')
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute("INSERT INTO communication_spaces(owner_user_id,name,description,privacy,created_at) VALUES(?,?,?,?,?)",
                         (u['id'],payload.name.strip(),payload.description.strip(),payload.privacy,now))
        sid=cur.lastrowid
        conn.execute("INSERT INTO communication_space_members(space_id,user_id,role,joined_at) VALUES(?,?,?,?)",(sid,u['id'],'owner',now))
        conn.execute("INSERT INTO communication_channels(space_id,name,channel_type,topic,created_at) VALUES(?,?,?,?,?)",(sid,'general','text','General discussion',now))
        audit(u['id'],'communication.space.create',f'space:{sid}')
        return {'id':sid}

@app.post('/api/communication/spaces/{space_id}/members',status_code=201)
def add_communication_member(space_id:int,payload:CommunicationMemberAdd,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        role=_space_role(conn,space_id,u['id'])
        if role not in {'owner','admin'}: raise HTTPException(403,'Only owners/admins can add members')
        target=conn.execute("SELECT id FROM users WHERE lower(username)=lower(?)",(payload.username.strip(),)).fetchone()
        if not target: raise HTTPException(404,'User not found')
        conn.execute("""INSERT OR IGNORE INTO communication_space_members(space_id,user_id,role,joined_at)
            VALUES(?,?,?,?)""",(space_id,target['id'],'member',datetime.now(timezone.utc).isoformat()))
        return {'ok':True}

@app.post('/api/communication/spaces/{space_id}/channels',status_code=201)
def create_communication_channel(space_id:int,payload:CommunicationChannelCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    if payload.channel_type not in {'text','announcement','voice'}: raise HTTPException(400,'Invalid channel type')
    with db() as conn:
        role=_space_role(conn,space_id,u['id'])
        if role not in {'owner','admin'}: raise HTTPException(403,'Only owners/admins can create channels')
        cur=conn.execute("INSERT INTO communication_channels(space_id,name,channel_type,topic,created_at) VALUES(?,?,?,?,?)",
                         (space_id,payload.name.strip(),payload.channel_type,payload.topic.strip(),datetime.now(timezone.utc).isoformat()))
        return {'id':cur.lastrowid}

@app.get('/api/communication/channels/{channel_id}/messages')
def communication_channel_messages(channel_id:int,limit:int=100,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); limit=max(1,min(limit,200))
    with db() as conn:
        ch=conn.execute("SELECT * FROM communication_channels WHERE id=?",(channel_id,)).fetchone()
        if not ch or not _space_role(conn,ch['space_id'],u['id']): raise HTTPException(403,'Not a member of this communication space')
        rows=conn.execute("""SELECT m.*,u.username,u.full_name,p.display_name FROM communication_channel_messages m
          JOIN users u ON u.id=m.author_user_id LEFT JOIN profiles p ON p.user_id=u.id
          WHERE m.channel_id=? ORDER BY m.id DESC LIMIT ?""",(channel_id,limit)).fetchall()
        out=[]
        for r in reversed(rows):
            d=dict(r); d['author_name']=r['display_name'] or r['full_name']
            d['reactions']=[dict(x) for x in conn.execute("""SELECT emoji,COUNT(*) count FROM communication_message_reactions
              WHERE message_id=? GROUP BY emoji""",(r['id'],)).fetchall()]
            out.append(d)
        return {'channel':dict(ch),'messages':out}

@app.post('/api/communication/channels/{channel_id}/messages',status_code=201)
def send_communication_channel_message(channel_id:int,payload:CommunicationMessageCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); body=payload.body.strip()
    if not body: raise HTTPException(400,'Message cannot be empty')
    with db() as conn:
        ch=conn.execute("SELECT * FROM communication_channels WHERE id=?",(channel_id,)).fetchone()
        if not ch or not _space_role(conn,ch['space_id'],u['id']): raise HTTPException(403,'Not a member of this communication space')
        if ch['channel_type']=='announcement' and _space_role(conn,ch['space_id'],u['id']) not in {'owner','admin'}:
            raise HTTPException(403,'Only owners/admins can post announcements')
        if payload.reply_to_id and not conn.execute("SELECT 1 FROM communication_channel_messages WHERE id=? AND channel_id=?",(payload.reply_to_id,channel_id)).fetchone():
            raise HTTPException(400,'Reply target is invalid')
        cur=conn.execute("INSERT INTO communication_channel_messages(channel_id,author_user_id,body,reply_to_id,created_at) VALUES(?,?,?,?,?)",
                         (channel_id,u['id'],body,payload.reply_to_id,datetime.now(timezone.utc).isoformat()))
        return {'id':cur.lastrowid}

@app.post('/api/communication/messages/{message_id}/reactions')
def react_communication_message(message_id:int,payload:CommunicationReactionCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        m=conn.execute("""SELECT m.id,c.space_id FROM communication_channel_messages m JOIN communication_channels c ON c.id=m.channel_id WHERE m.id=?""",(message_id,)).fetchone()
        if not m or not _space_role(conn,m['space_id'],u['id']): raise HTTPException(403,'Not allowed')
        existing=conn.execute("SELECT 1 FROM communication_message_reactions WHERE message_id=? AND user_id=? AND emoji=?",(message_id,u['id'],payload.emoji)).fetchone()
        if existing: conn.execute("DELETE FROM communication_message_reactions WHERE message_id=? AND user_id=? AND emoji=?",(message_id,u['id'],payload.emoji))
        else: conn.execute("INSERT INTO communication_message_reactions(message_id,user_id,emoji,created_at) VALUES(?,?,?,?)",(message_id,u['id'],payload.emoji,datetime.now(timezone.utc).isoformat()))
        return {'ok':True,'active':not bool(existing)}

@app.post('/api/communication/presence')
def communication_presence_update(payload:CommunicationPresenceUpdate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    if payload.status not in {'online','away','busy','offline'}: raise HTTPException(400,'Invalid presence status')
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        conn.execute("""INSERT INTO communication_presence(user_id,status,last_seen) VALUES(?,?,?)
          ON CONFLICT(user_id) DO UPDATE SET status=excluded.status,last_seen=excluded.last_seen""",(u['id'],payload.status,now))
    return {'ok':True,'status':payload.status,'last_seen':now}

@app.get('/api/communication/spaces/{space_id}/presence')
def communication_space_presence(space_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        if not _space_role(conn,space_id,u['id']): raise HTTPException(403,'Not a member of this communication space')
        rows=conn.execute("""SELECT m.user_id,u.username,u.full_name,p.display_name,
          COALESCE(pr.status,'offline') status,pr.last_seen
          FROM communication_space_members m JOIN users u ON u.id=m.user_id
          LEFT JOIN profiles p ON p.user_id=u.id LEFT JOIN communication_presence pr ON pr.user_id=u.id
          WHERE m.space_id=? ORDER BY COALESCE(p.display_name,u.full_name)""",(space_id,)).fetchall()
        return {'members':[{'user_id':r['user_id'],'username':r['username'],'name':r['display_name'] or r['full_name'],
          'status':r['status'],'last_seen':r['last_seen']} for r in rows]}

@app.post('/api/communication/channels/{channel_id}/signals',status_code=201)
def communication_send_signal(channel_id:int,payload:CommunicationSignalCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    allowed={'join','leave','offer','answer','ice','camera','microphone','screen'}
    if payload.signal_type not in allowed: raise HTTPException(400,'Invalid call signal')
    with db() as conn:
        ch=conn.execute("SELECT * FROM communication_channels WHERE id=?",(channel_id,)).fetchone()
        if not ch or ch['channel_type']!='voice' or not _space_role(conn,ch['space_id'],u['id']):
            raise HTTPException(403,'Voice/video signaling is available only to members of this voice channel')
        if payload.recipient_user_id and not _space_role(conn,ch['space_id'],payload.recipient_user_id):
            raise HTTPException(400,'Recipient is not a member of this communication space')
        cur=conn.execute("""INSERT INTO communication_call_signals(channel_id,sender_user_id,recipient_user_id,signal_type,payload,created_at)
          VALUES(?,?,?,?,?,?)""",(channel_id,u['id'],payload.recipient_user_id,payload.signal_type,json.dumps(payload.payload),datetime.now(timezone.utc).isoformat()))
        return {'id':cur.lastrowid}

@app.get('/api/communication/channels/{channel_id}/signals')
def communication_get_signals(channel_id:int,after:int=0,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        ch=conn.execute("SELECT * FROM communication_channels WHERE id=?",(channel_id,)).fetchone()
        if not ch or ch['channel_type']!='voice' or not _space_role(conn,ch['space_id'],u['id']):
            raise HTTPException(403,'Not allowed')
        rows=conn.execute("""SELECT s.*,u.username,u.full_name,p.display_name FROM communication_call_signals s
          JOIN users u ON u.id=s.sender_user_id LEFT JOIN profiles p ON p.user_id=u.id
          WHERE s.channel_id=? AND s.id>? AND s.sender_user_id<>? AND (s.recipient_user_id IS NULL OR s.recipient_user_id=?)
          ORDER BY s.id LIMIT 200""",(channel_id,after,u['id'],u['id'])).fetchall()
        out=[]
        for r in rows:
            d=dict(r); d['sender_name']=r['display_name'] or r['full_name']
            try: d['payload']=json.loads(r['payload'] or '{}')
            except: d['payload']={}
            out.append(d)
        return {'signals':out}

# ========================================
# NOVEAM COMMUNICATION PHASE 3 - MEETINGS
# ========================================
def _meeting_access(conn, meeting_id:int, user_id:int):
    m=conn.execute("SELECT * FROM communication_meetings WHERE id=?",(meeting_id,)).fetchone()
    if not m: raise HTTPException(404,'Meeting not found')
    p=conn.execute("SELECT * FROM communication_meeting_participants WHERE meeting_id=? AND user_id=?",(meeting_id,user_id)).fetchone()
    return m,p

def _meeting_hostish(conn, meeting_id:int, user_id:int):
    m,p=_meeting_access(conn,meeting_id,user_id)
    return m,p,(m['host_user_id']==user_id or (p and p['role'] in ('host','cohost')))

@app.get('/api/communication/meetings')
def communication_meetings(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        rows=conn.execute("""SELECT DISTINCT m.* FROM communication_meetings m
          LEFT JOIN communication_meeting_participants p ON p.meeting_id=m.id
          WHERE m.host_user_id=? OR p.user_id=? ORDER BY m.id DESC LIMIT 100""",(u['id'],u['id'])).fetchall()
        return {'meetings':[dict(r) for r in rows]}

@app.post('/api/communication/meetings',status_code=201)
def create_communication_meeting(payload:CommunicationMeetingCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        if payload.space_id and not _space_role(conn,payload.space_id,u['id']): raise HTTPException(403,'Not a member of that communication space')
        if payload.channel_id:
            ch=conn.execute("SELECT * FROM communication_channels WHERE id=?",(payload.channel_id,)).fetchone()
            if not ch or (payload.space_id and ch['space_id']!=payload.space_id): raise HTTPException(400,'Invalid meeting channel')
        code=secrets.token_urlsafe(7).replace('-','').replace('_','')[:10].upper()
        cur=conn.execute("""INSERT INTO communication_meetings(host_user_id,space_id,channel_id,title,description,meeting_code,scheduled_at,duration_minutes,waiting_room,status,created_at)
          VALUES(?,?,?,?,?,?,?,?,?,'scheduled',?)""",(u['id'],payload.space_id,payload.channel_id,payload.title.strip(),payload.description.strip(),code,payload.scheduled_at.strip(),payload.duration_minutes,1 if payload.waiting_room else 0,now))
        mid=cur.lastrowid
        conn.execute("""INSERT INTO communication_meeting_participants(meeting_id,user_id,role,state,joined_at,admitted_at)
          VALUES(?,?,'host','joined',?,?)""",(mid,u['id'],now,now))
        return {'id':mid,'meeting_code':code}

@app.post('/api/communication/meetings/join/{meeting_code}')
def join_communication_meeting(meeting_code:str,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        m=conn.execute("SELECT * FROM communication_meetings WHERE upper(meeting_code)=upper(?)",(meeting_code.strip(),)).fetchone()
        if not m: raise HTTPException(404,'Meeting not found')
        if m['status']=='ended': raise HTTPException(409,'Meeting has ended')
        state='joined' if (m['host_user_id']==u['id'] or not m['waiting_room']) else 'waiting'
        role='host' if m['host_user_id']==u['id'] else 'participant'
        conn.execute("""INSERT INTO communication_meeting_participants(meeting_id,user_id,role,state,joined_at,admitted_at,left_at)
          VALUES(?,?,?,?,?,?,NULL) ON CONFLICT(meeting_id,user_id) DO UPDATE SET state=excluded.state,joined_at=excluded.joined_at,left_at=NULL""",
          (m['id'],u['id'],role,state,now,now if state=='joined' else None))
        return {'meeting_id':m['id'],'state':state}

@app.get('/api/communication/meetings/{meeting_id}')
def communication_meeting_detail(meeting_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        m,p=_meeting_access(conn,meeting_id,u['id'])
        if m['host_user_id']!=u['id'] and not p: raise HTTPException(403,'Join this meeting first')
        people=conn.execute("""SELECT mp.*,u.username,u.full_name,pr.display_name FROM communication_meeting_participants mp
          JOIN users u ON u.id=mp.user_id LEFT JOIN profiles pr ON pr.user_id=u.id WHERE mp.meeting_id=? ORDER BY mp.role,mp.joined_at""",(meeting_id,)).fetchall()
        return {'meeting':dict(m),'me':dict(p) if p else None,'participants':[dict(x) for x in people]}

@app.post('/api/communication/meetings/{meeting_id}/actions')
def communication_meeting_action(meeting_id:int,payload:CommunicationMeetingParticipantAction,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        m,p,hostish=_meeting_hostish(conn,meeting_id,u['id'])
        a=payload.action
        if a=='raise_hand':
            if not p: raise HTTPException(403,'Not a participant')
            conn.execute("UPDATE communication_meeting_participants SET hand_raised=1 WHERE meeting_id=? AND user_id=?",(meeting_id,u['id']))
        elif a=='lower_hand':
            target=payload.user_id or u['id']
            if target!=u['id'] and not hostish: raise HTTPException(403,'Not allowed')
            conn.execute("UPDATE communication_meeting_participants SET hand_raised=0 WHERE meeting_id=? AND user_id=?",(meeting_id,target))
        elif a in {'admit','make_cohost','remove','mute'}:
            if not hostish or not payload.user_id: raise HTTPException(403,'Host/co-host permission required')
            if a=='admit': conn.execute("UPDATE communication_meeting_participants SET state='joined',admitted_at=? WHERE meeting_id=? AND user_id=?",(now,meeting_id,payload.user_id))
            elif a=='make_cohost': conn.execute("UPDATE communication_meeting_participants SET role='cohost' WHERE meeting_id=? AND user_id=?",(meeting_id,payload.user_id))
            elif a=='remove': conn.execute("UPDATE communication_meeting_participants SET state='removed',left_at=? WHERE meeting_id=? AND user_id=?",(now,meeting_id,payload.user_id))
            elif a=='mute': conn.execute("UPDATE communication_meeting_participants SET microphone_enabled=0 WHERE meeting_id=? AND user_id=?",(meeting_id,payload.user_id))
        elif a=='leave':
            conn.execute("UPDATE communication_meeting_participants SET state='left',left_at=? WHERE meeting_id=? AND user_id=?",(now,meeting_id,u['id']))
        elif a=='end':
            if m['host_user_id']!=u['id']: raise HTTPException(403,'Only the host can end the meeting')
            conn.execute("UPDATE communication_meetings SET status='ended',ended_at=? WHERE id=?",(now,meeting_id))
        else: raise HTTPException(400,'Invalid meeting action')
        return {'ok':True}

@app.get('/api/communication/meetings/{meeting_id}/chat')
def communication_meeting_chat(meeting_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        m,p=_meeting_access(conn,meeting_id,u['id'])
        if m['host_user_id']!=u['id'] and (not p or p['state']!='joined'): raise HTTPException(403,'Not admitted to meeting')
        rows=conn.execute("""SELECT c.*,u.username,u.full_name,pr.display_name FROM communication_meeting_chat c
          JOIN users u ON u.id=c.author_user_id LEFT JOIN profiles pr ON pr.user_id=u.id
          WHERE c.meeting_id=? ORDER BY c.id DESC LIMIT 200""",(meeting_id,)).fetchall()
        return {'messages':[dict(x) for x in reversed(rows)]}

@app.post('/api/communication/meetings/{meeting_id}/chat',status_code=201)
def communication_meeting_chat_send(meeting_id:int,payload:CommunicationMeetingChatCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); body=payload.body.strip()
    with db() as conn:
        m,p=_meeting_access(conn,meeting_id,u['id'])
        if m['host_user_id']!=u['id'] and (not p or p['state']!='joined'): raise HTTPException(403,'Not admitted to meeting')
        cur=conn.execute("INSERT INTO communication_meeting_chat(meeting_id,author_user_id,body,created_at) VALUES(?,?,?,?)",(meeting_id,u['id'],body,datetime.now(timezone.utc).isoformat()))
        return {'id':cur.lastrowid}

# ========================================
# NOVEAM COMMUNICATION PHASE 4 - MEETING WEBRTC
# ========================================
@app.post('/api/communication/meetings/{meeting_id}/signals',status_code=201)
def communication_meeting_send_signal(meeting_id:int,payload:CommunicationSignalCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    allowed={'join','leave','offer','answer','ice','camera','microphone','screen'}
    if payload.signal_type not in allowed: raise HTTPException(400,'Invalid meeting signal')
    with db() as conn:
        m,p=_meeting_access(conn,meeting_id,u['id'])
        if m['status']=='ended': raise HTTPException(409,'Meeting has ended')
        if m['host_user_id']!=u['id'] and (not p or p['state']!='joined'):
            raise HTTPException(403,'You must be admitted before joining meeting media')
        if payload.recipient_user_id:
            rp=conn.execute("SELECT state FROM communication_meeting_participants WHERE meeting_id=? AND user_id=?",(meeting_id,payload.recipient_user_id)).fetchone()
            if not rp or rp['state']!='joined': raise HTTPException(400,'Recipient is not an admitted participant')
        cur=conn.execute("""INSERT INTO communication_meeting_signals(meeting_id,sender_user_id,recipient_user_id,signal_type,payload,created_at)
          VALUES(?,?,?,?,?,?)""",(meeting_id,u['id'],payload.recipient_user_id,payload.signal_type,json.dumps(payload.payload),datetime.now(timezone.utc).isoformat()))
        return {'id':cur.lastrowid}

@app.get('/api/communication/meetings/{meeting_id}/signals')
def communication_meeting_get_signals(meeting_id:int,after:int=0,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        m,p=_meeting_access(conn,meeting_id,u['id'])
        if m['host_user_id']!=u['id'] and (not p or p['state']!='joined'): raise HTTPException(403,'Not admitted to meeting')
        rows=conn.execute("""SELECT s.*,u.username,u.full_name,pr.display_name FROM communication_meeting_signals s
          JOIN users u ON u.id=s.sender_user_id LEFT JOIN profiles pr ON pr.user_id=u.id
          WHERE s.meeting_id=? AND s.id>? AND s.sender_user_id<>? AND (s.recipient_user_id IS NULL OR s.recipient_user_id=?)
          ORDER BY s.id LIMIT 300""",(meeting_id,after,u['id'],u['id'])).fetchall()
        out=[]
        for r in rows:
            d=dict(r); d['sender_name']=r['display_name'] or r['full_name']
            try: d['payload']=json.loads(r['payload'] or '{}')
            except: d['payload']={}
            out.append(d)
        return {'signals':out}

# ========================================
# NOVEAM COMMUNICATION PHASE 5 - LIVEKIT SFU
# ========================================
@app.get('/api/communication/livekit/status')
def communication_livekit_status(authorization:Optional[str]=Header(default=None)):
    current_user(authorization)
    configured=bool(LIVEKIT_URL and LIVEKIT_API_KEY and LIVEKIT_API_SECRET and livekit_api)
    return {
        'configured': configured,
        'server_url': LIVEKIT_URL if configured else '',
        'sdk_installed': bool(livekit_api),
        'mode': 'livekit-sfu' if configured else 'phase4-peer-to-peer-fallback'
    }

@app.post('/api/communication/livekit/token')
def communication_livekit_token(payload:LiveKitTokenRequest,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    if not livekit_api:
        raise HTTPException(503,'LiveKit Python SDK is not installed. Install livekit-api.')
    if not (LIVEKIT_URL and LIVEKIT_API_KEY and LIVEKIT_API_SECRET):
        raise HTTPException(503,'LiveKit environment variables are not configured')
    with db() as conn:
        m,p=_meeting_access(conn,payload.meeting_id,u['id'])
        if m['status']=='ended': raise HTTPException(409,'Meeting has ended')
        if m['host_user_id']!=u['id'] and (not p or p['state']!='joined'):
            raise HTTPException(403,'You must be admitted before joining meeting media')
        # Opaque identifiers avoid putting user PII into LiveKit identity/room names.
        room_name=f"noveam-meeting-{m['id']}"
        participant_identity=f"noveam-user-{u['id']}-{uuid.uuid4().hex[:8]}"
        display_name=u['full_name'] or u['username']
        token=(livekit_api.AccessToken(LIVEKIT_API_KEY,LIVEKIT_API_SECRET)
            .with_identity(participant_identity)
            .with_name(display_name)
            .with_grants(livekit_api.VideoGrants(
                room_join=True,
                room=room_name,
                can_publish=True,
                can_subscribe=True,
                can_publish_data=True
            )).to_jwt())
        return {
            'server_url': LIVEKIT_URL,
            'participant_token': token,
            'room_name': room_name,
            'participant_identity': participant_identity
        }

# ========================================
# NOVEAM COMMUNICATION PHASE 6 - CALLS & SETTINGS
# ========================================
def _communication_settings_row(conn,user_id:int):
    row=conn.execute("SELECT * FROM communication_settings WHERE user_id=?",(user_id,)).fetchone()
    if row: return row
    now=datetime.now(timezone.utc).isoformat()
    conn.execute("INSERT INTO communication_settings(user_id,updated_at) VALUES(?,?)",(user_id,now))
    return conn.execute("SELECT * FROM communication_settings WHERE user_id=?",(user_id,)).fetchone()

@app.get('/api/communication/settings')
def get_communication_settings(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn: return dict(_communication_settings_row(conn,u['id']))

@app.put('/api/communication/settings')
def update_communication_settings(payload:CommunicationSettingsUpdate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    if payload.allow_direct_calls not in {'everyone','connections','nobody'}: raise HTTPException(400,'Invalid direct-call privacy')
    with db() as conn:
        _communication_settings_row(conn,u['id'])
        conn.execute("""UPDATE communication_settings SET allow_direct_calls=?,allow_video_calls=?,incoming_call_notifications=?,
          message_notifications=?,read_receipts=?,typing_indicators=?,online_status=?,auto_download_media=?,
          default_microphone=?,default_camera=?,default_speaker=?,updated_at=? WHERE user_id=?""",
          (payload.allow_direct_calls,int(payload.allow_video_calls),int(payload.incoming_call_notifications),int(payload.message_notifications),
           int(payload.read_receipts),int(payload.typing_indicators),int(payload.online_status),int(payload.auto_download_media),
           payload.default_microphone,payload.default_camera,payload.default_speaker,datetime.now(timezone.utc).isoformat(),u['id']))
        return dict(_communication_settings_row(conn,u['id']))

@app.post('/api/communication/direct-calls',status_code=201)
def create_direct_call(payload:DirectCallCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    if payload.call_type not in {'audio','video'}: raise HTTPException(400,'Call type must be audio or video')
    if payload.callee_user_id==u['id']: raise HTTPException(400,'You cannot call yourself')
    with db() as conn:
        target=conn.execute("SELECT id FROM users WHERE id=?",(payload.callee_user_id,)).fetchone()
        if not target: raise HTTPException(404,'User not found')
        settings=_communication_settings_row(conn,payload.callee_user_id)
        policy=settings['allow_direct_calls']
        if policy=='nobody': raise HTTPException(403,'This user is not accepting direct calls')
        if policy=='connections' and not are_connected(conn,u['id'],payload.callee_user_id):
            raise HTTPException(403,'This user accepts calls only from connections')
        if payload.call_type=='video' and not settings['allow_video_calls']:
            raise HTTPException(403,'This user is not accepting video calls')
        now=datetime.now(timezone.utc).isoformat()
        title=('Video' if payload.call_type=='video' else 'Audio')+' call'
        code=secrets.token_urlsafe(7).replace('-','').replace('_','')[:10].upper()
        mcur=conn.execute("""INSERT INTO communication_meetings(host_user_id,title,description,meeting_code,scheduled_at,duration_minutes,waiting_room,status,created_at)
          VALUES(?,?,?,?,'',60,0,'scheduled',?)""",(u['id'],title,'Direct NOVEAM call',code,now))
        mid=mcur.lastrowid
        conn.execute("""INSERT INTO communication_meeting_participants(meeting_id,user_id,role,state,joined_at,admitted_at)
          VALUES(?,?,'host','joined',?,?)""",(mid,u['id'],now,now))
        conn.execute("""INSERT INTO communication_meeting_participants(meeting_id,user_id,role,state,joined_at,admitted_at)
          VALUES(?,?,'participant','joined',?,?)""",(mid,payload.callee_user_id,now,now))
        cur=conn.execute("""INSERT INTO communication_direct_calls(caller_user_id,callee_user_id,call_type,status,meeting_id,created_at)
          VALUES(?,?,?,'ringing',?,?)""",(u['id'],payload.callee_user_id,payload.call_type,mid,now))
        if settings['incoming_call_notifications']:
            create_notification(conn,payload.callee_user_id,u['id'],'INCOMING_CALL','Incoming '+payload.call_type+' call','is calling you','meeting',mid)
        return {'call_id':cur.lastrowid,'meeting_id':mid,'meeting_code':code,'call_type':payload.call_type}

@app.get('/api/communication/direct-calls/incoming')
def incoming_direct_calls(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        rows=conn.execute("""SELECT c.*,u.username,u.full_name,p.display_name FROM communication_direct_calls c
          JOIN users u ON u.id=c.caller_user_id LEFT JOIN profiles p ON p.user_id=u.id
          WHERE c.callee_user_id=? AND c.status='ringing' ORDER BY c.id DESC LIMIT 20""",(u['id'],)).fetchall()
        return {'calls':[dict(x) for x in rows]}

@app.post('/api/communication/direct-calls/{call_id}/action')
def direct_call_action(call_id:int,payload:DirectCallAction,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        c=conn.execute("SELECT * FROM communication_direct_calls WHERE id=?",(call_id,)).fetchone()
        if not c or u['id'] not in (c['caller_user_id'],c['callee_user_id']): raise HTTPException(404,'Call not found')
        if payload.action=='accept':
            if u['id']!=c['callee_user_id']: raise HTTPException(403,'Only the recipient can accept')
            conn.execute("UPDATE communication_direct_calls SET status='active',answered_at=? WHERE id=?",(now,call_id))
        elif payload.action in {'decline','end'}:
            conn.execute("UPDATE communication_direct_calls SET status=?,ended_at=? WHERE id=?",('declined' if payload.action=='decline' else 'ended',now,call_id))
            conn.execute("UPDATE communication_meetings SET status='ended',ended_at=? WHERE id=?",(now,c['meeting_id']))
        else: raise HTTPException(400,'Invalid call action')
        return {'ok':True,'meeting_id':c['meeting_id'],'status':'active' if payload.action=='accept' else payload.action}


class MeetingReactionRequest(BaseModel):
    emoji: str = Field(min_length=1,max_length=16)

class MeetingPollCreate(BaseModel):
    question: str = Field(min_length=2,max_length=300)
    options: list[str]

class MeetingPollVote(BaseModel):
    option_index: int

class MeetingInviteCreate(BaseModel):
    invitee_user_id: int

class RealtimeMessageEvent(BaseModel):
    peer_id: int
    message_id: Optional[int] = None
    event: str = Field(default="message",max_length=30)

@app.post("/api/communication/realtime/fanout")
async def communication_realtime_fanout(payload:RealtimeMessageEvent,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    if payload.event not in {"message","read","deleted","edited","reaction"}: raise HTTPException(400,"Invalid realtime event")
    with db() as conn:
        if not are_connected(conn,u["id"],payload.peer_id): raise HTTPException(403,"Accepted connection required")
    await realtime_hub.send(payload.peer_id,{"type":payload.event,"user_id":u["id"],"message_id":payload.message_id})
    return {"ok":True}

@app.get("/api/communication/presence/{user_id}")
def communication_realtime_presence(user_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        if user_id!=u["id"] and not are_connected(conn,u["id"],user_id): raise HTTPException(403,"Accepted connection required")
        visible=bool(_communication_settings_row(conn,user_id)["online_status"])
    return {"user_id":user_id,"online":bool(visible and realtime_hub.online(user_id))}


# ========================================
# NOVEAM COMMUNICATION PHASE 8 - COMPLETE MESSAGING
# ========================================
def _private_message_access(conn,message_id:int,user_id:int):
    row=conn.execute("SELECT * FROM private_messages WHERE id=?",(message_id,)).fetchone()
    if not row or user_id not in (row["sender_user_id"],row["receiver_user_id"]):
        raise HTTPException(404,"Message not found")
    return row

@app.put("/api/messages/message/{message_id}/edit")
async def edit_private_message(message_id:int,payload:MessageEditRequest,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        m=_private_message_access(conn,message_id,u["id"])
        if m["sender_user_id"]!=u["id"]: raise HTTPException(403,"Only the sender can edit this message")
        conn.execute("UPDATE private_messages SET body=? WHERE id=?",(payload.body.strip(),message_id))
        peer=m["receiver_user_id"]
    await realtime_hub.send(peer,{"type":"edited","user_id":u["id"],"message_id":message_id})
    return {"ok":True,"edited_at":now}

@app.post("/api/messages/message/{message_id}/reaction")
async def react_private_message(message_id:int,payload:MessageReactionRequest,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        m=_private_message_access(conn,message_id,u["id"])
        exists=conn.execute("SELECT 1 FROM private_message_reactions WHERE message_id=? AND user_id=? AND emoji=?",
                            (message_id,u["id"],payload.emoji)).fetchone()
        if exists: conn.execute("DELETE FROM private_message_reactions WHERE message_id=? AND user_id=? AND emoji=?",(message_id,u["id"],payload.emoji))
        else: conn.execute("INSERT INTO private_message_reactions(message_id,user_id,emoji,created_at) VALUES(?,?,?,?)",(message_id,u["id"],payload.emoji,now))
        peer=m["receiver_user_id"] if m["sender_user_id"]==u["id"] else m["sender_user_id"]
    await realtime_hub.send(peer,{"type":"reaction","user_id":u["id"],"message_id":message_id})
    return {"ok":True,"active":not bool(exists)}

@app.post("/api/messages/message/{message_id}/save")
def save_private_message(message_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _private_message_access(conn,message_id,u["id"])
        exists=conn.execute("SELECT 1 FROM private_message_saved WHERE message_id=? AND user_id=?",(message_id,u["id"])).fetchone()
        if exists: conn.execute("DELETE FROM private_message_saved WHERE message_id=? AND user_id=?",(message_id,u["id"]))
        else: conn.execute("INSERT INTO private_message_saved(message_id,user_id,created_at) VALUES(?,?,?)",(message_id,u["id"],now))
    return {"ok":True,"saved":not bool(exists)}

@app.get("/api/messages/search/{peer_user_id}")
def search_private_messages(peer_user_id:int,q:str="",authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); q=q.strip()
    if len(q)<2: return {"results":[]}
    with db() as conn:
        if not are_connected(conn,u["id"],peer_user_id): raise HTTPException(403,"Accepted connection required")
        rows=conn.execute("""SELECT * FROM private_messages WHERE
          ((sender_user_id=? AND receiver_user_id=?) OR (sender_user_id=? AND receiver_user_id=?))
          AND body LIKE ? ORDER BY id DESC LIMIT 100""",(u["id"],peer_user_id,peer_user_id,u["id"],f"%{q}%")).fetchall()
    return {"results":[dict(x) for x in rows]}

@app.get("/api/communication/call-history")
def communication_call_history(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        rows=conn.execute("""SELECT c.*,
          CASE WHEN c.caller_user_id=? THEN c.callee_user_id ELSE c.caller_user_id END peer_user_id,
          CASE WHEN c.caller_user_id=? THEN 'outgoing' ELSE 'incoming' END direction
          FROM communication_direct_calls c WHERE c.caller_user_id=? OR c.callee_user_id=?
          ORDER BY c.id DESC LIMIT 100""",(u["id"],u["id"],u["id"],u["id"])).fetchall()
    return {"calls":[dict(x) for x in rows]}

@app.get("/api/communication/conversation-controls/{peer_user_id}")
def get_conversation_controls(peer_user_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        r=conn.execute("SELECT * FROM conversation_controls WHERE user_id=? AND peer_user_id=?",(u["id"],peer_user_id)).fetchone()
    return dict(r) if r else {"user_id":u["id"],"peer_user_id":peer_user_id,"muted":0,"blocked":0}

@app.post("/api/communication/conversation-controls/{peer_user_id}")
def set_conversation_controls(peer_user_id:int,payload:ConversationControlRequest,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    if payload.action not in {"mute","unmute","block","unblock"}: raise HTTPException(400,"Invalid action")
    with db() as conn:
        conn.execute("""INSERT OR IGNORE INTO conversation_controls(user_id,peer_user_id,updated_at) VALUES(?,?,?)""",(u["id"],peer_user_id,now))
        if payload.action in {"mute","unmute"}:
            conn.execute("UPDATE conversation_controls SET muted=?,updated_at=? WHERE user_id=? AND peer_user_id=?",(1 if payload.action=="mute" else 0,now,u["id"],peer_user_id))
        else:
            conn.execute("UPDATE conversation_controls SET blocked=?,updated_at=? WHERE user_id=? AND peer_user_id=?",(1 if payload.action=="block" else 0,now,u["id"],peer_user_id))
    return {"ok":True}

@app.post("/api/communication/report/{peer_user_id}",status_code=201)
def report_communication_user(peer_user_id:int,payload:CommunicationReportRequest,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    if peer_user_id==u["id"]: raise HTTPException(400,"Invalid user")
    with db() as conn:
        conn.execute("INSERT INTO communication_reports(reporter_user_id,reported_user_id,reason,created_at) VALUES(?,?,?,?)",
                     (u["id"],peer_user_id,payload.reason.strip(),now))
    return {"ok":True}


# ========================================
# NOVEAM COMMUNICATION PHASE 9 - ADVANCED CALLS & MEETINGS
# ========================================
def _meeting_member(conn,meeting_id:int,user_id:int):
    r=conn.execute("SELECT * FROM communication_meeting_participants WHERE meeting_id=? AND user_id=?",(meeting_id,user_id)).fetchone()
    if not r: raise HTTPException(403,"Meeting access required")
    return r

@app.post("/api/communication/meetings/{meeting_id}/reaction",status_code=201)
async def meeting_reaction(meeting_id:int,payload:MeetingReactionRequest,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _meeting_member(conn,meeting_id,u["id"])
        conn.execute("INSERT INTO communication_meeting_reactions(meeting_id,user_id,emoji,created_at) VALUES(?,?,?,?)",
                     (meeting_id,u["id"],payload.emoji,now))
    return {"ok":True,"emoji":payload.emoji,"user_id":u["id"],"created_at":now}

@app.get("/api/communication/meetings/{meeting_id}/reactions")
def meeting_reactions(meeting_id:int,after_id:int=0,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        _meeting_member(conn,meeting_id,u["id"])
        rows=conn.execute("SELECT * FROM communication_meeting_reactions WHERE meeting_id=? AND id>? ORDER BY id LIMIT 100",
                          (meeting_id,after_id)).fetchall()
    return {"reactions":[dict(x) for x in rows]}

@app.post("/api/communication/meetings/{meeting_id}/polls",status_code=201)
def create_meeting_poll(meeting_id:int,payload:MeetingPollCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    opts=[str(x).strip() for x in payload.options if str(x).strip()]
    if len(opts)<2 or len(opts)>10: raise HTTPException(400,"Poll requires 2 to 10 options")
    with db() as conn:
        _meeting_member(conn,meeting_id,u["id"])
        cur=conn.execute("INSERT INTO communication_meeting_polls(meeting_id,creator_user_id,question,options_json,created_at) VALUES(?,?,?,?,?)",
                         (meeting_id,u["id"],payload.question.strip(),json.dumps(opts),now))
        pid=cur.lastrowid
    return {"id":pid,"question":payload.question.strip(),"options":opts}

@app.get("/api/communication/meetings/{meeting_id}/polls")
def list_meeting_polls(meeting_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        _meeting_member(conn,meeting_id,u["id"])
        rows=conn.execute("SELECT * FROM communication_meeting_polls WHERE meeting_id=? ORDER BY id DESC",(meeting_id,)).fetchall()
        out=[]
        for r in rows:
            d=dict(r); d["options"]=json.loads(d.pop("options_json"))
            votes=conn.execute("SELECT option_index,COUNT(*) n FROM communication_meeting_poll_votes WHERE poll_id=? GROUP BY option_index",(r["id"],)).fetchall()
            d["votes"]={str(v["option_index"]):v["n"] for v in votes}; out.append(d)
    return {"polls":out}

@app.post("/api/communication/meetings/polls/{poll_id}/vote")
def vote_meeting_poll(poll_id:int,payload:MeetingPollVote,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        p=conn.execute("SELECT * FROM communication_meeting_polls WHERE id=?",(poll_id,)).fetchone()
        if not p: raise HTTPException(404,"Poll not found")
        _meeting_member(conn,p["meeting_id"],u["id"])
        opts=json.loads(p["options_json"])
        if payload.option_index<0 or payload.option_index>=len(opts): raise HTTPException(400,"Invalid option")
        conn.execute("""INSERT INTO communication_meeting_poll_votes(poll_id,user_id,option_index,created_at)
          VALUES(?,?,?,?) ON CONFLICT(poll_id,user_id) DO UPDATE SET option_index=excluded.option_index,created_at=excluded.created_at""",
          (poll_id,u["id"],payload.option_index,now))
    return {"ok":True}

@app.post("/api/communication/meetings/{meeting_id}/invite",status_code=201)
def invite_to_meeting(meeting_id:int,payload:MeetingInviteCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        me=_meeting_member(conn,meeting_id,u["id"])
        meeting=conn.execute("SELECT * FROM communication_meetings WHERE id=?",(meeting_id,)).fetchone()
        if not meeting: raise HTTPException(404,"Meeting not found")
        if payload.invitee_user_id==u["id"]: raise HTTPException(400,"Cannot invite yourself")
        target=conn.execute("SELECT id FROM users WHERE id=?",(payload.invitee_user_id,)).fetchone()
        if not target: raise HTTPException(404,"User not found")
        cur=conn.execute("INSERT INTO communication_meeting_invites(meeting_id,inviter_user_id,invitee_user_id,status,created_at) VALUES(?,?,?,?,?)",
                         (meeting_id,u["id"],payload.invitee_user_id,"pending",now))
        try:
            create_notification(conn,payload.invitee_user_id,"MEETING_INVITE","Meeting invitation",f"You were invited to meeting {meeting['title'] if 'title' in meeting.keys() else meeting_id}")
        except Exception: pass
    return {"ok":True,"invite_id":cur.lastrowid}

@app.get("/api/communication/meeting-invites")
def my_meeting_invites(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        rows=conn.execute("""SELECT i.*,m.* FROM communication_meeting_invites i
          JOIN communication_meetings m ON m.id=i.meeting_id
          WHERE i.invitee_user_id=? AND i.status='pending' ORDER BY i.id DESC LIMIT 50""",(u["id"],)).fetchall()
    return {"invites":[dict(x) for x in rows]}

# ========================================
# STEP 19 - PAGES, ORGANIZATIONS & COMPANY PROFILES
# ========================================
def step19_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS organization_pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            page_type TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT '',
            location TEXT NOT NULL DEFAULT '',
            about TEXT NOT NULL DEFAULT '',
            website TEXT NOT NULL DEFAULT '',
            contact TEXT NOT NULL DEFAULT '',
            services TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS organization_page_roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id INTEGER NOT NULL,
            created_by_user_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            person TEXT NOT NULL,
            designation TEXT NOT NULL DEFAULT '',
            contact TEXT NOT NULL DEFAULT '',
            about TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE CASCADE,
            FOREIGN KEY(created_by_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_org_pages_owner ON organization_pages(owner_user_id,id DESC);
        CREATE INDEX IF NOT EXISTS idx_org_pages_type ON organization_pages(page_type);
        CREATE INDEX IF NOT EXISTS idx_org_roles_page ON organization_page_roles(page_id,id DESC);
        """)
step19_init_db()

class OrganizationPagePayload(BaseModel):
    name: str = Field(min_length=1, max_length=150)
    page_type: str = Field(min_length=1, max_length=100)
    category: str = Field(default='', max_length=120)
    location: str = Field(default='', max_length=160)
    about: str = Field(default='', max_length=5000)
    website: str = Field(default='', max_length=500)
    contact: str = Field(default='', max_length=500)
    services: str = Field(default='', max_length=2000)

class OrganizationRolePayload(BaseModel):
    role: str = Field(min_length=1, max_length=100)
    person: str = Field(min_length=1, max_length=150)
    designation: str = Field(default='', max_length=150)
    contact: str = Field(default='', max_length=500)
    about: str = Field(default='', max_length=3000)

def _table_exists(conn,name:str):
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone())

def serialize_org_page(row, viewer_id:int):
    d=dict(row); d['is_owner']=row['owner_user_id']==viewer_id
    with db() as conn:
        owner=conn.execute("SELECT username,full_name FROM users WHERE id=?",(row['owner_user_id'],)).fetchone()
        d['owner_username']=owner['username'] if owner else ''
        d['owner_name']=owner['full_name'] if owner else ''
        d['role_count']=conn.execute("SELECT COUNT(*) n FROM organization_page_roles WHERE page_id=?",(row['id'],)).fetchone()['n']
        d['follower_count']=conn.execute("SELECT COUNT(*) n FROM organization_page_followers WHERE page_id=?",(row['id'],)).fetchone()['n'] if _table_exists(conn,'organization_page_followers') else 0
        m=conn.execute("SELECT role FROM organization_page_members WHERE page_id=? AND user_id=?",(row['id'],viewer_id)).fetchone() if _table_exists(conn,'organization_page_members') else None
        d['member_role']=m['role'] if m else ('Owner' if d['is_owner'] else '')
        d['is_member']=bool(m) or d['is_owner']
        d['is_following']=bool(conn.execute("SELECT 1 FROM organization_page_followers WHERE page_id=? AND user_id=?",(row['id'],viewer_id)).fetchone()) if _table_exists(conn,'organization_page_followers') else False
        d['can_manage_members']=d['is_owner'] or d['member_role']=='Admin'
        d['can_edit']=d['is_owner'] or d['member_role'] in ('Admin','Editor')
    return d

@app.get('/api/pages')
def list_organization_pages(page_type:Optional[str]=None, mine:bool=False, q:str='', authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    sql='SELECT * FROM organization_pages WHERE 1=1'; args=[]
    if mine: sql+=' AND owner_user_id=?'; args.append(viewer['id'])
    if page_type and page_type!='All': sql+=' AND page_type=?'; args.append(page_type)
    if q.strip(): sql+=' AND (name LIKE ? OR category LIKE ? OR about LIKE ?)'; term='%'+q.strip()+'%'; args += [term,term,term]
    sql+=' ORDER BY id DESC LIMIT 300'
    with db() as conn: rows=conn.execute(sql,args).fetchall()
    return {'pages':[serialize_org_page(r,viewer['id']) for r in rows]}

@app.get('/api/pages/{page_id}')
def get_organization_page(page_id:int, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute('SELECT * FROM organization_pages WHERE id=?',(page_id,)).fetchone()
        if not row: raise HTTPException(404,'Page not found')
        roles=conn.execute('SELECT * FROM organization_page_roles WHERE page_id=? ORDER BY id DESC',(page_id,)).fetchall()
    return {'page':serialize_org_page(row,viewer['id']),'roles':[dict(r) for r in roles]}

@app.post('/api/pages', status_code=201)
def create_organization_page(payload:OrganizationPagePayload, authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute('''INSERT INTO organization_pages(owner_user_id,name,page_type,category,location,about,website,contact,services,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)''',(viewer['id'],payload.name.strip(),payload.page_type.strip(),payload.category.strip(),payload.location.strip(),payload.about.strip(),payload.website.strip(),payload.contact.strip(),payload.services.strip(),now,now))
        row=conn.execute('SELECT * FROM organization_pages WHERE id=?',(cur.lastrowid,)).fetchone()
    audit(viewer['id'],'page.create',f'page:{row["id"]}'); return serialize_org_page(row,viewer['id'])

@app.put('/api/pages/{page_id}')
def update_organization_page(page_id:int,payload:OrganizationPagePayload,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute('SELECT * FROM organization_pages WHERE id=?',(page_id,)).fetchone()
        if not row: raise HTTPException(404,'Page not found')
        if row['owner_user_id']!=viewer['id'] and not conn.execute("SELECT 1 FROM organization_page_members WHERE page_id=? AND user_id=? AND role IN ('Admin','Editor')",(page_id,viewer['id'])).fetchone(): raise HTTPException(403,'Only the Page owner, Admin or Editor can edit this Page')
        conn.execute('''UPDATE organization_pages SET name=?,page_type=?,category=?,location=?,about=?,website=?,contact=?,services=?,updated_at=? WHERE id=?''',
        (payload.name.strip(),payload.page_type.strip(),payload.category.strip(),payload.location.strip(),payload.about.strip(),payload.website.strip(),payload.contact.strip(),payload.services.strip(),datetime.now(timezone.utc).isoformat(),page_id))
        row=conn.execute('SELECT * FROM organization_pages WHERE id=?',(page_id,)).fetchone()
    return serialize_org_page(row,viewer['id'])

@app.delete('/api/pages/{page_id}',status_code=204)
def delete_organization_page(page_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute('SELECT * FROM organization_pages WHERE id=?',(page_id,)).fetchone()
        if not row: raise HTTPException(404,'Page not found')
        if row['owner_user_id']!=viewer['id']: raise HTTPException(403,'Only the Page owner can delete this Page')
        conn.execute('DELETE FROM organization_page_roles WHERE page_id=?',(page_id,)); conn.execute('DELETE FROM organization_pages WHERE id=?',(page_id,))
    audit(viewer['id'],'page.delete',f'page:{page_id}')

@app.get('/api/pages/{page_id}/roles')
def list_organization_roles(page_id:int,authorization:Optional[str]=Header(default=None)):
    current_user(authorization)
    with db() as conn:
        if not conn.execute('SELECT 1 FROM organization_pages WHERE id=?',(page_id,)).fetchone(): raise HTTPException(404,'Page not found')
        rows=conn.execute('SELECT * FROM organization_page_roles WHERE page_id=? ORDER BY id DESC',(page_id,)).fetchall()
    return {'roles':[dict(r) for r in rows]}

@app.post('/api/pages/{page_id}/roles',status_code=201)
def add_organization_role(page_id:int,payload:OrganizationRolePayload,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        page=conn.execute('SELECT * FROM organization_pages WHERE id=?',(page_id,)).fetchone()
        if not page: raise HTTPException(404,'Page not found')
        if page['owner_user_id']!=viewer['id']: raise HTTPException(403,'Only the Page owner can manage role entries')
        cur=conn.execute('''INSERT INTO organization_page_roles(page_id,created_by_user_id,role,person,designation,contact,about,created_at) VALUES(?,?,?,?,?,?,?,?)''',
        (page_id,viewer['id'],payload.role.strip(),payload.person.strip(),payload.designation.strip(),payload.contact.strip(),payload.about.strip(),datetime.now(timezone.utc).isoformat()))
        row=conn.execute('SELECT * FROM organization_page_roles WHERE id=?',(cur.lastrowid,)).fetchone()
    return dict(row)

@app.delete('/api/pages/{page_id}/roles/{role_id}',status_code=204)
def delete_organization_role(page_id:int,role_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        page=conn.execute('SELECT * FROM organization_pages WHERE id=?',(page_id,)).fetchone()
        if not page: raise HTTPException(404,'Page not found')
        if page['owner_user_id']!=viewer['id']: raise HTTPException(403,'Only the Page owner can manage role entries')
        row=conn.execute('SELECT 1 FROM organization_page_roles WHERE id=? AND page_id=?',(role_id,page_id)).fetchone()
        if not row: raise HTTPException(404,'Role entry not found')
        conn.execute('DELETE FROM organization_page_roles WHERE id=?',(role_id,))

# ========================================
# STEP 20 - PAGE FOLLOWERS, MEMBERS, ADMINS & ROLE-BASED MANAGEMENT
# ========================================
def step20_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS organization_page_followers (
            page_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(page_id,user_id), FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS organization_page_members (
            page_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'Member',
            added_by_user_id INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(page_id,user_id),
            FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(added_by_user_id) REFERENCES users(id) ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS idx_page_followers_user ON organization_page_followers(user_id,page_id);
        CREATE INDEX IF NOT EXISTS idx_page_members_user ON organization_page_members(user_id,page_id);
        """)
step20_init_db()

class PageMemberPayload(BaseModel):
    username: str = Field(min_length=1,max_length=100)
    role: str = Field(default='Member',max_length=30)
class PageMemberRolePayload(BaseModel):
    role: str = Field(min_length=1,max_length=30)

def _page_and_permission(conn,page_id,user_id):
    page=conn.execute('SELECT * FROM organization_pages WHERE id=?',(page_id,)).fetchone()
    if not page: raise HTTPException(404,'Page not found')
    member=conn.execute('SELECT role FROM organization_page_members WHERE page_id=? AND user_id=?',(page_id,user_id)).fetchone()
    role='Owner' if page['owner_user_id']==user_id else (member['role'] if member else '')
    return page,role

@app.post('/api/pages/{page_id}/follow',status_code=201)
def follow_page(page_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        if not conn.execute('SELECT 1 FROM organization_pages WHERE id=?',(page_id,)).fetchone(): raise HTTPException(404,'Page not found')
        conn.execute('INSERT OR IGNORE INTO organization_page_followers(page_id,user_id,created_at) VALUES(?,?,?)',(page_id,viewer['id'],datetime.now(timezone.utc).isoformat()))
    return {'ok':True}

@app.delete('/api/pages/{page_id}/follow',status_code=204)
def unfollow_page(page_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn: conn.execute('DELETE FROM organization_page_followers WHERE page_id=? AND user_id=?',(page_id,viewer['id']))

@app.get('/api/pages/{page_id}/followers')
def page_followers(page_id:int,authorization:Optional[str]=Header(default=None)):
    current_user(authorization)
    with db() as conn:
        if not conn.execute('SELECT 1 FROM organization_pages WHERE id=?',(page_id,)).fetchone(): raise HTTPException(404,'Page not found')
        rows=conn.execute('SELECT u.id,u.username,u.full_name,f.created_at FROM organization_page_followers f JOIN users u ON u.id=f.user_id WHERE f.page_id=? ORDER BY f.created_at DESC LIMIT 500',(page_id,)).fetchall()
    return {'followers':[dict(r) for r in rows]}

@app.get('/api/pages/{page_id}/members')
def page_members(page_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        page,role=_page_and_permission(conn,page_id,viewer['id'])
        rows=conn.execute("SELECT m.user_id,u.username,u.full_name,m.role,m.created_at FROM organization_page_members m JOIN users u ON u.id=m.user_id WHERE m.page_id=? ORDER BY CASE m.role WHEN 'Admin' THEN 1 WHEN 'Editor' THEN 2 ELSE 3 END,u.username",(page_id,)).fetchall()
        owner=conn.execute('SELECT id user_id,username,full_name FROM users WHERE id=?',(page['owner_user_id'],)).fetchone()
    return {'owner':dict(owner) if owner else None,'members':[dict(r) for r in rows],'viewer_role':role,'can_manage':role in ('Owner','Admin')}

@app.post('/api/pages/{page_id}/members',status_code=201)
def add_page_member(page_id:int,payload:PageMemberPayload,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization); allowed={'Admin','Editor','Member'}
    if payload.role not in allowed: raise HTTPException(400,'Role must be Admin, Editor or Member')
    with db() as conn:
        page,role=_page_and_permission(conn,page_id,viewer['id'])
        if role not in ('Owner','Admin'): raise HTTPException(403,'Only the Page owner or Admin can add members')
        user=conn.execute('SELECT id,username,full_name FROM users WHERE lower(username)=lower(?)',(payload.username.strip(),)).fetchone()
        if not user: raise HTTPException(404,'User not found')
        if user['id']==page['owner_user_id']: raise HTTPException(400,'The Page owner already has full access')
        if role=='Admin' and payload.role=='Admin': raise HTTPException(403,'Only the Page owner can appoint another Admin')
        conn.execute("INSERT INTO organization_page_members(page_id,user_id,role,added_by_user_id,created_at) VALUES(?,?,?,?,?) ON CONFLICT(page_id,user_id) DO UPDATE SET role=excluded.role,added_by_user_id=excluded.added_by_user_id",(page_id,user['id'],payload.role,viewer['id'],datetime.now(timezone.utc).isoformat()))
    audit(viewer['id'],'page.member.add',f'page:{page_id}:user:{user["id"]}:{payload.role}')
    return {'user_id':user['id'],'username':user['username'],'full_name':user['full_name'],'role':payload.role}

@app.put('/api/pages/{page_id}/members/{user_id}')
def change_page_member_role(page_id:int,user_id:int,payload:PageMemberRolePayload,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization); allowed={'Admin','Editor','Member'}
    if payload.role not in allowed: raise HTTPException(400,'Role must be Admin, Editor or Member')
    with db() as conn:
        page,role=_page_and_permission(conn,page_id,viewer['id'])
        if role not in ('Owner','Admin'): raise HTTPException(403,'Only the Page owner or Admin can manage members')
        existing=conn.execute('SELECT role FROM organization_page_members WHERE page_id=? AND user_id=?',(page_id,user_id)).fetchone()
        if not existing: raise HTTPException(404,'Page member not found')
        if role=='Admin' and (existing['role']=='Admin' or payload.role=='Admin'): raise HTTPException(403,'Only the Page owner can manage Admin roles')
        conn.execute('UPDATE organization_page_members SET role=? WHERE page_id=? AND user_id=?',(payload.role,page_id,user_id))
    return {'ok':True,'role':payload.role}

@app.delete('/api/pages/{page_id}/members/{user_id}',status_code=204)
def remove_page_member(page_id:int,user_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        page,role=_page_and_permission(conn,page_id,viewer['id'])
        target=conn.execute('SELECT role FROM organization_page_members WHERE page_id=? AND user_id=?',(page_id,user_id)).fetchone()
        if not target: raise HTTPException(404,'Page member not found')
        if viewer['id']!=user_id and role not in ('Owner','Admin'): raise HTTPException(403,'Not allowed')
        if role=='Admin' and viewer['id']!=user_id and target['role']=='Admin': raise HTTPException(403,'Only the Page owner can remove another Admin')
        conn.execute('DELETE FROM organization_page_members WHERE page_id=? AND user_id=?',(page_id,user_id))
    audit(viewer['id'],'page.member.remove',f'page:{page_id}:user:{user_id}')

# ========================================
# STEP 21 - PAGE POSTS, PAGE FEED & POSTING AS AN ORGANIZATION
# ========================================
def step21_init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS page_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            media_file_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE CASCADE,
            FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(media_file_id) REFERENCES uploaded_files(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS page_post_likes (
            post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(post_id,user_id), FOREIGN KEY(post_id) REFERENCES page_posts(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS page_post_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT, post_id INTEGER NOT NULL, author_user_id INTEGER NOT NULL,
            text TEXT NOT NULL, created_at TEXT NOT NULL,
            FOREIGN KEY(post_id) REFERENCES page_posts(id) ON DELETE CASCADE,
            FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_page_posts_page ON page_posts(page_id,id DESC);
        CREATE INDEX IF NOT EXISTS idx_page_posts_created ON page_posts(id DESC);
        CREATE INDEX IF NOT EXISTS idx_page_post_comments ON page_post_comments(post_id,id);
        """)
step21_init_db()

class PagePostCreate(BaseModel):
    text: str = Field(min_length=1, max_length=5000)
    media_file_id: Optional[int] = None
class PagePostUpdate(BaseModel):
    text: str = Field(min_length=1, max_length=5000)
class PagePostCommentCreate(BaseModel):
    text: str = Field(min_length=1, max_length=1000)

def _page_post_permission(conn,page_id,user_id):
    page,role=_page_and_permission(conn,page_id,user_id)
    return page,role, role in ('Owner','Admin','Editor')

def serialize_page_post(row,viewer_id):
    with db() as conn:
        page=conn.execute('SELECT id,name,page_type FROM organization_pages WHERE id=?',(row['page_id'],)).fetchone()
        author=conn.execute('SELECT username,full_name FROM users WHERE id=?',(row['author_user_id'],)).fetchone()
        _,role,can_post=_page_post_permission(conn,row['page_id'],viewer_id)
        likes=conn.execute('SELECT COUNT(*) n FROM page_post_likes WHERE post_id=?',(row['id'],)).fetchone()['n']
        liked=bool(conn.execute('SELECT 1 FROM page_post_likes WHERE post_id=? AND user_id=?',(row['id'],viewer_id)).fetchone())
        comments=conn.execute('SELECT COUNT(*) n FROM page_post_comments WHERE post_id=?',(row['id'],)).fetchone()['n']
        return {**dict(row),'page_name':page['name'],'page_type':page['page_type'],'posted_by_username':author['username'] if author else '',
                'like_count':likes,'liked_by_me':liked,'comment_count':comments,'can_manage':can_post}

@app.post('/api/pages/{page_id}/posts/media',status_code=201)
async def upload_page_post_media(page_id:int,upload:UploadFile=File(...),authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        _,_,can_post=_page_post_permission(conn,page_id,viewer['id'])
        if not can_post: raise HTTPException(403,'Only the Page owner, Admin or Editor can post as this Page')
    ctype=(upload.content_type or '').lower()
    if ctype not in SOCIAL_IMAGE_TYPES: raise HTTPException(415,'Page post images must be PNG, JPG or WEBP.')
    data=await upload.read(MAX_UPLOAD_BYTES+1)
    if not data or len(data)>MAX_UPLOAD_BYTES: raise HTTPException(413 if data else 400,'Image must be non-empty and at most 10 MB.')
    stored=secrets.token_hex(24)+ALLOWED_UPLOAD_TYPES[ctype]; (UPLOAD_DIR/stored).write_bytes(data)
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute('INSERT INTO uploaded_files(owner_user_id,category,original_name,stored_name,content_type,size_bytes,created_at) VALUES(?,?,?,?,?,?,?)',
            (viewer['id'],'page_post_image',Path(upload.filename or 'page-post-image').name[:255],stored,ctype,len(data),now))
    return {'id':cur.lastrowid}

@app.get('/api/page-posts')
def page_feed(authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn: rows=conn.execute('SELECT * FROM page_posts ORDER BY id DESC LIMIT 200').fetchall()
    return {'posts':[serialize_page_post(r,viewer['id']) for r in rows]}

@app.get('/api/pages/{page_id}/posts')
def list_page_posts(page_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        if not conn.execute('SELECT 1 FROM organization_pages WHERE id=?',(page_id,)).fetchone(): raise HTTPException(404,'Page not found')
        rows=conn.execute('SELECT * FROM page_posts WHERE page_id=? ORDER BY id DESC LIMIT 200',(page_id,)).fetchall()
        _,role,can_post=_page_post_permission(conn,page_id,viewer['id'])
    return {'posts':[serialize_page_post(r,viewer['id']) for r in rows],'viewer_role':role,'can_post':can_post}

@app.post('/api/pages/{page_id}/posts',status_code=201)
def create_page_post(page_id:int,payload:PagePostCreate,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _,_,can_post=_page_post_permission(conn,page_id,viewer['id'])
        if not can_post: raise HTTPException(403,'Only the Page owner, Admin or Editor can post as this Page')
        if payload.media_file_id:
            f=conn.execute("SELECT 1 FROM uploaded_files WHERE id=? AND owner_user_id=? AND category='page_post_image'",(payload.media_file_id,viewer['id'])).fetchone()
            if not f: raise HTTPException(400,'Invalid Page post image')
        cur=conn.execute('INSERT INTO page_posts(page_id,author_user_id,text,media_file_id,created_at,updated_at) VALUES(?,?,?,?,?,?)',(page_id,viewer['id'],payload.text.strip(),payload.media_file_id,now,now))
        row=conn.execute('SELECT * FROM page_posts WHERE id=?',(cur.lastrowid,)).fetchone()
    audit(viewer['id'],'page.post.create',f'page:{page_id}:post:{row["id"]}'); return serialize_page_post(row,viewer['id'])

@app.put('/api/page-posts/{post_id}')
def update_page_post(post_id:int,payload:PagePostUpdate,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute('SELECT * FROM page_posts WHERE id=?',(post_id,)).fetchone()
        if not row: raise HTTPException(404,'Page post not found')
        _,_,can_post=_page_post_permission(conn,row['page_id'],viewer['id'])
        if not can_post: raise HTTPException(403,'You cannot edit posts for this Page')
        conn.execute('UPDATE page_posts SET text=?,updated_at=? WHERE id=?',(payload.text.strip(),datetime.now(timezone.utc).isoformat(),post_id))
        row=conn.execute('SELECT * FROM page_posts WHERE id=?',(post_id,)).fetchone()
    return serialize_page_post(row,viewer['id'])

@app.delete('/api/page-posts/{post_id}',status_code=204)
def delete_page_post(post_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute('SELECT * FROM page_posts WHERE id=?',(post_id,)).fetchone()
        if not row: raise HTTPException(404,'Page post not found')
        _,_,can_post=_page_post_permission(conn,row['page_id'],viewer['id'])
        if not can_post: raise HTTPException(403,'You cannot delete posts for this Page')
        conn.execute('DELETE FROM page_posts WHERE id=?',(post_id,))

@app.post('/api/page-posts/{post_id}/like',status_code=201)
def like_page_post(post_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute('SELECT * FROM page_posts WHERE id=?',(post_id,)).fetchone()
        if not row: raise HTTPException(404,'Page post not found')
        conn.execute('INSERT OR IGNORE INTO page_post_likes(post_id,user_id,created_at) VALUES(?,?,?)',(post_id,viewer['id'],datetime.now(timezone.utc).isoformat()))
    return {'ok':True}

@app.delete('/api/page-posts/{post_id}/like',status_code=204)
def unlike_page_post(post_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn: conn.execute('DELETE FROM page_post_likes WHERE post_id=? AND user_id=?',(post_id,viewer['id']))

@app.get('/api/page-posts/{post_id}/comments')
def page_post_comments(post_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        if not conn.execute('SELECT 1 FROM page_posts WHERE id=?',(post_id,)).fetchone(): raise HTTPException(404,'Page post not found')
        rows=conn.execute('''SELECT c.*,u.username,u.full_name,p.display_name FROM page_post_comments c JOIN users u ON u.id=c.author_user_id LEFT JOIN profiles p ON p.user_id=u.id WHERE c.post_id=? ORDER BY c.id''',(post_id,)).fetchall()
    return {'comments':[{**dict(r),'author_name':r['display_name'] or r['full_name'],'is_owner':r['author_user_id']==viewer['id']} for r in rows]}

@app.post('/api/page-posts/{post_id}/comments',status_code=201)
def add_page_post_comment(post_id:int,payload:PagePostCommentCreate,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        if not conn.execute('SELECT 1 FROM page_posts WHERE id=?',(post_id,)).fetchone(): raise HTTPException(404,'Page post not found')
        cur=conn.execute('INSERT INTO page_post_comments(post_id,author_user_id,text,created_at) VALUES(?,?,?,?)',(post_id,viewer['id'],payload.text.strip(),datetime.now(timezone.utc).isoformat()))
    return {'id':cur.lastrowid,'ok':True}

@app.delete('/api/page-post-comments/{comment_id}',status_code=204)
def delete_page_post_comment(comment_id:int,authorization:Optional[str]=Header(default=None)):
    viewer,_=current_user(authorization)
    with db() as conn:
        row=conn.execute('SELECT * FROM page_post_comments WHERE id=?',(comment_id,)).fetchone()
        if not row: raise HTTPException(404,'Comment not found')
        post=conn.execute('SELECT * FROM page_posts WHERE id=?',(row['post_id'],)).fetchone(); _,_,can_post=_page_post_permission(conn,post['page_id'],viewer['id'])
        if row['author_user_id']!=viewer['id'] and not can_post: raise HTTPException(403,'You cannot delete this comment')
        conn.execute('DELETE FROM page_post_comments WHERE id=?',(comment_id,))

# -----------------------------------------------------------------------------
# Step 22 - Groups & Communities
# -----------------------------------------------------------------------------

def init_groups_step22():
    with db() as conn:
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS community_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            privacy TEXT NOT NULL DEFAULT 'public',
            created_at TEXT NOT NULL,
            FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS community_group_members (
            group_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'member',
            status TEXT NOT NULL DEFAULT 'active',
            joined_at TEXT NOT NULL,
            PRIMARY KEY(group_id,user_id),
            FOREIGN KEY(group_id) REFERENCES community_groups(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS community_group_join_requests (
            group_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            PRIMARY KEY(group_id,user_id),
            FOREIGN KEY(group_id) REFERENCES community_groups(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS community_group_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES community_groups(id) ON DELETE CASCADE,
            FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS community_group_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(post_id) REFERENCES community_group_posts(id) ON DELETE CASCADE,
            FOREIGN KEY(author_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_group_posts_group ON community_group_posts(group_id,id DESC);
        ''')

@app.on_event('startup')
def startup_step22_groups():
    init_groups_step22()

class CommunityGroupCreate(BaseModel):
    name: str = Field(min_length=2,max_length=100)
    description: str = Field(default='',max_length=1000)
    privacy: str = 'public'
class CommunityGroupPostCreate(BaseModel):
    text: str = Field(min_length=1,max_length=5000)
class CommunityGroupCommentCreate(BaseModel):
    text: str = Field(min_length=1,max_length=1000)
class CommunityGroupRoleUpdate(BaseModel):
    role: str

def _group_role(conn,gid,uid):
    r=conn.execute("SELECT role FROM community_group_members WHERE group_id=? AND user_id=? AND status='active'",(gid,uid)).fetchone()
    return r['role'] if r else None

def _group_access(conn,gid,uid):
    g=conn.execute('SELECT * FROM community_groups WHERE id=?',(gid,)).fetchone()
    if not g: raise HTTPException(404,'Group not found')
    role=_group_role(conn,gid,uid)
    return g,role

def _group_json(conn,g,uid):
    role=_group_role(conn,g['id'],uid)
    req=conn.execute("SELECT status FROM community_group_join_requests WHERE group_id=? AND user_id=?",(g['id'],uid)).fetchone()
    count=conn.execute("SELECT COUNT(*) n FROM community_group_members WHERE group_id=? AND status='active'",(g['id'],)).fetchone()['n']
    return {'id':g['id'],'name':g['name'],'description':g['description'],'privacy':g['privacy'],'owner_user_id':g['owner_user_id'],'created_at':g['created_at'],'member_count':count,'my_role':role,'join_status':req['status'] if req else None,'can_manage':role in ('owner','admin','moderator')}

@app.get('/api/groups')
def list_groups(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        rows=conn.execute('SELECT * FROM community_groups ORDER BY id DESC').fetchall()
        return {'groups':[_group_json(conn,g,u['id']) for g in rows if g['privacy']=='public' or _group_role(conn,g['id'],u['id'])]}

@app.post('/api/groups',status_code=201)
def create_group(payload:CommunityGroupCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); privacy=payload.privacy.lower()
    if privacy not in ('public','private'): raise HTTPException(400,'Privacy must be public or private')
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute('INSERT INTO community_groups(owner_user_id,name,description,privacy,created_at) VALUES(?,?,?,?,?)',(u['id'],payload.name.strip(),payload.description.strip(),privacy,now)); gid=cur.lastrowid
        conn.execute("INSERT INTO community_group_members(group_id,user_id,role,status,joined_at) VALUES(?,?, 'owner','active',?)",(gid,u['id'],now))
        g=conn.execute('SELECT * FROM community_groups WHERE id=?',(gid,)).fetchone(); return _group_json(conn,g,u['id'])

@app.post('/api/groups/{gid}/join')
def join_group(gid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        g,role=_group_access(conn,gid,u['id'])
        if role:return {'status':'active'}
        if g['privacy']=='public':
            conn.execute("INSERT OR REPLACE INTO community_group_members(group_id,user_id,role,status,joined_at) VALUES(?,?,'member','active',?)",(gid,u['id'],now)); return {'status':'active'}
        conn.execute("INSERT OR REPLACE INTO community_group_join_requests(group_id,user_id,status,created_at) VALUES(?,?,'pending',?)",(gid,u['id'],now)); return {'status':'pending'}

@app.delete('/api/groups/{gid}/membership',status_code=204)
def leave_group(gid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        g,role=_group_access(conn,gid,u['id'])
        if role=='owner':raise HTTPException(400,'Group owner cannot leave the group')
        conn.execute('DELETE FROM community_group_members WHERE group_id=? AND user_id=?',(gid,u['id'])); conn.execute('DELETE FROM community_group_join_requests WHERE group_id=? AND user_id=?',(gid,u['id']))

@app.get('/api/groups/{gid}/members')
def group_members(gid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        g,role=_group_access(conn,gid,u['id'])
        if g['privacy']=='private' and not role:raise HTTPException(403,'Private group membership required')
        rows=conn.execute("SELECT m.user_id,m.role,u.username,u.full_name FROM community_group_members m JOIN users u ON u.id=m.user_id WHERE m.group_id=? AND m.status='active' ORDER BY CASE m.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 WHEN 'moderator' THEN 2 ELSE 3 END,u.full_name",(gid,)).fetchall()
        return {'members':[dict(r) for r in rows]}

@app.get('/api/groups/{gid}/join-requests')
def group_join_requests(gid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        _,role=_group_access(conn,gid,u['id'])
        if role not in ('owner','admin','moderator'):raise HTTPException(403,'Moderator permission required')
        rows=conn.execute("SELECT r.user_id,r.status,r.created_at,u.username,u.full_name FROM community_group_join_requests r JOIN users u ON u.id=r.user_id WHERE r.group_id=? AND r.status='pending'",(gid,)).fetchall(); return {'requests':[dict(r) for r in rows]}

@app.post('/api/groups/{gid}/join-requests/{uid}/accept')
def accept_group_request(gid:int,uid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _,role=_group_access(conn,gid,u['id'])
        if role not in ('owner','admin','moderator'):raise HTTPException(403,'Moderator permission required')
        if not conn.execute("SELECT 1 FROM community_group_join_requests WHERE group_id=? AND user_id=? AND status='pending'",(gid,uid)).fetchone():raise HTTPException(404,'Join request not found')
        conn.execute("INSERT OR REPLACE INTO community_group_members(group_id,user_id,role,status,joined_at) VALUES(?,?,'member','active',?)",(gid,uid,now)); conn.execute("UPDATE community_group_join_requests SET status='accepted' WHERE group_id=? AND user_id=?",(gid,uid)); return {'status':'accepted'}

@app.delete('/api/groups/{gid}/join-requests/{uid}',status_code=204)
def decline_group_request(gid:int,uid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        _,role=_group_access(conn,gid,u['id'])
        if role not in ('owner','admin','moderator'):raise HTTPException(403,'Moderator permission required')
        conn.execute('DELETE FROM community_group_join_requests WHERE group_id=? AND user_id=?',(gid,uid))

@app.put('/api/groups/{gid}/members/{uid}/role')
def group_member_role(gid:int,uid:int,payload:CommunityGroupRoleUpdate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); role2=payload.role.lower()
    if role2 not in ('admin','moderator','member'):raise HTTPException(400,'Role must be admin, moderator or member')
    with db() as conn:
        _,role=_group_access(conn,gid,u['id'])
        if role not in ('owner','admin'):raise HTTPException(403,'Admin permission required')
        target=_group_role(conn,gid,uid)
        if not target:raise HTTPException(404,'Member not found')
        if target=='owner':raise HTTPException(403,'Owner role cannot be changed')
        conn.execute('UPDATE community_group_members SET role=? WHERE group_id=? AND user_id=?',(role2,gid,uid)); return {'role':role2}

@app.get('/api/groups/{gid}/posts')
def group_posts(gid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        g,role=_group_access(conn,gid,u['id'])
        if g['privacy']=='private' and not role:raise HTTPException(403,'Private group membership required')
        rows=conn.execute('''SELECT p.*,u.username,u.full_name,(SELECT COUNT(*) FROM community_group_comments c WHERE c.post_id=p.id) comment_count FROM community_group_posts p JOIN users u ON u.id=p.author_user_id WHERE p.group_id=? ORDER BY p.id DESC LIMIT 200''',(gid,)).fetchall()
        return {'posts':[dict(r)|{'can_manage':r['author_user_id']==u['id'] or role in ('owner','admin','moderator')} for r in rows]}

@app.post('/api/groups/{gid}/posts',status_code=201)
def create_group_post(gid:int,payload:CommunityGroupPostCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        _,role=_group_access(conn,gid,u['id'])
        if not role:raise HTTPException(403,'Join the group before posting')
        cur=conn.execute('INSERT INTO community_group_posts(group_id,author_user_id,text,created_at,updated_at) VALUES(?,?,?,?,?)',(gid,u['id'],payload.text.strip(),now,now)); return {'id':cur.lastrowid}

@app.delete('/api/group-posts/{pid}',status_code=204)
def delete_group_post(pid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        p=conn.execute('SELECT * FROM community_group_posts WHERE id=?',(pid,)).fetchone()
        if not p:raise HTTPException(404,'Group post not found')
        role=_group_role(conn,p['group_id'],u['id'])
        if p['author_user_id']!=u['id'] and role not in ('owner','admin','moderator'):raise HTTPException(403,'Not allowed')
        conn.execute('DELETE FROM community_group_posts WHERE id=?',(pid,))

@app.get('/api/group-posts/{pid}/comments')
def group_comments(pid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        p=conn.execute('SELECT * FROM community_group_posts WHERE id=?',(pid,)).fetchone()
        if not p:raise HTTPException(404,'Group post not found')
        g,role=_group_access(conn,p['group_id'],u['id'])
        if g['privacy']=='private' and not role:raise HTTPException(403,'Private group membership required')
        rows=conn.execute('SELECT c.*,u.full_name,u.username FROM community_group_comments c JOIN users u ON u.id=c.author_user_id WHERE c.post_id=? ORDER BY c.id',(pid,)).fetchall(); return {'comments':[dict(r) for r in rows]}

@app.post('/api/group-posts/{pid}/comments',status_code=201)
def add_group_comment(pid:int,payload:CommunityGroupCommentCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        p=conn.execute('SELECT * FROM community_group_posts WHERE id=?',(pid,)).fetchone()
        if not p:raise HTTPException(404,'Group post not found')
        if not _group_role(conn,p['group_id'],u['id']):raise HTTPException(403,'Join the group before commenting')
        cur=conn.execute('INSERT INTO community_group_comments(post_id,author_user_id,text,created_at) VALUES(?,?,?,?)',(pid,u['id'],payload.text.strip(),now)); return {'id':cur.lastrowid}

# Step 23: Events, Meetings & Calendar
class EventCreate(BaseModel):
    title: str = Field(min_length=1, max_length=160)
    description: str = ''
    start_at: str
    end_at: str = ''
    location: str = ''
    meeting_url: str = ''
    visibility: str = 'public'
    host_type: str = 'user'
    host_id: Optional[int] = None

class EventInvite(BaseModel):
    username: str

class EventRSVP(BaseModel):
    status: str

def init_events_step23():
    with db() as conn:
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, creator_user_id INTEGER NOT NULL,
          host_type TEXT NOT NULL DEFAULT 'user', host_id INTEGER,
          title TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', start_at TEXT NOT NULL,
          end_at TEXT NOT NULL DEFAULT '', location TEXT NOT NULL DEFAULT '', meeting_url TEXT NOT NULL DEFAULT '',
          visibility TEXT NOT NULL DEFAULT 'public', created_at TEXT NOT NULL,
          FOREIGN KEY(creator_user_id) REFERENCES users(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS event_attendees (
          event_id INTEGER NOT NULL, user_id INTEGER NOT NULL, rsvp TEXT NOT NULL DEFAULT 'invited', invited_by INTEGER,
          updated_at TEXT NOT NULL, PRIMARY KEY(event_id,user_id),
          FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE,
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);
        ''')

@app.on_event('startup')
def startup_step23_events():
    init_events_step23()

def _event_json(conn,e,uid):
    mine=e['creator_user_id']==uid
    a=conn.execute('SELECT rsvp FROM event_attendees WHERE event_id=? AND user_id=?',(e['id'],uid)).fetchone()
    counts={r['rsvp']:r['n'] for r in conn.execute('SELECT rsvp,COUNT(*) n FROM event_attendees WHERE event_id=? GROUP BY rsvp',(e['id'],)).fetchall()}
    d=dict(e); d.update({'is_creator':mine,'my_rsvp':a['rsvp'] if a else None,'rsvp_counts':counts}); return d

@app.get('/api/events')
def list_events(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        rows=conn.execute("SELECT * FROM events WHERE visibility='public' OR creator_user_id=? OR id IN (SELECT event_id FROM event_attendees WHERE user_id=?) ORDER BY start_at",(u['id'],u['id'])).fetchall()
        return {'events':[_event_json(conn,e,u['id']) for e in rows]}

@app.post('/api/events',status_code=201)
def create_event(payload:EventCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); vis=payload.visibility if payload.visibility in ('public','private') else 'public'; ht=payload.host_type if payload.host_type in ('user','page','group') else 'user'
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute('INSERT INTO events(creator_user_id,host_type,host_id,title,description,start_at,end_at,location,meeting_url,visibility,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(u['id'],ht,payload.host_id,payload.title.strip(),payload.description.strip(),payload.start_at,payload.end_at,payload.location.strip(),payload.meeting_url.strip(),vis,now))
        eid=cur.lastrowid; conn.execute("INSERT INTO event_attendees(event_id,user_id,rsvp,invited_by,updated_at) VALUES(?,?,?,?,?)",(eid,u['id'],'going',u['id'],now)); return {'id':eid}

@app.post('/api/events/{eid}/invite',status_code=201)
def invite_event(eid:int,payload:EventInvite,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        e=conn.execute('SELECT * FROM events WHERE id=?',(eid,)).fetchone()
        if not e: raise HTTPException(404,'Event not found')
        if e['creator_user_id']!=u['id']: raise HTTPException(403,'Only the event creator can invite people')
        target=conn.execute('SELECT id FROM users WHERE lower(username)=lower(?)',(payload.username.strip(),)).fetchone()
        if not target: raise HTTPException(404,'User not found')
        conn.execute("INSERT INTO event_attendees(event_id,user_id,rsvp,invited_by,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(event_id,user_id) DO UPDATE SET rsvp='invited',invited_by=excluded.invited_by,updated_at=excluded.updated_at",(eid,target['id'],'invited',u['id'],now)); return {'ok':True}

@app.put('/api/events/{eid}/rsvp')
def rsvp_event(eid:int,payload:EventRSVP,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); st=payload.status.lower()
    if st not in ('going','maybe','declined'): raise HTTPException(400,'RSVP must be going, maybe, or declined')
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        if not conn.execute('SELECT 1 FROM events WHERE id=?',(eid,)).fetchone(): raise HTTPException(404,'Event not found')
        conn.execute('INSERT INTO event_attendees(event_id,user_id,rsvp,updated_at) VALUES(?,?,?,?) ON CONFLICT(event_id,user_id) DO UPDATE SET rsvp=excluded.rsvp,updated_at=excluded.updated_at',(eid,u['id'],st,now)); return {'ok':True,'rsvp':st}

@app.delete('/api/events/{eid}',status_code=204)
def delete_event(eid:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        e=conn.execute('SELECT * FROM events WHERE id=?',(eid,)).fetchone()
        if not e: raise HTTPException(404,'Event not found')
        if e['creator_user_id']!=u['id']: raise HTTPException(403,'Only the event creator can delete it')
        conn.execute('DELETE FROM events WHERE id=?',(eid,))

# ========================================
# STEP 24 - UNIFIED SEARCH & DISCOVERY
# ========================================
@app.get('/api/search')
def unified_search(q:str='', category:str='all', limit:int=20, authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    term=q.strip()
    if not term:
        return {'query':'','people':[],'pages':[],'groups':[],'posts':[],'events':[]}
    like=f'%{term}%'; limit=max(1,min(limit,50)); category=category.lower()
    out={'query':term,'people':[],'pages':[],'groups':[],'posts':[],'events':[]}
    with db() as conn:
        if category in ('all','people'):
            rows=conn.execute('''SELECT u.id,u.username,u.full_name,u.role,COALESCE(p.display_name,u.full_name) display_name,
                COALESCE(p.headline,'') headline,COALESCE(p.photo,'') photo FROM users u LEFT JOIN profiles p ON p.user_id=u.id
                WHERE u.id<>? AND (u.username LIKE ? OR u.full_name LIKE ? OR p.display_name LIKE ? OR p.headline LIKE ?)
                ORDER BY u.full_name LIMIT ?''',(u['id'],like,like,like,like,limit)).fetchall()
            out['people']=[dict(r) for r in rows]
        if category in ('all','pages'):
            rows=conn.execute('''SELECT id,name,page_type,category,location,about FROM organization_pages
                WHERE name LIKE ? OR page_type LIKE ? OR category LIKE ? OR location LIKE ? OR about LIKE ? ORDER BY id DESC LIMIT ?''',(like,like,like,like,like,limit)).fetchall()
            out['pages']=[dict(r) for r in rows]
        if category in ('all','groups'):
            rows=conn.execute('''SELECT id,name,description,privacy FROM community_groups
                WHERE (name LIKE ? OR description LIKE ?) AND (privacy='public' OR owner_user_id=? OR id IN
                (SELECT group_id FROM community_group_members WHERE user_id=? AND status='active')) ORDER BY id DESC LIMIT ?''',(like,like,u['id'],u['id'],limit)).fetchall()
            out['groups']=[dict(r) for r in rows]
        if category in ('all','posts'):
            rows=conn.execute('''SELECT p.id,p.text,p.category,p.created_at,u.username,u.full_name FROM posts p JOIN users u ON u.id=p.author_user_id
                WHERE (p.text LIKE ? OR p.category LIKE ?) AND (upper(p.visibility)='PUBLIC' OR p.author_user_id=?) ORDER BY p.id DESC LIMIT ?''',(like,like,u['id'],limit)).fetchall()
            out['posts']=[dict(r) for r in rows]
        if category in ('all','events'):
            rows=conn.execute('''SELECT id,title,description,start_at,location,visibility FROM events WHERE
                (title LIKE ? OR description LIKE ? OR location LIKE ?) AND (visibility='public' OR creator_user_id=? OR id IN
                (SELECT event_id FROM event_attendees WHERE user_id=?)) ORDER BY start_at LIMIT ?''',(like,like,like,u['id'],u['id'],limit)).fetchall()
            out['events']=[dict(r) for r in rows]
    return out

# ========================================
# STEP 25 - JOBS, FREELANCING & PROFESSIONAL OPPORTUNITIES
# ========================================
def init_step25_jobs():
    with db() as conn:
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS jobs (
          id INTEGER PRIMARY KEY AUTOINCREMENT, creator_user_id INTEGER NOT NULL, page_id INTEGER,
          title TEXT NOT NULL, opportunity_type TEXT NOT NULL DEFAULT 'Job', work_mode TEXT NOT NULL DEFAULT 'On-site',
          location TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', skills TEXT NOT NULL DEFAULT '',
          compensation TEXT NOT NULL DEFAULT '', application_deadline TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'open',
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          FOREIGN KEY(creator_user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY(page_id) REFERENCES organization_pages(id) ON DELETE SET NULL);
        CREATE TABLE IF NOT EXISTS job_applications (
          id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL, applicant_user_id INTEGER NOT NULL,
          cover_note TEXT NOT NULL DEFAULT '', portfolio_url TEXT NOT NULL DEFAULT '', resume_file_id INTEGER,
          status TEXT NOT NULL DEFAULT 'submitted', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          UNIQUE(job_id,applicant_user_id), FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE,
          FOREIGN KEY(applicant_user_id) REFERENCES users(id) ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status,created_at);
        CREATE INDEX IF NOT EXISTS idx_job_apps_job ON job_applications(job_id,status);
        ''')

@app.on_event('startup')
def startup_step25_jobs(): init_step25_jobs()

class JobCreate(BaseModel):
    title: str = Field(min_length=2,max_length=160)
    opportunity_type: str = Field(default='Job',max_length=40)
    work_mode: str = Field(default='On-site',max_length=40)
    location: str = Field(default='',max_length=160)
    description: str = Field(default='',max_length=10000)
    skills: str = Field(default='',max_length=2000)
    compensation: str = Field(default='',max_length=200)
    application_deadline: str = Field(default='',max_length=40)
    page_id: Optional[int] = None
class JobApplicationCreate(BaseModel):
    cover_note: str = Field(default='',max_length=5000)
    portfolio_url: str = Field(default='',max_length=1000)
    resume_file_id: Optional[int] = None
class JobApplicationStatus(BaseModel):
    status: str = Field(min_length=1,max_length=30)

def _job_manage_ok(conn,j,user_id):
    if j['creator_user_id']==user_id: return True
    if j['page_id']:
        _,role=_page_and_permission(conn,j['page_id'],user_id)
        return role in ('Owner','Admin','Editor')
    return False

def _job_json(conn,j,uid):
    d=dict(j); page=conn.execute('SELECT name FROM organization_pages WHERE id=?',(j['page_id'],)).fetchone() if j['page_id'] else None
    creator=conn.execute('SELECT username,full_name FROM users WHERE id=?',(j['creator_user_id'],)).fetchone()
    app=conn.execute('SELECT id,status,created_at FROM job_applications WHERE job_id=? AND applicant_user_id=?',(j['id'],uid)).fetchone()
    d.update({'page_name':page['name'] if page else '', 'creator_name':creator['full_name'] if creator else '', 'creator_username':creator['username'] if creator else '', 'can_manage':_job_manage_ok(conn,j,uid), 'my_application':dict(app) if app else None})
    return d

@app.get('/api/jobs')
def list_jobs(q:str='', opportunity_type:str='', mine:bool=False, authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); where=[]; args=[]
    if mine: where.append('j.creator_user_id=?'); args.append(u['id'])
    else: where.append("j.status='open'")
    if q.strip(): where.append('(j.title LIKE ? OR j.description LIKE ? OR j.skills LIKE ? OR j.location LIKE ?)'); like='%'+q.strip()+'%'; args += [like]*4
    if opportunity_type.strip(): where.append('j.opportunity_type=?'); args.append(opportunity_type.strip())
    sql='SELECT j.* FROM jobs j'+((' WHERE '+' AND '.join(where)) if where else '')+' ORDER BY j.id DESC LIMIT 200'
    with db() as conn: return {'jobs':[_job_json(conn,r,u['id']) for r in conn.execute(sql,args).fetchall()]}

@app.post('/api/jobs',status_code=201)
def create_job(payload:JobCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); allowed={'Job','Freelance','Internship','Contract','Volunteer','Collaboration'}; modes={'On-site','Remote','Hybrid'}
    if payload.opportunity_type not in allowed: raise HTTPException(400,'Invalid opportunity type')
    if payload.work_mode not in modes: raise HTTPException(400,'Invalid work mode')
    now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        if payload.page_id:
            _,role=_page_and_permission(conn,payload.page_id,u['id'])
            if role not in ('Owner','Admin','Editor'): raise HTTPException(403,'You cannot post opportunities for this Page')
        cur=conn.execute('''INSERT INTO jobs(creator_user_id,page_id,title,opportunity_type,work_mode,location,description,skills,compensation,application_deadline,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,'open',?,?)''',(u['id'],payload.page_id,payload.title.strip(),payload.opportunity_type,payload.work_mode,payload.location.strip(),payload.description.strip(),payload.skills.strip(),payload.compensation.strip(),payload.application_deadline.strip(),now,now))
        audit(u['id'],'job:create',f'job:{cur.lastrowid}'); return {'id':cur.lastrowid}

@app.put('/api/jobs/{job_id}')
def update_job(job_id:int,payload:JobCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        j=conn.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        if not j: raise HTTPException(404,'Opportunity not found')
        if not _job_manage_ok(conn,j,u['id']): raise HTTPException(403,'Not allowed')
        conn.execute('UPDATE jobs SET title=?,opportunity_type=?,work_mode=?,location=?,description=?,skills=?,compensation=?,application_deadline=?,updated_at=? WHERE id=?',(payload.title.strip(),payload.opportunity_type,payload.work_mode,payload.location.strip(),payload.description.strip(),payload.skills.strip(),payload.compensation.strip(),payload.application_deadline.strip(),now,job_id)); return {'ok':True}

@app.delete('/api/jobs/{job_id}',status_code=204)
def delete_job(job_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        j=conn.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        if not j: raise HTTPException(404,'Opportunity not found')
        if not _job_manage_ok(conn,j,u['id']): raise HTTPException(403,'Not allowed')
        conn.execute('DELETE FROM jobs WHERE id=?',(job_id,))

@app.post('/api/jobs/{job_id}/apply',status_code=201)
def apply_job(job_id:int,payload:JobApplicationCreate,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        j=conn.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        if not j or j['status']!='open': raise HTTPException(404,'Open opportunity not found')
        if j['creator_user_id']==u['id']: raise HTTPException(400,'You cannot apply to your own opportunity')
        try: conn.execute('INSERT INTO job_applications(job_id,applicant_user_id,cover_note,portfolio_url,resume_file_id,status,created_at,updated_at) VALUES(?,?,?,?,?,\'submitted\',?,?)',(job_id,u['id'],payload.cover_note.strip(),payload.portfolio_url.strip(),payload.resume_file_id,now,now))
        except sqlite3.IntegrityError: raise HTTPException(409,'You already applied')
        return {'ok':True}

@app.get('/api/jobs/{job_id}/applications')
def job_applications(job_id:int,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        j=conn.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        if not j: raise HTTPException(404,'Opportunity not found')
        if not _job_manage_ok(conn,j,u['id']): raise HTTPException(403,'Not allowed')
        rows=conn.execute('''SELECT a.*,u.username,u.full_name,COALESCE(p.headline,'') headline FROM job_applications a JOIN users u ON u.id=a.applicant_user_id LEFT JOIN profiles p ON p.user_id=u.id WHERE a.job_id=? ORDER BY a.id DESC''',(job_id,)).fetchall(); return {'applications':[dict(r) for r in rows]}

@app.put('/api/job-applications/{application_id}/status')
def update_application_status(application_id:int,payload:JobApplicationStatus,authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization); allowed={'submitted','reviewing','shortlisted','accepted','rejected','withdrawn'}
    if payload.status not in allowed: raise HTTPException(400,'Invalid application status')
    with db() as conn:
        a=conn.execute('SELECT a.*,j.creator_user_id,j.page_id FROM job_applications a JOIN jobs j ON j.id=a.job_id WHERE a.id=?',(application_id,)).fetchone()
        if not a: raise HTTPException(404,'Application not found')
        if payload.status=='withdrawn':
            if a['applicant_user_id']!=u['id']: raise HTTPException(403,'Only applicant can withdraw')
        else:
            j=conn.execute('SELECT * FROM jobs WHERE id=?',(a['job_id'],)).fetchone()
            if not _job_manage_ok(conn,j,u['id']): raise HTTPException(403,'Not allowed')
        conn.execute('UPDATE job_applications SET status=?,updated_at=? WHERE id=?',(payload.status,datetime.now(timezone.utc).isoformat(),application_id)); return {'ok':True}

@app.get('/api/job-applications/me')
def my_job_applications(authorization:Optional[str]=Header(default=None)):
    u,_=current_user(authorization)
    with db() as conn:
        rows=conn.execute('''SELECT a.*,j.title,j.opportunity_type,j.work_mode,j.location,op.name page_name FROM job_applications a JOIN jobs j ON j.id=a.job_id LEFT JOIN organization_pages op ON op.id=j.page_id WHERE a.applicant_user_id=? ORDER BY a.id DESC''',(u['id'],)).fetchall(); return {'applications':[dict(r) for r in rows]}

# Step 26: Portfolio, Projects & Professional Profile Backend Integration
class PortfolioProjectCreate(BaseModel):
    title: str = Field(min_length=1, max_length=180)
    category: str = Field(default="Other", max_length=80)
    description: str = Field(default="", max_length=5000)
    skills: str = Field(default="", max_length=1000)
    tools: str = Field(default="", max_length=1000)
    project_url: str = Field(default="", max_length=1000)
    repository_url: str = Field(default="", max_length=1000)
    media_file_id: Optional[int] = None
    visibility: str = "Public"


def init_step26():
    with db() as conn:
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS portfolio_projects (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          title TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'Other',
          description TEXT NOT NULL DEFAULT '', skills TEXT NOT NULL DEFAULT '',
          tools TEXT NOT NULL DEFAULT '', project_url TEXT NOT NULL DEFAULT '',
          repository_url TEXT NOT NULL DEFAULT '', media_file_id INTEGER,
          visibility TEXT NOT NULL DEFAULT 'Public', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY(media_file_id) REFERENCES uploaded_files(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_user ON portfolio_projects(user_id,id DESC);
        ''')

@app.on_event("startup")
def startup_step26():
    init_step26()


def _portfolio_json(conn,row,viewer_id):
    d=dict(row)
    u=conn.execute("SELECT username,full_name FROM users WHERE id=?",(row['user_id'],)).fetchone()
    p=conn.execute("SELECT headline,photo FROM profiles WHERE user_id=?",(row['user_id'],)).fetchone()
    d.update({'username':u['username'] if u else '', 'full_name':u['full_name'] if u else '', 'headline':p['headline'] if p else '', 'profile_photo':p['photo'] if p else '', 'can_manage':row['user_id']==viewer_id})
    return d

@app.get('/api/portfolio/projects')
def portfolio_projects(username: str = '', mine: bool = False, authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization)
    with db() as conn:
        if mine:
            rows=conn.execute('SELECT * FROM portfolio_projects WHERE user_id=? ORDER BY id DESC',(u['id'],)).fetchall()
        elif username:
            owner=conn.execute('SELECT id FROM users WHERE username=?',(username,)).fetchone()
            if not owner: raise HTTPException(404,'User not found')
            if owner['id']==u['id']: rows=conn.execute('SELECT * FROM portfolio_projects WHERE user_id=? ORDER BY id DESC',(owner['id'],)).fetchall()
            else: rows=conn.execute("SELECT * FROM portfolio_projects WHERE user_id=? AND visibility='Public' ORDER BY id DESC",(owner['id'],)).fetchall()
        else:
            rows=conn.execute("SELECT * FROM portfolio_projects WHERE visibility='Public' OR user_id=? ORDER BY id DESC LIMIT 100",(u['id'],)).fetchall()
        return {'projects':[_portfolio_json(conn,r,u['id']) for r in rows]}

@app.post('/api/portfolio/projects',status_code=201)
def create_portfolio_project(payload:PortfolioProjectCreate,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization); now=datetime.now(timezone.utc).isoformat()
    if payload.visibility not in {'Public','Private'}: raise HTTPException(400,'Invalid visibility')
    with db() as conn:
        if payload.media_file_id:
            f=conn.execute('SELECT id FROM uploaded_files WHERE id=? AND owner_user_id=?',(payload.media_file_id,u['id'])).fetchone()
            if not f: raise HTTPException(400,'Invalid media file')
        cur=conn.execute('''INSERT INTO portfolio_projects(user_id,title,category,description,skills,tools,project_url,repository_url,media_file_id,visibility,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',(u['id'],payload.title.strip(),payload.category.strip(),payload.description.strip(),payload.skills.strip(),payload.tools.strip(),payload.project_url.strip(),payload.repository_url.strip(),payload.media_file_id,payload.visibility,now,now))
        return {'id':cur.lastrowid,'ok':True}

@app.put('/api/portfolio/projects/{project_id}')
def update_portfolio_project(project_id:int,payload:PortfolioProjectCreate,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization); now=datetime.now(timezone.utc).isoformat()
    if payload.visibility not in {'Public','Private'}: raise HTTPException(400,'Invalid visibility')
    with db() as conn:
        r=conn.execute('SELECT * FROM portfolio_projects WHERE id=?',(project_id,)).fetchone()
        if not r: raise HTTPException(404,'Project not found')
        if r['user_id']!=u['id']: raise HTTPException(403,'Not allowed')
        conn.execute('''UPDATE portfolio_projects SET title=?,category=?,description=?,skills=?,tools=?,project_url=?,repository_url=?,media_file_id=?,visibility=?,updated_at=? WHERE id=?''',(payload.title.strip(),payload.category.strip(),payload.description.strip(),payload.skills.strip(),payload.tools.strip(),payload.project_url.strip(),payload.repository_url.strip(),payload.media_file_id,payload.visibility,now,project_id))
        return {'ok':True}

@app.delete('/api/portfolio/projects/{project_id}',status_code=204)
def delete_portfolio_project(project_id:int,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization)
    with db() as conn:
        r=conn.execute('SELECT user_id FROM portfolio_projects WHERE id=?',(project_id,)).fetchone()
        if not r: raise HTTPException(404,'Project not found')
        if r['user_id']!=u['id']: raise HTTPException(403,'Not allowed')
        conn.execute('DELETE FROM portfolio_projects WHERE id=?',(project_id,))

# Step 27: Education, Courses, Certifications & Learning
class CourseCreate(BaseModel):
    title: str = Field(min_length=1,max_length=180)
    provider: str = Field(default='',max_length=180)
    instructor: str = Field(default='',max_length=180)
    description: str = Field(default='',max_length=4000)
    skills: str = Field(default='',max_length=1000)
    course_url: str = Field(default='',max_length=1000)
    visibility: str = 'Public'

class EnrollmentProgress(BaseModel):
    progress: int = Field(default=0,ge=0,le=100)
    status: str = 'Enrolled'

class CertificationCreate(BaseModel):
    name: str = Field(min_length=1,max_length=200)
    issuer: str = Field(default='',max_length=200)
    credential_id: str = Field(default='',max_length=200)
    credential_url: str = Field(default='',max_length=1000)
    issue_date: str = Field(default='',max_length=40)
    skills: str = Field(default='',max_length=1000)


def init_step27():
    with db() as conn:
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS learning_courses(id INTEGER PRIMARY KEY AUTOINCREMENT,creator_user_id INTEGER NOT NULL,title TEXT NOT NULL,provider TEXT NOT NULL DEFAULT '',instructor TEXT NOT NULL DEFAULT '',description TEXT NOT NULL DEFAULT '',skills TEXT NOT NULL DEFAULT '',course_url TEXT NOT NULL DEFAULT '',visibility TEXT NOT NULL DEFAULT 'Public',created_at TEXT NOT NULL,FOREIGN KEY(creator_user_id) REFERENCES users(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS course_enrollments(id INTEGER PRIMARY KEY AUTOINCREMENT,course_id INTEGER NOT NULL,user_id INTEGER NOT NULL,progress INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'Enrolled',created_at TEXT NOT NULL,UNIQUE(course_id,user_id),FOREIGN KEY(course_id) REFERENCES learning_courses(id) ON DELETE CASCADE,FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS certifications(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,name TEXT NOT NULL,issuer TEXT NOT NULL DEFAULT '',credential_id TEXT NOT NULL DEFAULT '',credential_url TEXT NOT NULL DEFAULT '',issue_date TEXT NOT NULL DEFAULT '',skills TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);
        ''')

@app.on_event('startup')
def startup_step27(): init_step27()

@app.get('/api/learning/courses')
def list_courses(authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization)
    with db() as conn:
        rows=conn.execute("SELECT c.*,COALESCE(e.progress,0) progress,COALESCE(e.status,'') enrollment_status FROM learning_courses c LEFT JOIN course_enrollments e ON e.course_id=c.id AND e.user_id=? WHERE c.visibility='Public' OR c.creator_user_id=? ORDER BY c.id DESC",(u['id'],u['id'])).fetchall()
        return {'courses':[dict(r) for r in rows]}

@app.post('/api/learning/courses',status_code=201)
def create_course(payload:CourseCreate,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization); now=datetime.now(timezone.utc).isoformat()
    if payload.visibility not in {'Public','Private'}: raise HTTPException(400,'Invalid visibility')
    with db() as conn:
        cur=conn.execute('INSERT INTO learning_courses(creator_user_id,title,provider,instructor,description,skills,course_url,visibility,created_at) VALUES(?,?,?,?,?,?,?,?,?)',(u['id'],payload.title.strip(),payload.provider.strip(),payload.instructor.strip(),payload.description.strip(),payload.skills.strip(),payload.course_url.strip(),payload.visibility,now))
        return {'id':cur.lastrowid,'ok':True}

@app.post('/api/learning/courses/{course_id}/enroll',status_code=201)
def enroll_course(course_id:int,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        if not conn.execute('SELECT id FROM learning_courses WHERE id=?',(course_id,)).fetchone(): raise HTTPException(404,'Course not found')
        conn.execute("INSERT OR IGNORE INTO course_enrollments(course_id,user_id,progress,status,created_at) VALUES(?,?,0,'Enrolled',?)",(course_id,u['id'],now)); return {'ok':True}

@app.put('/api/learning/courses/{course_id}/progress')
def course_progress(course_id:int,payload:EnrollmentProgress,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization)
    with db() as conn:
        cur=conn.execute('UPDATE course_enrollments SET progress=?,status=? WHERE course_id=? AND user_id=?',(payload.progress,payload.status,course_id,u['id']))
        if not cur.rowcount: raise HTTPException(404,'Enrollment not found')
        return {'ok':True}

@app.get('/api/learning/certifications')
def list_certifications(authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization)
    with db() as conn: return {'certifications':[dict(r) for r in conn.execute('SELECT * FROM certifications WHERE user_id=? ORDER BY id DESC',(u['id'],)).fetchall()]}

@app.post('/api/learning/certifications',status_code=201)
def create_certification(payload:CertificationCreate,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization); now=datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur=conn.execute('INSERT INTO certifications(user_id,name,issuer,credential_id,credential_url,issue_date,skills,created_at) VALUES(?,?,?,?,?,?,?,?)',(u['id'],payload.name.strip(),payload.issuer.strip(),payload.credential_id.strip(),payload.credential_url.strip(),payload.issue_date.strip(),payload.skills.strip(),now)); return {'id':cur.lastrowid,'ok':True}

@app.delete('/api/learning/certifications/{cert_id}',status_code=204)
def delete_certification(cert_id:int,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization)
    with db() as conn: conn.execute('DELETE FROM certifications WHERE id=? AND user_id=?',(cert_id,u['id']))


# Step 27 navigation/auth registration schema upgrade
def init_step27_registration_schema():
    with db() as conn:
        cols={r['name'] for r in conn.execute('PRAGMA table_info(users)').fetchall()}
        if 'email' not in cols: conn.execute("ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT ''")
        if 'phone' not in cols: conn.execute("ALTER TABLE users ADD COLUMN phone TEXT NOT NULL DEFAULT ''")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_nonempty ON users(email) WHERE email<>''")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_phone_nonempty ON users(phone) WHERE phone<>''")

@app.on_event('startup')
def startup_step27_registration_schema(): init_step27_registration_schema()

# ========================================
# STEP 28 - EXPLORE & PERSONALIZED DISCOVERY
# ========================================
class ExploreInterests(BaseModel):
    interests: list[str] = []

def init_step28():
    with db() as conn:
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS user_interests(
          user_id INTEGER NOT NULL, interest TEXT NOT NULL, created_at TEXT NOT NULL,
          PRIMARY KEY(user_id,interest), FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_user_interests_interest ON user_interests(interest);
        ''')

@app.on_event('startup')
def startup_step28(): init_step28()

@app.get('/api/explore/interests')
def get_explore_interests(authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization)
    with db() as conn:
        return {'interests':[r['interest'] for r in conn.execute('SELECT interest FROM user_interests WHERE user_id=? ORDER BY interest',(u['id'],)).fetchall()]}

@app.put('/api/explore/interests')
def put_explore_interests(payload:ExploreInterests,authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization); now=datetime.now(timezone.utc).isoformat()
    clean=[]
    for x in payload.interests[:30]:
        x=str(x).strip()[:80]
        if x and x.lower() not in {v.lower() for v in clean}: clean.append(x)
    with db() as conn:
        conn.execute('DELETE FROM user_interests WHERE user_id=?',(u['id'],))
        conn.executemany('INSERT INTO user_interests(user_id,interest,created_at) VALUES(?,?,?)',[(u['id'],x,now) for x in clean])
    return {'ok':True,'interests':clean}

@app.get('/api/explore')
def explore_discovery(authorization:Optional[str]=Header(default=None)):
    u=require_user(authorization)
    with db() as conn:
        interests=[r['interest'] for r in conn.execute('SELECT interest FROM user_interests WHERE user_id=?',(u['id'],)).fetchall()]
        people=[dict(r) for r in conn.execute('''SELECT u.id,u.username,u.full_name,u.role,COALESCE(p.display_name,u.full_name) display_name,COALESCE(p.headline,'') headline,COALESCE(p.photo,'') photo,
          (SELECT COUNT(*) FROM follows f WHERE f.followed_user_id=u.id) follower_count
          FROM users u LEFT JOIN profiles p ON p.user_id=u.id WHERE u.id<>? AND u.id NOT IN (SELECT followed_user_id FROM follows WHERE follower_user_id=?) ORDER BY follower_count DESC,u.id DESC LIMIT 12''',(u['id'],u['id'])).fetchall()]
        pages=[dict(r) for r in conn.execute('''SELECT p.id,p.name,p.page_type,p.category,p.location,p.about,(SELECT COUNT(*) FROM organization_page_followers f WHERE f.page_id=p.id) follower_count
          FROM organization_pages p WHERE p.id NOT IN (SELECT page_id FROM organization_page_followers WHERE user_id=?) ORDER BY follower_count DESC,p.id DESC LIMIT 12''',(u['id'],)).fetchall()]
        groups=[dict(r) for r in conn.execute('''SELECT g.id,g.name,g.description,g.privacy,(SELECT COUNT(*) FROM community_group_members m WHERE m.group_id=g.id AND m.status='active') member_count
          FROM community_groups g WHERE g.privacy='public' OR g.owner_user_id=? OR g.id IN (SELECT group_id FROM community_group_members WHERE user_id=? AND status='active') ORDER BY member_count DESC,g.id DESC LIMIT 12''',(u['id'],u['id'])).fetchall()]
        posts=[dict(r) for r in conn.execute('''SELECT p.id,p.text,p.category,p.created_at,u.username,u.full_name,(SELECT COUNT(*) FROM post_likes l WHERE l.post_id=p.id) like_count
          FROM posts p JOIN users u ON u.id=p.author_user_id WHERE upper(p.visibility)='PUBLIC' OR p.author_user_id=? ORDER BY like_count DESC,p.id DESC LIMIT 12''',(u['id'],)).fetchall()]
        events=[dict(r) for r in conn.execute("SELECT id,title,description,start_at,location,visibility FROM events WHERE visibility='public' OR creator_user_id=? OR id IN (SELECT event_id FROM event_attendees WHERE user_id=?) ORDER BY start_at LIMIT 12",(u['id'],u['id'])).fetchall()]
        jobs=[dict(r) for r in conn.execute("SELECT id,title,opportunity_type,work_mode,location,skills,created_at FROM jobs WHERE status='open' ORDER BY id DESC LIMIT 12").fetchall()]
        courses=[dict(r) for r in conn.execute("SELECT id,title,provider,instructor,skills,created_at FROM learning_courses WHERE visibility='Public' OR creator_user_id=? ORDER BY id DESC LIMIT 12",(u['id'],)).fetchall()]
        projects=[dict(r) for r in conn.execute('''SELECT pp.id,pp.title,pp.category,pp.description,pp.skills,pp.tools,u.username,u.full_name FROM portfolio_projects pp JOIN users u ON u.id=pp.user_id WHERE pp.visibility='Public' OR pp.user_id=? ORDER BY pp.id DESC LIMIT 12''',(u['id'],)).fetchall()]
        return {'interests':interests,'people':people,'pages':pages,'groups':groups,'posts':posts,'events':events,'jobs':jobs,'courses':courses,'projects':projects}

# ========================================
# COMBINED FRONTEND + BACKEND
# ========================================
# IMPORTANT: this mount is intentionally last. All /api/* routes above retain
# precedence, while /, HTML, CSS, JS, icons, manifest files, etc. are served
# from frontend-web on the same origin.

@app.websocket("/ws/communication")
async def communication_websocket(ws:WebSocket):
    token=ws.query_params.get("token","").strip()
    with db() as conn:
        sess=conn.execute("SELECT user_id FROM sessions WHERE token=?",(token,)).fetchone() if token else None
    if not sess:
        await ws.close(code=4401); return
    uid=int(sess["user_id"]); await realtime_hub.connect(uid,ws)
    try:
        await ws.send_json({"type":"ready","user_id":uid})
        while True:
            data=await ws.receive_json(); typ=str(data.get("type","")); peer_id=int(data.get("peer_id") or 0)
            if typ in {"typing_start","typing_stop"} and peer_id:
                with db() as conn:
                    if are_connected(conn,uid,peer_id) and _communication_settings_row(conn,uid)["typing_indicators"]:
                        await realtime_hub.send(peer_id,{"type":typ,"user_id":uid})
            elif typ=="presence_query" and peer_id:
                with db() as conn:
                    visible=bool(_communication_settings_row(conn,peer_id)["online_status"])
                await ws.send_json({"type":"presence","user_id":peer_id,"online":bool(visible and realtime_hub.online(peer_id))})
    except WebSocketDisconnect: pass
    except Exception: pass
    finally: realtime_hub.disconnect(uid,ws)


@app.middleware("http")
async def noveam_production_guard(request:Request,call_next):
    # Optional HTTPS enforcement for production deployments. Keep disabled for localhost development.
    if NOVEAM_REQUIRE_HTTPS:
        forwarded=request.headers.get("x-forwarded-proto","").lower()
        secure=request.url.scheme=="https" or (NOVEAM_TRUST_PROXY_HTTPS and forwarded=="https")
        if not secure and request.url.hostname not in {"127.0.0.1","localhost"}:
            from fastapi.responses import JSONResponse
            return JSONResponse({"detail":"HTTPS required"},status_code=426)

    # Simple per-process abuse protection. Redis-backed distributed limiting is recommended for multi-worker production.
    now=time.time(); key=_client_key(request); bucket=_rate_buckets[key]
    while bucket and bucket[0] < now-60: bucket.popleft()
    if len(bucket)>=NOVEAM_RATE_LIMIT_PER_MINUTE:
        from fastapi.responses import JSONResponse
        return JSONResponse({"detail":"Too many requests"},status_code=429,headers={"Retry-After":"60"})
    bucket.append(now)

    cl=request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl)>NOVEAM_MAX_REQUEST_BYTES:
        from fastapi.responses import JSONResponse
        return JSONResponse({"detail":"Request too large"},status_code=413)

    response=await call_next(request)
    response.headers["X-Content-Type-Options"]="nosniff"
    response.headers["X-Frame-Options"]="DENY"
    response.headers["Referrer-Policy"]="strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"]="camera=(self), microphone=(self), geolocation=(self)"
    response.headers["Cross-Origin-Opener-Policy"]="same-origin"
    if NOVEAM_ENV=="production":
        response.headers["Cache-Control"]="no-store" if request.url.path.startswith("/api/") else response.headers.get("Cache-Control","")
        if NOVEAM_REQUIRE_HTTPS:
            response.headers["Strict-Transport-Security"]="max-age=31536000; includeSubDomains"
    return response

@app.get("/api/system/health")
def system_health():
    livekit_configured=bool(LIVEKIT_URL and LIVEKIT_API_KEY and LIVEKIT_API_SECRET)
    return {
        "status":"ok",
        "environment":NOVEAM_ENV,
        "database":"sqlite",
        "livekit_configured":livekit_configured,
        "https_required":NOVEAM_REQUIRE_HTTPS,
        "realtime":"in_process_websocket"
    }

if FRONTEND_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    @app.get("/")
    def frontend_missing():
        raise HTTPException(
            status_code=500,
            detail=(
                "Noveagamheam frontend-web folder was not found. "
                f"Expected: {FRONTEND_DIR}. "
                "Set NOVEAGAMHEAM_FRONTEND_DIR if your frontend is elsewhere."
            ),
        )

