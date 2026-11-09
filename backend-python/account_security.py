"""Account security storage and Turnstile verification."""
import base64
import hashlib
import json
import logging
import os
import secrets
import time
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from reportlab.graphics import renderSVG
from reportlab.graphics.barcode.qr import QrCodeWidget
from reportlab.graphics.shapes import Drawing
from cryptography.fernet import Fernet, InvalidToken
from fastapi import HTTPException


logger = logging.getLogger("noveam.backend")


def initialize(db):
    with db() as conn:
        conn.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS mfa_verified boolean NOT NULL DEFAULT false")
        conn.execute("ALTER TABLE pending_2fa_logins ADD COLUMN IF NOT EXISTS remember boolean NOT NULL DEFAULT false")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mfa_recovery_codes (
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                code_hash TEXT NOT NULL,
                PRIMARY KEY (user_id, code_hash)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mfa_login_risk (
                bucket TEXT PRIMARY KEY,
                window_id BIGINT NOT NULL,
                attempts BIGINT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS mfa_login_risk_window_idx ON mfa_login_risk(window_id)")


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def cipher():
    key = os.environ.get("NOVEAM_MFA_KEY", "").strip()
    if not key:
        logger.error("operation=mfa_configuration outcome=key_missing")
        raise HTTPException(503, "Authenticator service is temporarily unavailable")
    try:
        return Fernet(key.encode())
    except (ValueError, TypeError):
        logger.error("operation=mfa_configuration outcome=key_invalid")
        raise HTTPException(503, "Authenticator service is temporarily unavailable") from None


def encrypt(secret):
    return "fernet:" + cipher().encrypt(secret.encode()).decode()


def decrypt(secret):
    if not secret.startswith("fernet:"):
        return secret
    try:
        return cipher().decrypt(secret[7:].encode()).decode()
    except InvalidToken:
        logger.error("operation=mfa_decryption outcome=stored_secret_unreadable")
        raise HTTPException(503, "Authenticator service is temporarily unavailable") from None


def qr_code(secret, username):
    label = quote(f"NOVEAM:{username}", safe="")
    query = urlencode({"secret": secret, "issuer": "NOVEAM"})
    size = 250
    drawing = Drawing(size, size)
    drawing.add(QrCodeWidget(f"otpauth://totp/{label}?{query}", barWidth=size, barHeight=size))
    svg = renderSVG.drawToString(drawing).encode()
    return "data:image/svg+xml;base64," + base64.b64encode(svg).decode()


def replace_recovery(conn, user_id):
    codes = [secrets.token_hex(10) for _ in range(10)]
    conn.execute("DELETE FROM mfa_recovery_codes WHERE user_id=%s", (user_id,))
    with conn.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO mfa_recovery_codes(user_id,code_hash) VALUES(%s,%s)",
            [(user_id, digest(code)) for code in codes],
        )
    return codes


def consume_recovery(conn, user_id, code):
    return bool(conn.execute(
        "DELETE FROM mfa_recovery_codes WHERE user_id=%s AND code_hash=%s RETURNING user_id",
        (user_id, digest(code.strip().lower())),
    ).fetchone())


def check_login_risk(db, request, payload):
    site_key = os.environ.get("NOVEAM_TURNSTILE_SITE_KEY")
    secret = os.environ.get("NOVEAM_TURNSTILE_SECRET")
    hostname = os.environ.get("NOVEAM_TURNSTILE_HOSTNAME")
    if not any((site_key, secret, hostname)):
        return
    if not all((site_key, secret, hostname)):
        raise HTTPException(503, "Login verification is temporarily unavailable")
    address = request.client.host if request.client else "unknown"
    identity = (payload.identifier or payload.username).strip().lower()
    counts = count_attempts(db, "ip:" + address, "account:" + identity)
    if counts[0] < 20 and counts[1] < 4 and not payload.captcha_token:
        return
    if payload.captcha_token:
        verification = Request(
            "https://challenges.cloudflare.com/turnstile/v0/siteverify",
            data=urlencode({"secret": secret, "response": payload.captcha_token}).encode(),
        )
        try:
            with urlopen(verification, timeout=5) as response:
                result = json.load(response)
        except (OSError, ValueError):
            raise HTTPException(503, "Login verification is temporarily unavailable") from None
        if result.get("success") is True and result.get("hostname") == hostname and result.get("action") == "login":
            return
    raise HTTPException(403, {"code": "captcha_required", "site_key": site_key})

def count_attempts(db, *buckets):
    window = int(time.time()) // 300
    with db() as conn:
        conn.execute("DELETE FROM mfa_login_risk WHERE window_id < %s", (window - 1,))
        counts = []
        for bucket in buckets:
            row = conn.execute("""
                INSERT INTO mfa_login_risk(bucket,window_id,attempts) VALUES(%s,%s,1)
                ON CONFLICT(bucket) DO UPDATE SET
                    attempts=CASE WHEN mfa_login_risk.window_id < excluded.window_id
                        THEN 1 ELSE mfa_login_risk.attempts+1 END,
                    window_id=GREATEST(mfa_login_risk.window_id,excluded.window_id)
                RETURNING attempts
            """, (digest(bucket), window)).fetchone()
            counts.append(row["attempts"])
    return counts


def limit_factor_attempts(db, user_id):
    if count_attempts(db, f"mfa:{user_id}")[0] > 10:
        raise HTTPException(429, "Too many verification attempts. Try again later.")
