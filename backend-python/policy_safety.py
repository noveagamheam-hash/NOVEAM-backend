"""Versioned Rules and human-reviewed safety workflows."""
import hashlib
import json
import logging
import os
import re
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Query
import psycopg
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field, StrictBool, field_validator

VERSION = "1.6"
ENABLED = os.getenv("NOVEAM_POLICY_ENABLED") == "1"
STAGING = os.getenv("NOVEAM_ENVIRONMENT") == "staging"
OBSERVE = ENABLED and STAGING and os.getenv("NRS_A01_OBSERVE") == "1"
ENFORCE = ENABLED and STAGING and os.getenv("NOVEAM_SAFETY_ENFORCE") == "1"
WARNING_THRESHOLD = 3


class TextModel(BaseModel):
    @field_validator("reason", "rationale", check_fields=False, mode="before")
    @classmethod
    def trim(cls, value):
        return value.strip() if isinstance(value, str) else value


class Acceptance(BaseModel):
    version: str = Field(max_length=20)
    digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    accepted: StrictBool


class Report(TextModel):
    subject_id: int = Field(gt=0)
    section: int | None = Field(default=None, ge=1, le=60)
    reason: str = Field(min_length=1, max_length=2000)


class Decision(TextModel):
    case_id: int = Field(gt=0)
    kind: Literal["dismiss", "reminder", "warning", "restriction"]
    rationale: str = Field(min_length=1, max_length=2000)
    emergency: StrictBool = False
    hours: int | None = Field(default=None, ge=1, le=168)


class Appeal(TextModel):
    action_id: int = Field(gt=0)
    reason: str = Field(min_length=1, max_length=2000)


class Resolution(TextModel):
    outcome: Literal["upheld", "reversed"]
    rationale: str = Field(min_length=1, max_length=2000)


def read_catalog(path, staging):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    sections = data["sections"]
    if (data["version"] != VERSION or len(sections) != 60
            or {section["id"] for section in sections} != set(range(1, 61))
            or any(not section[key].strip() for section in sections
                   for key in ("title", "chapter", "text"))
            or not data["terms"].strip()):
        raise ValueError("Rules require terms and 60 distinct, nonempty sections")
    if not staging and data.get("status") != "approved":
        raise ValueError("Only approved rules can be published outside staging")
    encoded = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return data, hashlib.sha256(encoded.encode()).hexdigest()


def publish_catalog(conn, path, staging):
    catalog, digest = read_catalog(path, staging)
    conn.execute(
        """INSERT INTO policy_versions(version,digest,catalog) VALUES(%s,%s,%s)
           ON CONFLICT(version) DO NOTHING""",
        (VERSION, digest, Jsonb(catalog)),
    )
    if current_policy(conn)["digest"] != digest:
        raise ValueError("This policy version already has different text. Use a new version.")


def current_policy(conn):
    row = conn.execute(
        "SELECT * FROM policy_versions WHERE version=%s", (VERSION,)
    ).fetchone()
    if not row or (not STAGING and row["catalog"].get("status") != "approved"):
        raise HTTPException(503, "Current Rules catalog is unavailable")
    return row


def policy_status(conn, uid):
    policy = current_policy(conn)
    row = conn.execute(
        """SELECT a.accepted_at, o.completed_at, o.step
           FROM users u LEFT JOIN policy_acceptances a
           ON a.user_id=u.id AND a.version=%s AND a.digest=%s
           LEFT JOIN user_onboarding o ON o.user_id=u.id WHERE u.id=%s""",
        (VERSION, policy["digest"], uid),
    ).fetchone()
    if not row:
        raise HTTPException(401, "Account unavailable")
    return {
        "version": VERSION,
        "digest": policy["digest"],
        "accepted": row["accepted_at"] is not None,
        "accepted_at": row["accepted_at"],
        "onboarding_completed": row["completed_at"] is not None,
        "onboarding_step": row["step"],
        "next": "/rules.html" if row["accepted_at"] is None else
                "/feed.html" if row["completed_at"] is not None else
                "/visitor/onboarding.html",
    }


def check_access(conn, uid):
    state = policy_status(conn, uid)
    if not state["accepted"]:
        raise HTTPException(428, {"code": "RULES_ACCEPTANCE_REQUIRED", **state})
    if ENFORCE and conn.execute(
        """SELECT 1 FROM moderation_actions WHERE subject_id=%s
           AND kind='restriction' AND reversed_at IS NULL AND expires_at > now()
           LIMIT 1""", (uid,),
    ).fetchone():
        raise HTTPException(403, {"code": "SAFETY_RESTRICTION_ACTIVE"})


def audit_event(conn, actor, event, resource=None):
    conn.execute(
        "INSERT INTO safety_audit_events(actor_id,event,resource_id) VALUES(%s,%s,%s)",
        (actor, event, resource),
    )


def open_report(conn, reporter, subject, reason, source, section=None,
                target_type=None, target_id=None, urgent=False):
    current_policy(conn)
    report = conn.execute(
        """INSERT INTO safety_reports(reporter_id,subject_id,reason,source,version,
           section,target_type,target_id,urgent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
           RETURNING id""",
        (reporter, subject, reason, source, VERSION, section, target_type, target_id, urgent),
    ).fetchone()
    conn.execute("INSERT INTO moderation_cases(report_id) VALUES(%s)", (report["id"],))
    audit_event(conn, reporter, "report.opened", report["id"])
    return report


SIGNALS = (
    (13, "possible direct threat", True, re.compile(r"\bi (?:will|am going to) (?:kill|shoot) you\b", re.I)),
    (25, "possible credential solicitation", False,
     re.compile(r"\b(?:send|share) (?:me )?(?:your )?(?:otp|password|seed phrase)\b", re.I)),
)


def observe_public_text(conn, subject, target_type, target_id, text):
    if not OBSERVE:
        return
    for section, label, urgent, pattern in SIGNALS:
        if pattern.search(text[:20000]):
            # Save a content pointer and a signal label, without copying content.
            # A failed observation must not abort the user's post.
            try:
                with conn.transaction():
                    open_report(conn, None, subject, label, "NRS-A01", section,
                                target_type, target_id, urgent)
            except (psycopg.Error, HTTPException):
                logging.getLogger(__name__).warning("NRS-A01 observation could not be saved")
            return


def install_policy_safety(app, db, authenticate):
    router = APIRouter(prefix="/api/v1")

    def identity(authorization):
        return authenticate(authorization)[0]["id"]

    def staff(conn, uid, role):
        if not conn.execute(
            "SELECT 1 FROM safety_staff WHERE user_id=%s AND role=%s", (uid, role)
        ).fetchone():
            raise HTTPException(403, "Safety permission required")

    @router.get("/policies/current")
    @router.get("/policies/rules")
    def catalog():
        with db() as conn:
            row = current_policy(conn)
            return {**row["catalog"], "digest": row["digest"]}

    @router.get("/policies/rules/{section_id}")
    def rule(section_id: int):
        if not 1 <= section_id <= 60:
            raise HTTPException(404, "Rule section unavailable")
        with db() as conn:
            row = current_policy(conn)
            section = next(item for item in row["catalog"]["sections"] if item["id"] == section_id)
            return {**section, "version": VERSION, "digest": row["digest"]}

    @router.get("/policies/status")
    def status(authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        with db() as conn:
            return policy_status(conn, uid)

    @router.post("/policies/accept")
    def accept(payload: Acceptance, authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        if payload.accepted is not True:
            raise HTTPException(422, "Explicit agreement is required")
        with db() as conn:
            policy = current_policy(conn)
            if (payload.version, payload.digest) != (VERSION, policy["digest"]):
                raise HTTPException(409, "Rules changed. Load the current version.")
            inserted = conn.execute(
                """INSERT INTO policy_acceptances(user_id,version,digest) VALUES(%s,%s,%s)
                   ON CONFLICT(user_id,version) DO NOTHING RETURNING user_id""",
                (uid, VERSION, payload.digest),
            ).fetchone()
            if inserted:
                audit_event(conn, uid, "rules.accepted")
            return policy_status(conn, uid)

    @router.post("/safety/reports", status_code=201)
    def report(payload: Report, authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        if uid == payload.subject_id:
            raise HTTPException(422, "Choose another account")
        with db() as conn:
            if not conn.execute("SELECT 1 FROM users WHERE id=%s", (payload.subject_id,)).fetchone():
                raise HTTPException(404, "Account unavailable")
            return open_report(conn, uid, payload.subject_id, payload.reason, "member", payload.section)

    @router.get("/safety/reports/mine")
    def reports(after: int = Query(0, ge=0), authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        with db() as conn:
            return conn.execute(
                """SELECT r.id,r.subject_id,r.section,r.reason,r.created_at,c.status
                   FROM safety_reports r JOIN moderation_cases c ON c.report_id=r.id
                   WHERE r.reporter_id=%s AND r.id>%s ORDER BY r.id LIMIT 100""", (uid, after),
            ).fetchall()

    @router.get("/safety/actions/mine")
    def my_actions(after: int = Query(0, ge=0), authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        with db() as conn:
            return conn.execute(
                """SELECT a.id,a.kind,a.rationale,a.created_at,a.expires_at,a.reversed_at,
                          p.id AS appeal_id,p.status AS appeal_status,p.rationale AS appeal_rationale
                   FROM moderation_actions a LEFT JOIN moderation_appeals p ON p.action_id=a.id
                   WHERE a.subject_id=%s AND a.kind<>'dismiss' AND a.id>%s ORDER BY a.id LIMIT 100""",
                (uid, after),
            ).fetchall()

    @router.get("/admin/moderation/cases")
    def cases(after: int = Query(0, ge=0), authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        with db() as conn:
            staff(conn, uid, "moderator")
            rows = conn.execute(
                """SELECT r.*,c.id AS case_id,c.status FROM moderation_cases c
                   JOIN safety_reports r ON r.id=c.report_id
                   WHERE c.status='open' AND c.id>%s ORDER BY c.id LIMIT 100""", (after,),
            ).fetchall()
            audit_event(conn, uid, "cases.viewed")
            return rows

    @router.post("/admin/moderation/actions", status_code=201)
    def decide(payload: Decision, authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        restriction = payload.kind == "restriction"
        if restriction != (payload.hours is not None) or (payload.emergency and not restriction):
            raise HTTPException(422, "Only restrictions can specify hours or emergency status")
        with db() as conn:
            staff(conn, uid, "moderator")
            case = conn.execute(
                """SELECT c.status,r.subject_id,r.reporter_id FROM moderation_cases c
                   JOIN safety_reports r ON r.id=c.report_id WHERE c.id=%s FOR UPDATE OF c""",
                (payload.case_id,),
            ).fetchone()
            if not case:
                raise HTTPException(404, "Case unavailable")
            if case["status"] != "open":
                raise HTTPException(409, "Case already decided")
            if uid in (case["subject_id"], case["reporter_id"]):
                raise HTTPException(403, "Reviewer has a conflict of interest")
            subject = case["subject_id"]
            if not subject and payload.kind != "dismiss":
                raise HTTPException(409, "Subject account is unavailable")
            if restriction:
                # Serialize review of a subject's warning history with reversals.
                conn.execute("SELECT id FROM users WHERE id=%s FOR UPDATE", (subject,))
                if not payload.emergency:
                    warnings = conn.execute(
                        """SELECT count(*) AS total FROM moderation_actions a
                           WHERE a.subject_id=%s AND a.kind='warning' AND a.reversed_at IS NULL
                           AND a.created_at > now()-interval '90 days'
                           AND NOT EXISTS (SELECT 1 FROM moderation_appeals p
                               WHERE p.action_id=a.id AND p.status='pending')""", (subject,),
                    ).fetchone()["total"]
                    if warnings < WARNING_THRESHOLD:
                        raise HTTPException(409, "Three current, unchallenged warnings are required")
            action = conn.execute(
                """INSERT INTO moderation_actions(case_id,subject_id,reviewer_id,kind,rationale,
                   emergency,expires_at) VALUES(%s,%s,%s,%s,%s,%s,now()+%s::integer * interval '1 hour')
                   RETURNING id,kind,expires_at""",
                (payload.case_id, subject, uid, payload.kind, payload.rationale,
                 payload.emergency, payload.hours),
            ).fetchone()
            conn.execute(
                """UPDATE moderation_cases SET status='decided',reviewer_id=%s,decided_at=now()
                   WHERE id=%s""", (uid, payload.case_id),
            )
            audit_event(conn, uid, "action." + payload.kind, action["id"])
            return action

    @router.post("/safety/appeals", status_code=201)
    def appeal(payload: Appeal, authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        with db() as conn:
            conn.execute("SELECT id FROM users WHERE id=%s FOR UPDATE", (uid,))
            row = conn.execute(
                """INSERT INTO moderation_appeals(action_id,reason)
                   SELECT id,%s FROM moderation_actions WHERE id=%s AND subject_id=%s
                   AND kind<>'dismiss' AND reversed_at IS NULL
                   ON CONFLICT(action_id) DO NOTHING RETURNING id,status""",
                (payload.reason, payload.action_id, uid),
            ).fetchone()
            if not row:
                raise HTTPException(404, "Appealable action unavailable")
            audit_event(conn, uid, "appeal.opened", row["id"])
            return row

    @router.get("/admin/moderation/appeals")
    def appeals(after: int = Query(0, ge=0), authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        with db() as conn:
            staff(conn, uid, "appeal_reviewer")
            rows = conn.execute(
                """SELECT p.*,a.kind,a.rationale AS action_rationale,a.case_id
                   FROM moderation_appeals p JOIN moderation_actions a ON a.id=p.action_id
                   WHERE p.status='pending' AND p.id>%s ORDER BY p.id LIMIT 100""", (after,),
            ).fetchall()
            audit_event(conn, uid, "appeals.viewed")
            return rows

    @router.post("/admin/moderation/appeals/{appeal_id}/resolve")
    def resolve(appeal_id: int, payload: Resolution, authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        with db() as conn:
            staff(conn, uid, "appeal_reviewer")
            appeal = conn.execute(
                """SELECT p.status,a.id AS action_id,a.reviewer_id,a.subject_id,r.reporter_id
                   FROM moderation_appeals p JOIN moderation_actions a ON a.id=p.action_id
                   JOIN moderation_cases c ON c.id=a.case_id JOIN safety_reports r ON r.id=c.report_id
                   WHERE p.id=%s FOR UPDATE OF p""", (appeal_id,),
            ).fetchone()
            if not appeal:
                raise HTTPException(404, "Appeal unavailable")
            if uid in (appeal["reviewer_id"], appeal["subject_id"], appeal["reporter_id"]):
                raise HTTPException(403, "Independent reviewer required")
            if appeal["status"] != "pending":
                raise HTTPException(409, "Appeal already resolved")
            conn.execute("SELECT id FROM users WHERE id=%s FOR UPDATE", (appeal["subject_id"],))
            if payload.outcome == "reversed":
                conn.execute("UPDATE moderation_actions SET reversed_at=now() WHERE id=%s",
                             (appeal["action_id"],))
            conn.execute(
                """UPDATE moderation_appeals SET status=%s,reviewer_id=%s,rationale=%s,
                   decided_at=now() WHERE id=%s""", (payload.outcome, uid, payload.rationale, appeal_id),
            )
            audit_event(conn, uid, "appeal." + payload.outcome, appeal_id)
            return {"id": appeal_id, "status": payload.outcome}

    @router.get("/admin/moderation/audit")
    def audit(after: int = Query(0, ge=0), authorization: str | None = Header(default=None)):
        uid = identity(authorization)
        with db() as conn:
            staff(conn, uid, "auditor")
            rows = conn.execute(
                "SELECT * FROM safety_audit_events WHERE id>%s ORDER BY id LIMIT 100", (after,),
            ).fetchall()
            audit_event(conn, uid, "audit.viewed")
            return rows

    app.include_router(router)
