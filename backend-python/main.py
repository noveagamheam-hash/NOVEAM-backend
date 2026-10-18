"""NOVEAM backend: configuration, schemas, services, routes and lifecycle.

Run with: uvicorn main:app --host 127.0.0.1 --port 8000
See CHANGELOG.md and DEPLOYMENT.md before replacing an existing deployment.
"""

from collections import OrderedDict, deque
from contextlib import contextmanager, asynccontextmanager
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
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
import sqlite3
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
