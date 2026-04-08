from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


PBKDF2_ITERATIONS = 240_000
SESSION_TTL_DAYS = 30


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_tables(engine: Engine | None) -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS report_users (
                    user_id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    full_name TEXT NULL,
                    password_hash TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at_utc TIMESTAMPTZ NOT NULL,
                    updated_at_utc TIMESTAMPTZ NOT NULL,
                    last_login_at_utc TIMESTAMPTZ NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_report_users_email
                ON report_users (email)
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE report_users
                ADD COLUMN IF NOT EXISTS auth_provider TEXT NOT NULL DEFAULT 'password'
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE report_users
                ADD COLUMN IF NOT EXISTS provider_subject TEXT NULL
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS report_user_sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    session_token_hash TEXT NOT NULL UNIQUE,
                    user_agent TEXT NULL,
                    ip_address TEXT NULL,
                    created_at_utc TIMESTAMPTZ NOT NULL,
                    last_seen_at_utc TIMESTAMPTZ NOT NULL,
                    expires_at_utc TIMESTAMPTZ NOT NULL,
                    revoked_at_utc TIMESTAMPTZ NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_report_user_sessions_user
                ON report_user_sessions (user_id, last_seen_at_utc DESC)
                """
            )
        )


def _normalize_email(value: Any) -> str:
    email = str(value or "").strip().lower()
    if not email or "@" not in email:
        raise ValueError("A valid email is required.")
    return email


def _normalize_name(value: Any) -> str | None:
    name = str(value or "").strip()
    return name or None


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _hash_password(password: str, *, salt: str | None = None) -> str:
    normalized = str(password or "")
    if len(normalized) < 8:
        raise ValueError("Password must be at least 8 characters.")
    active_salt = salt or secrets.token_hex(16)
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        normalized.encode("utf-8"),
        active_salt.encode("utf-8"),
        PBKDF2_ITERATIONS,
    )
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${active_salt}${derived.hex()}"


def _verify_password(password: str, encoded_hash: str) -> bool:
    try:
        scheme, iterations_raw, salt, expected = str(encoded_hash or "").split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        iterations = int(iterations_raw)
    except Exception:
        return False
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    ).hex()
    return hmac.compare_digest(derived, expected)


def _row_to_user_payload(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "user_id": row.get("user_id"),
        "email": row.get("email"),
        "full_name": row.get("full_name"),
        "status": row.get("status"),
        "auth_provider": row.get("auth_provider"),
        "created_at_utc": row.get("created_at_utc").isoformat() if row.get("created_at_utc") else None,
        "updated_at_utc": row.get("updated_at_utc").isoformat() if row.get("updated_at_utc") else None,
        "last_login_at_utc": row.get("last_login_at_utc").isoformat() if row.get("last_login_at_utc") else None,
        "session_expires_at_utc": row.get("session_expires_at_utc").isoformat() if row.get("session_expires_at_utc") else None,
    }


def get_user_by_email(db: Session, email: str) -> dict[str, Any] | None:
    normalized_email = _normalize_email(email)
    row = (
        db.execute(
            text(
                """
                SELECT
                    user_id,
                    email,
                    full_name,
                    auth_provider,
                    status,
                    created_at_utc,
                    updated_at_utc,
                    last_login_at_utc
                FROM report_users
                WHERE email = :email
                """
            ),
            {"email": normalized_email},
        )
        .mappings()
        .first()
    )
    return _row_to_user_payload(dict(row)) if row else None


def register_user(db: Session, *, email: str, password: str, full_name: str | None = None) -> dict[str, Any]:
    normalized_email = _normalize_email(email)
    if get_user_by_email(db, normalized_email):
        raise ValueError("An account already exists for this email.")
    now = _utcnow()
    user_id = str(uuid4())
    db.execute(
        text(
            """
            INSERT INTO report_users (
                user_id,
                email,
                full_name,
                password_hash,
                auth_provider,
                provider_subject,
                status,
                created_at_utc,
                updated_at_utc,
                last_login_at_utc
            )
            VALUES (
                :user_id,
                :email,
                :full_name,
                :password_hash,
                'password',
                NULL,
                'active',
                :created_at_utc,
                :updated_at_utc,
                NULL
            )
            """
        ),
        {
            "user_id": user_id,
            "email": normalized_email,
            "full_name": _normalize_name(full_name),
            "password_hash": _hash_password(password),
            "created_at_utc": now,
            "updated_at_utc": now,
        },
    )
    db.commit()
    payload = get_user_by_email(db, normalized_email)
    if not payload:
        raise RuntimeError("Failed to load created user.")
    return payload


def authenticate_user(db: Session, *, email: str, password: str) -> dict[str, Any] | None:
    normalized_email = _normalize_email(email)
    row = (
        db.execute(
            text(
                """
                SELECT
                    user_id,
                    email,
                    full_name,
                    password_hash,
                    auth_provider,
                    provider_subject,
                    status,
                    created_at_utc,
                    updated_at_utc,
                    last_login_at_utc
                FROM report_users
                WHERE email = :email
                """
            ),
            {"email": normalized_email},
        )
        .mappings()
        .first()
    )
    if not row:
        return None
    row_dict = dict(row)
    if str(row_dict.get("status") or "").strip().lower() != "active":
        return None
    if not _verify_password(password, str(row_dict.get("password_hash") or "")):
        return None
    now = _utcnow()
    db.execute(
        text(
            """
            UPDATE report_users
            SET
                last_login_at_utc = :last_login_at_utc,
                updated_at_utc = :updated_at_utc
            WHERE user_id = :user_id
            """
        ),
        {
            "user_id": row_dict["user_id"],
            "last_login_at_utc": now,
            "updated_at_utc": now,
        },
    )
    db.commit()
    row_dict["last_login_at_utc"] = now
    row_dict["updated_at_utc"] = now
    return _row_to_user_payload(row_dict)


def upsert_oauth_user(
    db: Session,
    *,
    email: str,
    full_name: str | None = None,
    auth_provider: str,
    provider_subject: str | None = None,
) -> dict[str, Any]:
    normalized_email = _normalize_email(email)
    provider = str(auth_provider or "").strip().lower()
    if not provider:
        raise ValueError("An authentication provider is required.")
    normalized_name = _normalize_name(full_name)
    normalized_subject = str(provider_subject or "").strip() or None
    now = _utcnow()

    row = (
        db.execute(
            text(
                """
                SELECT
                    user_id,
                    email,
                    full_name,
                    auth_provider,
                    provider_subject,
                    status,
                    created_at_utc,
                    updated_at_utc,
                    last_login_at_utc
                FROM report_users
                WHERE email = :email
                """
            ),
            {"email": normalized_email},
        )
        .mappings()
        .first()
    )

    if row:
        row_dict = dict(row)
        db.execute(
            text(
                """
                UPDATE report_users
                SET
                    full_name = COALESCE(:full_name, full_name),
                    auth_provider = COALESCE(NULLIF(auth_provider, 'password'), :auth_provider),
                    provider_subject = COALESCE(:provider_subject, provider_subject),
                    last_login_at_utc = :last_login_at_utc,
                    updated_at_utc = :updated_at_utc
                WHERE user_id = :user_id
                """
            ),
            {
                "user_id": row_dict["user_id"],
                "full_name": normalized_name,
                "auth_provider": provider,
                "provider_subject": normalized_subject,
                "last_login_at_utc": now,
                "updated_at_utc": now,
            },
        )
        db.commit()
        return get_user_by_email(db, normalized_email) or _row_to_user_payload(row_dict) or {}

    user_id = str(uuid4())
    db.execute(
        text(
            """
            INSERT INTO report_users (
                user_id,
                email,
                full_name,
                password_hash,
                auth_provider,
                provider_subject,
                status,
                created_at_utc,
                updated_at_utc,
                last_login_at_utc
            )
            VALUES (
                :user_id,
                :email,
                :full_name,
                :password_hash,
                :auth_provider,
                :provider_subject,
                'active',
                :created_at_utc,
                :updated_at_utc,
                :last_login_at_utc
            )
            """
        ),
        {
            "user_id": user_id,
            "email": normalized_email,
            "full_name": normalized_name,
            "password_hash": _hash_password(secrets.token_urlsafe(24)),
            "auth_provider": provider,
            "provider_subject": normalized_subject,
            "created_at_utc": now,
            "updated_at_utc": now,
            "last_login_at_utc": now,
        },
    )
    db.commit()
    payload = get_user_by_email(db, normalized_email)
    if not payload:
        raise RuntimeError("Failed to load OAuth user.")
    return payload


def create_session(
    db: Session,
    *,
    user_id: str,
    user_agent: str | None = None,
    ip_address: str | None = None,
    ttl_days: int = SESSION_TTL_DAYS,
) -> tuple[str, dict[str, Any]]:
    session_token = secrets.token_urlsafe(32)
    session_id = str(uuid4())
    now = _utcnow()
    expires_at = now + timedelta(days=max(1, int(ttl_days or SESSION_TTL_DAYS)))
    db.execute(
        text(
            """
            INSERT INTO report_user_sessions (
                session_id,
                user_id,
                session_token_hash,
                user_agent,
                ip_address,
                created_at_utc,
                last_seen_at_utc,
                expires_at_utc,
                revoked_at_utc
            )
            VALUES (
                :session_id,
                :user_id,
                :session_token_hash,
                :user_agent,
                :ip_address,
                :created_at_utc,
                :last_seen_at_utc,
                :expires_at_utc,
                NULL
            )
            """
        ),
        {
            "session_id": session_id,
            "user_id": user_id,
            "session_token_hash": _hash_session_token(session_token),
            "user_agent": _normalize_name(user_agent),
            "ip_address": _normalize_name(ip_address),
            "created_at_utc": now,
            "last_seen_at_utc": now,
            "expires_at_utc": expires_at,
        },
    )
    db.commit()
    return session_token, {
        "session_id": session_id,
        "user_id": user_id,
        "created_at_utc": now.isoformat(),
        "expires_at_utc": expires_at.isoformat(),
    }


def get_session_user(db: Session, session_token: str | None, *, touch: bool = True) -> dict[str, Any] | None:
    token = str(session_token or "").strip()
    if not token:
        return None
    now = _utcnow()
    row = (
        db.execute(
            text(
                """
                SELECT
                    u.user_id,
                    u.email,
                    u.full_name,
                    u.status,
                    u.created_at_utc,
                    u.updated_at_utc,
                    u.last_login_at_utc,
                    s.session_id,
                    s.expires_at_utc AS session_expires_at_utc
                FROM report_user_sessions s
                INNER JOIN report_users u
                    ON u.user_id = s.user_id
                WHERE s.session_token_hash = :session_token_hash
                  AND s.revoked_at_utc IS NULL
                  AND s.expires_at_utc > :now_utc
                LIMIT 1
                """
            ),
            {
                "session_token_hash": _hash_session_token(token),
                "now_utc": now,
            },
        )
        .mappings()
        .first()
    )
    if not row:
        return None
    row_dict = dict(row)
    if touch:
        db.execute(
            text(
                """
                UPDATE report_user_sessions
                SET last_seen_at_utc = :last_seen_at_utc
                WHERE session_id = :session_id
                """
            ),
            {
                "session_id": row_dict["session_id"],
                "last_seen_at_utc": now,
            },
        )
        db.commit()
    return _row_to_user_payload(row_dict)


def revoke_session(db: Session, session_token: str | None) -> bool:
    token = str(session_token or "").strip()
    if not token:
        return False
    now = _utcnow()
    result = db.execute(
        text(
            """
            UPDATE report_user_sessions
            SET revoked_at_utc = :revoked_at_utc
            WHERE session_token_hash = :session_token_hash
              AND revoked_at_utc IS NULL
            """
        ),
        {
            "session_token_hash": _hash_session_token(token),
            "revoked_at_utc": now,
        },
    )
    db.commit()
    return bool(result.rowcount)
