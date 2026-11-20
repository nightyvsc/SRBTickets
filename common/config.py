"""
Centralized configuration for all SRB services.

All ports, hostnames and tuning parameters are read from environment
variables with sensible defaults for local development.
"""

from __future__ import annotations

import os


class Config:
    """Configuration singleton — reads all settings from env vars."""

    # ── ZeroMQ Ports ──────────────────────────────────────────────────
    GT_SYNC_PORT: int = int(os.getenv("GT_SYNC_PORT", "5555"))
    GT_ASYNC_PORT: int = int(os.getenv("GT_ASYNC_PORT", "5556"))
    GT_RMC_PORT: int = int(os.getenv("GT_RMC_PORT", "5570"))
    GT_CANCEL_PUSH_PORT: int = int(os.getenv("GT_CANCEL_PUSH_PORT", "5575"))
    PERSIST_PORT: int = int(os.getenv("PERSIST_PORT", "5580"))
    NOTIFY_PORT: int = int(os.getenv("NOTIFY_PORT", "5590"))
    HEALTH_PORT: int = int(os.getenv("HEALTH_PORT", "8100"))

    # ── Service hostnames (K8s DNS) ───────────────────────────────────
    GT_SYNC_HOST: str = os.getenv("GT_SYNC_HOST", "gt-svc.srb.svc.cluster.local")
    GT_ASYNC_HOST: str = os.getenv("GT_ASYNC_HOST", "gt-svc.srb.svc.cluster.local")
    GT_RMC_HOST: str = os.getenv("GT_RMC_HOST", "gt-svc.srb.svc.cluster.local")
    GT_CANCEL_PUSH_HOST: str = os.getenv(
        "GT_CANCEL_PUSH_HOST", "gt-svc.srb.svc.cluster.local"
    )
    PERSIST_HOST: str = os.getenv(
        "PERSIST_HOST", "persistence-svc.srb.svc.cluster.local"
    )
    NOTIFY_HOST: str = os.getenv("NOTIFY_HOST", "cancel-svc.srb.svc.cluster.local")

    # ── CockroachDB ──────────────────────────────────────────────────
    CRDB_HOST: str = os.getenv(
        "CRDB_HOST", "cockroachdb.srb.svc.cluster.local"
    )
    CRDB_PORT: int = int(os.getenv("CRDB_PORT", "26257"))
    CRDB_DATABASE: str = os.getenv("CRDB_DATABASE", "srb")
    CRDB_USER: str = os.getenv("CRDB_USER", "root")
    CRDB_CONNSTR: str = os.getenv(
        "CRDB_CONNSTR",
        f"postgresql://{os.getenv('CRDB_USER', 'root')}@"
        f"{os.getenv('CRDB_HOST', 'cockroachdb.srb.svc.cluster.local')}:"
        f"{os.getenv('CRDB_PORT', '26257')}/"
        f"{os.getenv('CRDB_DATABASE', 'srb')}?sslmode=disable",
    )

    # ── Heartbeat / LRU tuning ────────────────────────────────────────
    HEARTBEAT_INTERVAL_MS: int = int(os.getenv("HEARTBEAT_INTERVAL_MS", "2000"))
    HEARTBEAT_LIVENESS: int = int(os.getenv("HEARTBEAT_LIVENESS", "3"))
    # Number of missed heartbeats before GT considers a worker dead

    # ── Client tuning ────────────────────────────────────────────────
    CLIENT_REQUEST_TIMEOUT_MS: int = int(
        os.getenv("CLIENT_REQUEST_TIMEOUT_MS", "10000")
    )
    CLIENT_RECONNECT_DELAY_S: float = float(
        os.getenv("CLIENT_RECONNECT_DELAY_S", "1.0")
    )

    # ── Component identity ────────────────────────────────────────────
    COMPONENT_NAME: str = os.getenv("COMPONENT_NAME", "unknown")

    # ── Seed ──────────────────────────────────────────────────────────
    SEED_RANDOM_SEED: int = int(os.getenv("SEED_RANDOM_SEED", "42"))
    SEED_MONTH: str = os.getenv("SEED_MONTH", "2026-11")
    SEED_NUM_EVENTS: int = int(os.getenv("SEED_NUM_EVENTS", "50"))
    SEED_DUAL_OCCURRENCE_COUNT: int = int(
        os.getenv("SEED_DUAL_OCCURRENCE_COUNT", "20")
    )
    SEAT_CAPACITY: int = int(os.getenv("SEAT_CAPACITY", "100"))

    @classmethod
    def crdb_connstr(cls) -> str:
        """Build CockroachDB connection string from individual env vars."""
        return (
            f"postgresql://{cls.CRDB_USER}@{cls.CRDB_HOST}:{cls.CRDB_PORT}"
            f"/{cls.CRDB_DATABASE}?sslmode=disable"
        )
