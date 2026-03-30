from __future__ import annotations

import logging
from logging.config import dictConfig
from pathlib import Path

from app.config import Settings


def setup_logging(settings: Settings) -> None:
    handlers: dict[str, dict] = {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        }
    }
    loggers: dict[str, dict] = {}
    startup_warnings: list[str] = []

    if settings.ai_reply_audit_log_enabled:
        audit_log_path = Path(settings.ai_reply_audit_log_path)
        try:
            audit_log_path.parent.mkdir(parents=True, exist_ok=True)
            handlers["ai_reply_audit"] = {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "jsonl",
                "filename": str(audit_log_path),
                "maxBytes": settings.ai_reply_audit_max_bytes,
                "backupCount": settings.ai_reply_audit_backup_count,
                "encoding": "utf-8",
            }
            loggers["app.ai_reply_audit"] = {
                "level": "INFO",
                "handlers": ["ai_reply_audit"],
                "propagate": False,
            }
        except OSError as exc:
            handlers["ai_reply_audit_stdout"] = {
                "class": "logging.StreamHandler",
                "formatter": "jsonl",
            }
            loggers["app.ai_reply_audit"] = {
                "level": "INFO",
                "handlers": ["ai_reply_audit_stdout"],
                "propagate": False,
            }
            startup_warnings.append(
                "AI reply audit file logging is unavailable for "
                f"{audit_log_path}; falling back to stdout ({exc})."
            )

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
                },
                "jsonl": {"format": "%(message)s"},
            },
            "handlers": handlers,
            "loggers": loggers,
            "root": {
                "level": settings.log_level.upper(),
                "handlers": ["default"],
            },
        }
    )
    for warning in startup_warnings:
        logging.getLogger(__name__).warning(warning)
