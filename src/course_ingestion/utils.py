"""Helpers for course ingestion: KB content checks and the text sent to the embedding model.

The embedding text is the same as cbp-ai-service's scripts/course_embedding_pipeline.py.
"""
from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup

# ── KB content checks ─────────────────────────────────────────────────────────

LIVE = "Live"
RETIRED = "Retired"
CONTENT_ID_RE = re.compile(r"^do_\w+$")  # KB course id, e.g. do_12345


def _matches(value: Any, target: str) -> bool:
    return target in value if isinstance(value, list) else value == target


def is_course(item: dict[str, Any]) -> bool:
    """Same scope as the batch embedding script: primaryCategory=Course and courseCategory=Course."""
    return _matches(item.get("primaryCategory"), "Course") and _matches(item.get("courseCategory"), "Course")


def is_older(current_version: Any, event_version: str | None) -> bool:
    """True when KB is still returning a version older than the event's (read cache lag).

    versionKey is an epoch-millis string on the platform. Non-numeric values can't be
    ordered, so we don't claim staleness for them.
    """
    if not event_version or current_version in (None, ""):
        return False
    try:
        return int(str(current_version)) < int(event_version)
    except ValueError:
        return False


# ── Embedding text ────────────────────────────────────────────────────────────


def parse_html_to_text(raw_html: str | None) -> str:
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    return re.sub(r"\s+", " ", soup.get_text(separator=" ", strip=True)).strip()


def normalise_level(raw: str) -> str | None:
    """Course level from the API's courseLevel; 'NA' and blanks become NULL."""
    value = (raw or "").strip()
    if not value or value.upper() == "NA":
        return None
    return value


def build_description_text(data: dict[str, Any]) -> str:
    description = (data.get("description") or "").strip()
    return f"title: {(data.get('name') or '').strip()} | text: Description: {description}"


def build_keywords_text(data: dict[str, Any]) -> str:
    keywords = data.get("keywords") or []
    kw_str = ", ".join(keywords) if isinstance(keywords, list) else str(keywords)
    return f"title: {(data.get('name') or '').strip()} | text: Keywords: {kw_str}"


def build_combined_text(data: dict[str, Any]) -> str:
    """name + instructions + source + difficultyLevel + courseLevel + competencies_v6."""
    course_name = (data.get("name") or "").strip()
    instructions = parse_html_to_text(data.get("instructions")) if data.get("instructions") else ""

    formatted_comps = []
    if isinstance(data.get("competencies_v6"), list):
        for i, comp in enumerate(data["competencies_v6"], 1):
            area = comp.get("competencyAreaName", "None")
            theme = comp.get("competencyThemeName", "None")
            sub_theme = comp.get("competencySubThemeName", "None")
            formatted_comps.append(f"[{i}] Type: {area} -> Theme: {theme} -> Sub-Theme: {sub_theme}")
    comp_str = " | ".join(formatted_comps)

    source = (data.get("source") or "").strip()
    difficulty_level = (data.get("difficultyLevel") or "").strip()
    course_level = normalise_level(data.get("courseLevel")) or ""

    text_body = (
        f"Name: {course_name} | Instructions: {instructions} | "
        f"Source: {source} | Difficulty Level: {difficulty_level} | "
        f"Course Level: {course_level} | Competencies: {comp_str}"
    )
    return f"title: {course_name} | text: {text_body}"
