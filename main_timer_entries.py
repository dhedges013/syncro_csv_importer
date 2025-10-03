"""Generate realistic Syncro timer entries for the most recent tickets."""

from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, time, timedelta, tzinfo
import json
import os
from typing import Any, Dict, List, Optional

import pytz
from dateutil import parser

from syncro_configs import SYNCRO_TIMEZONE, TEMP_FILE_PATH, get_logger
from syncro_read import (
    syncro_get_all_techs,
    syncro_get_labor_products,
    syncro_get_recent_tickets,
)
from syncro_write import syncro_create_time_entry

logger = get_logger(__name__)

WORKDAY_START_HOUR = 7
WORKDAY_END_HOUR = 18
WORKDAY_MINUTES = (WORKDAY_END_HOUR - WORKDAY_START_HOUR) * 60
MIN_UTILIZATION = 0.30
MAX_UTILIZATION = 0.80
MIN_ENTRY_MINUTES = 15
MAX_ENTRY_MINUTES = 240


def _coerce_bool(value: Any) -> bool:
    """Best-effort conversion of a value to a boolean."""

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return value != 0

    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}

    return bool(value)


def _normalize_datetime(value: str, tz: tzinfo) -> datetime:
    """Parse a string datetime and convert it to the configured timezone."""
    try:
        dt = parser.parse(value)
    except (TypeError, ValueError) as exc:
        logger.warning(f"Unable to parse datetime '{value}': {exc}. Using current time instead.")
        dt = datetime.now(tz)

    if dt.tzinfo is None:
        dt = tz.localize(dt)
    else:
        dt = dt.astimezone(tz)
    return dt


def _build_note(ticket: Dict) -> str:
    """Create a friendly note for the time entry based on ticket metadata."""
    subject = (
        ticket.get("subject")
        or ticket.get("summary")
        or ticket.get("problem_type")
        or ticket.get("issue_type")
        or "General work"
    )
    number = ticket.get("number") or ticket.get("id")
    return f"Worked on ticket {number}: {subject}"


def _generate_durations(entry_count: int) -> List[int]:
    """Generate durations that satisfy utilization constraints."""
    if entry_count == 0:
        return []

    min_total = max(int(WORKDAY_MINUTES * MIN_UTILIZATION), entry_count * MIN_ENTRY_MINUTES)
    max_total = min(int(WORKDAY_MINUTES * MAX_UTILIZATION), entry_count * MAX_ENTRY_MINUTES)

    if min_total > max_total:
        # If there are many tickets, fall back to filling most of the day.
        min_total = max_total

    target_total = random.randint(min_total, max_total)

    remaining = target_total
    durations: List[int] = []

    for idx in range(entry_count):
        entries_left = entry_count - idx
        if entries_left == 1:
            duration = remaining
        else:
            max_for_entry = remaining - MIN_ENTRY_MINUTES * (entries_left - 1)
            min_for_entry = MIN_ENTRY_MINUTES
            if max_for_entry <= min_for_entry:
                duration = min_for_entry
            else:
                duration = random.randint(min_for_entry, max_for_entry)
        durations.append(duration)
        remaining -= duration

    return durations


def _load_cached_section(section: str) -> List[Any]:
    """Load cached data from the temp file if it exists."""

    if not os.path.exists(TEMP_FILE_PATH):
        return []

    try:
        with open(TEMP_FILE_PATH, "r", encoding="utf-8") as handle:
            cached = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Unable to load cached data from %s: %s", TEMP_FILE_PATH, exc)
        return []

    section_data = cached.get(section, []) if isinstance(cached, dict) else []
    if section_data:
        logger.info("Loaded %s entries from cached %s data.", len(section_data), section)
    return section_data


def _normalize_tech(record: Any) -> Optional[Dict[str, Any]]:
    """Normalize various tech representations into a common dict."""

    if isinstance(record, dict):
        candidate = record.get("user") if isinstance(record.get("user"), dict) else record
        tech_id = (
            candidate.get("id")
            or candidate.get("user_id")
            or record.get("id")
            or record.get("user_id")
        )
        if tech_id is None:
            return None

        name = (
            candidate.get("name")
            or candidate.get("full_name")
            or record.get("name")
            or record.get("full_name")
        )
        disabled = _coerce_bool(candidate.get("disabled", record.get("disabled", False)))
        return {"id": tech_id, "name": name, "disabled": disabled}

    if isinstance(record, (list, tuple)) and record:
        tech_id = record[0]
        if tech_id in {None, ""}:
            return None
        name = record[1] if len(record) > 1 else None
        disabled = _coerce_bool(record[2]) if len(record) > 2 else False
        return {"id": tech_id, "name": name, "disabled": disabled}

    return None


def _normalize_labor_product(record: Any) -> Optional[Dict[str, Any]]:
    """Normalize labor product data into a common dict structure."""

    if isinstance(record, dict):
        product = record.get("product") if isinstance(record.get("product"), dict) else record
        product_id = product.get("id") or record.get("id")
        if product_id is None:
            return None

        name = product.get("name") or record.get("name")
        archived = _coerce_bool(product.get("archived", record.get("archived", False)))
        normalized = dict(product)
        normalized.update({"id": product_id, "name": name, "archived": archived})
        return normalized

    if isinstance(record, (list, tuple)) and record:
        product_id = record[0]
        if product_id in {None, ""}:
            return None
        name = record[1] if len(record) > 1 else None
        archived = _coerce_bool(record[2]) if len(record) > 2 else False
        return {"id": product_id, "name": name, "archived": archived}

    return None


def _generate_time_entries_for_day(date_key, assignments, labor_products, tz):
    """Generate timer entries for a single date keyed by tech."""
    generated_entries = []
    workday_start = tz.localize(datetime.combine(date_key, time(hour=WORKDAY_START_HOUR)))

    for tech_id, tickets in assignments.items():
        if not tickets:
            continue

        durations = _generate_durations(len(tickets))
        total_minutes = sum(durations)
        available_offset = WORKDAY_MINUTES - total_minutes
        start_offset = random.randint(0, available_offset) if available_offset > 0 else 0
        current_offset = start_offset

        sorted_tickets = sorted(tickets, key=lambda item: item["created_at"])

        for duration, assignment in zip(durations, sorted_tickets):
            ticket = assignment["ticket"]
            tech = assignment["tech"]
            labor_product = random.choice(labor_products)

            start_time = workday_start + timedelta(minutes=current_offset)
            end_time = start_time + timedelta(minutes=duration)
            current_offset += duration

            generated_entries.append(
                {
                    "ticket_id": ticket.get("id"),
                    "time_entry": {
                        "user_id": tech.get("id"),
                        "labor_product_id": labor_product.get("id"),
                        "minutes": duration,
                        "started_at": start_time.isoformat(),
                        "ended_at": end_time.isoformat(),
                        "notes": _build_note(ticket),
                    },
                }
            )

    return generated_entries


def build_timer_entries(config) -> List[Dict]:
    tz = pytz.timezone(SYNCRO_TIMEZONE)

    tickets = syncro_get_recent_tickets(config)
    if not tickets:
        logger.warning("No recent tickets retrieved. Aborting timer entry creation.")
        return []

    raw_techs = syncro_get_all_techs(config) or []
    if not raw_techs and hasattr(config, "techs"):
        raw_techs = getattr(config, "techs") or []
    if not raw_techs:
        raw_techs = _load_cached_section("techs")

    techs = [
        tech
        for tech in (_normalize_tech(item) for item in raw_techs)
        if tech and tech.get("id") and not tech.get("disabled", False)
    ]
    if not techs:
        logger.error("No techs available to assign timer entries.")
        return []

    raw_labor_products = syncro_get_labor_products(config) or []
    if not raw_labor_products and hasattr(config, "labor_products"):
        raw_labor_products = getattr(config, "labor_products") or []

    labor_products = [
        product
        for product in (_normalize_labor_product(item) for item in raw_labor_products)
        if product and product.get("id") and not product.get("archived", False)
    ]
    if not labor_products:
        logger.error("No labor products found. Unable to create timer entries.")
        return []

    assignments = defaultdict(lambda: defaultdict(list))

    for ticket in tickets:
        created_at_raw = ticket.get("created_at")
        created_at = _normalize_datetime(created_at_raw, tz) if created_at_raw else datetime.now(tz)
        date_key = created_at.date()
        tech = random.choice(techs)
        assignments[date_key][tech.get("id")].append(
            {
                "ticket": ticket,
                "tech": tech,
                "created_at": created_at,
            }
        )

    generated_entries = []
    for date_key, tech_assignments in assignments.items():
        generated_entries.extend(_generate_time_entries_for_day(date_key, tech_assignments, labor_products, tz))

    return generated_entries


def run_timer_entries(config) -> None:
    """Generate and push Syncro timer entries for the latest tickets."""

    logger.info("Starting timer entry generation workflow.")
    entries = build_timer_entries(config)

    if not entries:
        logger.warning("No timer entries were generated.")
        return

    success_count = 0
    for entry in entries:
        ticket_id = entry["ticket_id"]
        payload = entry["time_entry"]

        if not ticket_id:
            logger.warning("Skipping timer entry with missing ticket ID: %s", payload)
            continue

        if not payload.get("user_id") or not payload.get("labor_product_id"):
            logger.warning("Skipping timer entry missing required associations: %s", payload)
            continue

        response = syncro_create_time_entry(config, ticket_id, payload)
        if response:
            success_count += 1

    logger.info("Completed timer entry generation. %s entries created successfully.", success_count)


__all__ = ["run_timer_entries", "build_timer_entries"]
