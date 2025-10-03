"""Generate realistic Syncro timer entries for the most recent tickets."""

from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, time, timedelta, tzinfo
from typing import Dict, List

import pytz
from dateutil import parser

from syncro_configs import SYNCRO_TIMEZONE, get_logger
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

    techs = [
        tech
        for tech in syncro_get_all_techs(config)
        if tech.get("id") and not tech.get("disabled", False)
    ]
    if not techs:
        logger.error("No techs available to assign timer entries.")
        return []

    labor_products = [
        product
        for product in syncro_get_labor_products(config)
        if product.get("id") and not product.get("archived", False)
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
