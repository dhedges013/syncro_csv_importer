"""Utility for parsing ad-hoc CSV exports into Syncro-style ticket payloads.

This module reads an input CSV file and produces a dictionary keyed by ticket
number.  Each ticket contains metadata about the ticket alongside a list of
comments that are ordered chronologically.  The first comment is always the
original ticket description, the second comment is the change plan (with a
comment subject of "Change Plan"), and any remaining comments from the CSV are
appended afterwards in their original order.  Comment timestamps are adjusted
so they are strictly increasing to preserve comment ordering when imported
into Syncro.
"""
from __future__ import annotations

import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from dateutil import parser as date_parser


@dataclass
class CommentRow:
    """Represents an ad-hoc comment row associated with a ticket."""

    order_hint: int
    body: str
    created_at: Optional[str]


def clean_text(value: Optional[str]) -> str:
    """Normalise text by stripping whitespace and removing odd characters."""

    if value is None:
        return ""

    cleaned = value.replace("\ufeff", "")
    cleaned = cleaned.encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", cleaned).strip()


def norm(value: Optional[str]) -> str:
    """Case-insensitive normalisation used for matching CSV headers."""

    return re.sub(r"\s+", " ", (value or "").replace("\ufeff", "")).strip().lower()


def pick_key(fieldnames: Iterable[str], target: str) -> Optional[str]:
    """Find the CSV header that best matches ``target``."""

    fieldnames_list = list(fieldnames)
    normalised = [norm(name) for name in fieldnames_list]

    # Exact match first
    if target in normalised:
        index = normalised.index(target)
        return fieldnames_list[index]

    # Fuzzy match: require all words to be present
    target_words = target.split()
    for index, normalised_field in enumerate(normalised):
        if all(word in normalised_field for word in target_words):
            return fieldnames_list[index]

    return None


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse a datetime string if possible."""

    if not value:
        return None

    try:
        return date_parser.parse(value)
    except (ValueError, TypeError):
        return None


def ensure_future(previous: datetime, candidate: Optional[datetime]) -> datetime:
    """Return a timestamp that is strictly greater than ``previous``."""

    if candidate is None or candidate <= previous:
        candidate = previous + timedelta(seconds=1)
    return candidate


def format_timestamp(value: datetime) -> str:
    """Format timestamps using ISO 8601."""

    return value.isoformat()


def csv_to_ticket_dicts(csv_path: Path) -> Dict[str, Dict[str, object]]:
    """Parse the provided CSV and return ticket dictionaries keyed by number."""

    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    groups: Dict[str, Dict[str, object]] = {}
    comments_tmp: Dict[str, List[CommentRow]] = defaultdict(list)

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []

        k_num = pick_key(fieldnames, "number")
        k_title = pick_key(fieldnames, "title")
        k_desc = pick_key(fieldnames, "description")
        k_change = pick_key(fieldnames, "change plan")
        k_created = pick_key(fieldnames, "created at")
        k_assignee = pick_key(fieldnames, "assignee name")
        k_cbody = pick_key(fieldnames, "comment body")
        k_ccreated = pick_key(fieldnames, "comment created")

        for index, row in enumerate(reader, start=1):
            ticket_number = clean_text(row.get(k_num, "")) if k_num else ""
            if not ticket_number:
                continue

            groups.setdefault(
                ticket_number,
                {
                    "ticket number": ticket_number,
                    "ticket subject": clean_text(row.get(k_title, "")) if k_title else "",
                    "ticket description": clean_text(row.get(k_desc, "")) if k_desc else "",
                    "change plan": clean_text(row.get(k_change, "")) if k_change else "",
                    "created at": clean_text(row.get(k_created, "")) if k_created else "",
                    "assignee": clean_text(row.get(k_assignee, "")) if k_assignee else "",
                    "comments": [],
                },
            )

            if k_cbody:
                body = clean_text(row.get(k_cbody, ""))
                created_at = clean_text(row.get(k_ccreated, "")) if k_ccreated else ""
                if body:
                    comments_tmp[ticket_number].append(
                        CommentRow(order_hint=index, body=body, created_at=created_at or None)
                    )

    for ticket_number, ticket in groups.items():
        description = ticket.get("ticket description", "")
        change_plan = ticket.get("change plan", "") or "none"
        created_raw = ticket.get("created at", "")

        base_datetime = parse_datetime(created_raw) or datetime.now()
        current_datetime = base_datetime
        order = 0
        ordered_comments: List[Dict[str, object]] = []

        if description:
            order += 1
            ordered_comments.append(
                {
                    "order": order,
                    "subject": "Description",
                    "body": description,
                    "created_at": format_timestamp(current_datetime),
                }
            )

        # Always include a change plan comment even if it is "none"
        order += 1
        change_plan_datetime = ensure_future(current_datetime, base_datetime + timedelta(seconds=1))
        ordered_comments.append(
            {
                "order": order,
                "subject": "Change Plan",
                "body": change_plan,
                "created_at": format_timestamp(change_plan_datetime),
            }
        )
        current_datetime = change_plan_datetime

        additional_comments = sorted(comments_tmp.get(ticket_number, []), key=lambda row: row.order_hint)
        for row in additional_comments:
            order += 1
            candidate_datetime = parse_datetime(row.created_at) if row.created_at else None
            current_datetime = ensure_future(current_datetime, candidate_datetime)
            ordered_comments.append(
                {
                    "order": order,
                    "subject": f"Comment {order}",
                    "body": row.body,
                    "created_at": format_timestamp(current_datetime),
                }
            )

        ticket["comments"] = ordered_comments

    return groups


def pprint_ticket(tickets: Dict[str, Dict[str, object]], change_number: str) -> None:
    """Pretty-print a single ticket by change number."""

    from pprint import pprint

    if change_number in tickets:
        pprint(tickets[change_number])
    else:
        print(f"Change number {change_number} not found.")


def main(csv_path: str) -> None:
    tickets = csv_to_ticket_dicts(Path(csv_path))
    change_number = input("What change number do you want to pprint? ").strip()
    pprint_ticket(tickets, change_number)


if __name__ == "__main__":
    main("output_adjusted.csv")
