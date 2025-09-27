"""Ad-hoc CSV importer that uses the existing Syncro helpers.

Running this module reads an ad-hoc CSV export, normalises its data, and then
creates the represented tickets inside Syncro using the existing utility
functions that ship with this repository.  The importer relies entirely on the
existing modules for API calls, logging, configuration, and payload shaping –
no new helpers are introduced.

Highlights
----------
* Ticket descriptions become the first Syncro comment with subject
  ``"Description"``.
* Change plans are always added as the second comment with subject
  ``"Change Plan"`` (defaulting to ``"none"`` when the CSV omits a value).
* Additional comments follow in CSV order.  Their timestamps are adjusted so
  each is strictly newer than the previous one to preserve ordering within
  Syncro.
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

from dateutil import parser as date_parser

from cli import (
    check_and_clear_temp_data,
    get_log_level,
    prompt_for_missing_credentials,
)
from syncro_configs import get_logger, setup_logging
from syncro_utils import (
    DEFAULTS,
    clean_syncro_ticket_number,
    get_customer_id_by_name,
    get_syncro_created_date,
    get_syncro_customer_contact,
    get_syncro_issue_type,
    get_syncro_priority,
    get_syncro_tech,
    load_or_fetch_temp_data,
)
from syncro_write import syncro_create_comment, syncro_create_ticket


logger = get_logger(__name__)


@dataclass
class CommentRow:
    """Represents a raw comment row harvested from the CSV."""

    order_hint: int
    body: str
    created_at: Optional[str]
    author: Optional[str]
    subject: Optional[str]


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


def pick_first(fieldnames: Iterable[str], *targets: str) -> Optional[str]:
    """Return the first header key that matches one of ``targets``."""

    for target in targets:
        key = pick_key(fieldnames, target)
        if key:
            return key
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


def build_comment_payload(
    subject: str,
    body: str,
    created_at: datetime,
    tech: Optional[str],
) -> Dict[str, object]:
    """Convert comment information into a Syncro comment payload."""

    created_value = get_syncro_created_date(created_at)
    payload: Dict[str, object] = {
        "subject": subject or "API Import",
        "body": body,
        "hidden": True,
        "do_not_email": True,
    }
    if tech:
        payload["tech"] = tech
    if created_value:
        payload["created_at"] = created_value
    return payload


def _iter_rows(reader: csv.DictReader) -> Iterator[Dict[str, str]]:
    """Yield rows from a CSV ``DictReader`` while stripping BOM artefacts."""

    for row in reader:
        yield {key: clean_text(value) for key, value in row.items()}


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
        if not k_num:
            raise ValueError("CSV must contain a column for ticket numbers")

        k_title = pick_first(fieldnames, "title", "subject")
        k_desc = pick_first(fieldnames, "description", "ticket description")
        k_change = pick_first(fieldnames, "change plan", "change description")
        k_created = pick_first(fieldnames, "created at", "created", "ticket created")
        k_assignee = pick_first(fieldnames, "assignee name", "assignee", "tech")
        k_customer = pick_first(fieldnames, "customer", "customer name", "business name")
        k_contact = pick_first(fieldnames, "contact", "contact name", "ticket contact")
        k_status = pick_first(fieldnames, "status", "ticket status")
        k_issue = pick_first(fieldnames, "issue type", "ticket issue type", "problem type")
        k_priority = pick_first(fieldnames, "priority", "ticket priority")
        k_cbody = pick_first(fieldnames, "comment body", "comment", "email body")
        k_ccreated = pick_first(fieldnames, "comment created", "comment timestamp", "timestamp")
        k_cauthor = pick_first(
            fieldnames,
            "comment author",
            "commented by",
            "comment contact",
            "comment user",
            "user",
        )
        k_csubject = pick_first(fieldnames, "comment subject", "comment title")

        for index, row in enumerate(_iter_rows(reader), start=1):
            ticket_number = row.get(k_num, "") if k_num else ""
            if not ticket_number:
                logger.debug("Skipping row %s without ticket number", index)
                continue

            ticket = groups.setdefault(
                ticket_number,
                {
                    "number": ticket_number,
                    "subject": "",
                    "description": "",
                    "change_plan": "",
                    "created_at": "",
                    "assignee": "",
                    "customer": "",
                    "contact": "",
                    "status": "",
                    "issue_type": "",
                    "priority": "",
                    "comments": [],
                },
            )

            def update_if_present(key: str, value: Optional[str]) -> None:
                if value:
                    ticket[key] = value

            update_if_present("subject", row.get(k_title, "") if k_title else None)
            update_if_present("description", row.get(k_desc, "") if k_desc else None)
            update_if_present("change_plan", row.get(k_change, "") if k_change else None)
            update_if_present("created_at", row.get(k_created, "") if k_created else None)
            update_if_present("assignee", row.get(k_assignee, "") if k_assignee else None)
            update_if_present("customer", row.get(k_customer, "") if k_customer else None)
            update_if_present("contact", row.get(k_contact, "") if k_contact else None)
            update_if_present("status", row.get(k_status, "") if k_status else None)
            update_if_present("issue_type", row.get(k_issue, "") if k_issue else None)
            update_if_present("priority", row.get(k_priority, "") if k_priority else None)

            if k_cbody:
                body = row.get(k_cbody, "")
                created_at = row.get(k_ccreated, "") if k_ccreated else ""
                author = row.get(k_cauthor, "") if k_cauthor else ""
                subject = row.get(k_csubject, "") if k_csubject else ""
                if body:
                    comments_tmp[ticket_number].append(
                        CommentRow(
                            order_hint=index,
                            body=body,
                            created_at=created_at or None,
                            author=author or None,
                            subject=subject or None,
                        )
                    )

    for ticket_number, ticket in groups.items():
        ticket["comments"] = sorted(
            comments_tmp.get(ticket_number, []), key=lambda row: row.order_hint
        )

    return groups


def build_ticket_payload(ticket: Dict[str, object], defaults: Dict[str, Optional[str]]) -> Dict[str, object]:
    """Translate a parsed ticket dictionary into a Syncro ticket payload."""

    number_raw = str(ticket.get("number", ""))
    cleaned_number = clean_syncro_ticket_number(number_raw) or number_raw
    subject = ticket.get("subject") or f"Imported Ticket {cleaned_number}" if cleaned_number else "Imported Ticket"
    assignee = ticket.get("assignee") or defaults.get("assignee")

    created_raw = ticket.get("created_at") or defaults.get("created_at")
    base_datetime = parse_datetime(created_raw) or datetime.now()

    description_text = ticket.get("description") or DEFAULTS.get("ticket description") or "Description not provided"
    change_plan_text = ticket.get("change_plan") or "none"

    description_comment = build_comment_payload("Description", description_text, base_datetime, assignee)

    change_plan_time = ensure_future(base_datetime, base_datetime + timedelta(seconds=1))
    change_plan_comment = build_comment_payload("Change Plan", change_plan_text, change_plan_time, assignee)

    additional_comments: List[Dict[str, object]] = []
    current_datetime = change_plan_time
    for offset, comment in enumerate(ticket.get("comments", []), start=1):
        if not comment.body:
            continue
        candidate = parse_datetime(comment.created_at) if comment.created_at else None
        current_datetime = ensure_future(current_datetime, candidate)
        subject = comment.subject or f"Comment {offset + 2}"
        author = comment.author or assignee or defaults.get("assignee")
        additional_comments.append(
            build_comment_payload(subject, comment.body, current_datetime, author)
        )

    customer_name = ticket.get("customer") or defaults.get("customer")
    contact_name = ticket.get("contact") or defaults.get("contact")
    status = ticket.get("status") or defaults.get("status")
    issue_type = ticket.get("issue_type") or defaults.get("issue_type")
    priority = ticket.get("priority") or defaults.get("priority")

    issue_type_value = get_syncro_issue_type(issue_type)
    priority_value = get_syncro_priority(priority)
    tech_id = get_syncro_tech(assignee) if assignee else None

    payload: Dict[str, object] = {
        "number": cleaned_number,
        "subject": subject,
        "created_at": get_syncro_created_date(base_datetime),
        "comments_attributes": [description_comment],
    }

    if status:
        payload["status"] = status
    if issue_type_value:
        payload["problem_type"] = issue_type_value
    if priority_value:
        payload["priority"] = priority_value
    if tech_id:
        payload["user_id"] = tech_id

    payload["_change_plan_comment"] = change_plan_comment
    payload["_extra_comments"] = additional_comments
    payload["_customer_name"] = customer_name
    payload["_contact_name"] = contact_name

    return payload


def enrich_payload_with_ids(payload: Dict[str, object], config) -> None:
    """Resolve customer/contact IDs for the ticket payload in-place."""

    customer_name = payload.pop("_customer_name", None)
    contact_name = payload.pop("_contact_name", None)

    customer_id = None
    if customer_name:
        customer_id = get_customer_id_by_name(customer_name, config)
        if not customer_id:
            logger.warning("Customer '%s' could not be resolved", customer_name)
    if customer_id:
        payload["customer_id"] = customer_id
        if contact_name:
            contact_id = get_syncro_customer_contact(customer_id, contact_name)
            if contact_id:
                payload["contact_id"] = contact_id
            else:
                logger.warning(
                    "Contact '%s' could not be resolved for customer '%s'",
                    contact_name,
                    customer_name,
                )


def create_ticket_with_comments(config, payload: Dict[str, object]) -> bool:
    """Create a ticket and add its associated comments in order.

    Returns ``True`` when ticket creation succeeds, otherwise ``False``.
    """

    change_plan_comment = payload.pop("_change_plan_comment")
    additional_comments: List[Dict[str, object]] = payload.pop("_extra_comments")

    response = syncro_create_ticket(config, payload)
    if not response:
        logger.error("Ticket creation failed for %s", payload.get("number"))
        return False

    ticket_number = response.get("ticket", {}).get("number") or payload.get("number")
    if not ticket_number:
        logger.error("Unable to determine ticket number after creation")
        return False

    def push_comment(comment_payload: Dict[str, object]) -> None:
        comment_data = dict(comment_payload)
        comment_data["ticket_number"] = ticket_number
        syncro_create_comment(config, comment_data)

    push_comment(change_plan_comment)
    for comment in additional_comments:
        push_comment(comment)

    return True


def load_defaults_from_user() -> Dict[str, Optional[str]]:
    """Prompt the operator for optional default values used during import."""

    defaults: Dict[str, Optional[str]] = {
        "customer": None,
        "contact": None,
        "status": None,
        "issue_type": None,
        "priority": None,
        "assignee": None,
        "created_at": None,
    }

    print("\nOptional defaults (press Enter to skip):")
    for field in defaults:
        value = input(f"Default {field.replace('_', ' ')}: ").strip()
        defaults[field] = value or None

    return defaults


def run(csv_path: Path) -> None:
    """Drive the ad-hoc importer workflow."""

    logger.info("Loading tickets from %s", csv_path)
    tickets = csv_to_ticket_dicts(csv_path)
    logger.info("Discovered %d tickets in the ad-hoc CSV", len(tickets))

    defaults = load_defaults_from_user()

    config = prompt_for_missing_credentials()
    load_or_fetch_temp_data(config)

    created_count = 0
    for ticket_number in sorted(tickets):
        ticket = tickets[ticket_number]
        logger.info("Preparing ticket %s", ticket_number)
        payload = build_ticket_payload(ticket, defaults)
        enrich_payload_with_ids(payload, config)
        if create_ticket_with_comments(config, payload):
            created_count += 1

    logger.info("Created %d tickets from ad-hoc CSV", created_count)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import ad-hoc tickets into Syncro")
    parser.add_argument(
        "csv",
        nargs="?",
        default="output_adjusted.csv",
        help="Path to the ad-hoc CSV file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_level = get_log_level()
    setup_logging(log_level)
    logger.info("Starting ad-hoc ticket import")
    check_and_clear_temp_data()
    try:
        run(Path(args.csv))
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Ad-hoc import failed: %s", exc)


if __name__ == "__main__":
    main()

