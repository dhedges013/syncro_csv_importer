import json
import os
from datetime import timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz

from syncro_utils import (
    clean_syncro_ticket_number,
    load_or_fetch_temp_data,
    parse_charge_flag,
    parse_comment_created,
    syncro_get_all_ticket_labor_entries_from_csv,
    syncro_prepare_ticket_labor_json,
    get_syncro_tech_name_by_id,
)
from syncro_write import (
    syncro_create_ticket_timer_entry,
    syncro_charge_ticket_timer_entry,
)
from syncro_read import (
    get_api_call_count,
    get_syncro_ticket_by_number,
    syncro_get_ticket_timer_entries,
)
from syncro_configs import (
    SYNCRO_API_KEY,
    SYNCRO_SUBDOMAIN,
    SYNCRO_TIMEZONE,
    TEMP_CREDENTIALS_FILE_PATH,
    get_logger,
    setup_logging,
)

logger = get_logger(__name__)


def _interactive_pause(enabled: bool, message: str) -> bool:
    """Pause execution for user confirmation during interactive runs."""

    if not enabled:
        return True

    try:
        input(f"{message} (press Enter to continue, Ctrl+C to abort) ")
        return True
    except KeyboardInterrupt:
        logger.info("Interactive run cancelled by user.")
        return False


def _load_direct_run_config():
    """Create a SyncroConfig using stored credentials or interactive prompts."""

    from syncro_config_object import SyncroConfig  # Local import to avoid circular deps

    if SYNCRO_SUBDOMAIN and SYNCRO_API_KEY:
        logger.info("Using API credentials from syncro_configs.py for interactive run.")
        return SyncroConfig(SYNCRO_SUBDOMAIN, SYNCRO_API_KEY)

    if os.path.exists(TEMP_CREDENTIALS_FILE_PATH):
        try:
            with open(TEMP_CREDENTIALS_FILE_PATH, "r", encoding="utf-8") as handle:
                data = json.load(handle)
                subdomain = data.get("subdomain")
                api_key = data.get("api_key")
                if subdomain and api_key:
                    logger.info(
                        "Loaded API credentials from %s for interactive run.",
                        TEMP_CREDENTIALS_FILE_PATH,
                    )
                    return SyncroConfig(subdomain, api_key)
                logger.warning(
                    "Credentials file %s is missing required fields.",
                    TEMP_CREDENTIALS_FILE_PATH,
                )
        except (OSError, json.JSONDecodeError) as exc:
            logger.error(
                "Unable to read credentials from %s: %s",
                TEMP_CREDENTIALS_FILE_PATH,
                exc,
            )

    logger.info("Prompting for Syncro subdomain and API key for interactive run.")
    subdomain = input("Enter Syncro subdomain: ").strip()
    api_key = input("Enter Syncro API key: ").strip()
    if not subdomain or not api_key:
        logger.error("Subdomain and API key are required.")
        return None

    return SyncroConfig(subdomain, api_key)


def _sort_labor_entries(entries: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Sort entries by ticket number then entry sequence."""

    def sort_key(entry: Dict[str, str]):
        ticket_number_raw = entry.get("ticket number") or ""
        cleaned = clean_syncro_ticket_number(ticket_number_raw) or ticket_number_raw
        sequence_raw = entry.get("entry sequence") or "0"
        try:
            sequence = int(float(sequence_raw))
        except (TypeError, ValueError):
            logger.warning(
                f"Invalid entry sequence '{sequence_raw}' for ticket {ticket_number_raw}; defaulting to 0."
            )
            sequence = 0
        return (cleaned, sequence)

    return sorted(entries, key=sort_key)


def _ensure_ticket(
    config,
    ticket_cache: Dict[str, Optional[Dict[str, Optional[str]]]],
    ticket_number_raw: str,
) -> Optional[Dict[str, Optional[str]]]:
    """Return ticket data from cache or fetch it from Syncro."""

    cleaned = clean_syncro_ticket_number(ticket_number_raw) or ticket_number_raw

    if cleaned in ticket_cache:
        return ticket_cache[cleaned]

    try:
        ticket = get_syncro_ticket_by_number(config, cleaned)
    except Exception as e:
        logger.error(f"Error retrieving ticket '{cleaned}': {e}")
        ticket = None

    if ticket:
        ticket_cache[cleaned] = ticket
        return ticket

    logger.error(f"Ticket '{ticket_number_raw}' (normalized '{cleaned}') was not found; skipping labor entries.")
    ticket_cache[cleaned] = None  # Cache miss to avoid repeated lookups
    return None


def _extract_timer_entry_id(api_response: Optional[Dict[str, Any]]) -> Optional[int]:
    """Best-effort extraction of the timer entry ID from Syncro's response."""

    if not isinstance(api_response, dict):
        return None

    def _coerce_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    direct_id = api_response.get("timer_entry_id") or api_response.get("id")
    coerced_direct = _coerce_int(direct_id)
    if coerced_direct is not None:
        return coerced_direct

    for key in ("timer_entry", "ticket_timer_entry", "timer", "data"):
        nested = api_response.get(key)
        if isinstance(nested, dict):
            nested_id = nested.get("id") or nested.get("timer_entry_id")
            coerced_nested = _coerce_int(nested_id)
            if coerced_nested is not None:
                return coerced_nested

    return None

def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()

def _normalize_text_lower(value: Any) -> str:
    return _normalize_text(value).lower()

def _truncate_for_log(value: str, max_length: int = 80) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 3] + "..."

def _normalize_timestamp(value: Any) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        return ""

    parsed = parse_comment_created(normalized)
    if not parsed:
        logger.debug(
            "Unable to parse timestamp '%s'; using raw value for duplicate detection.",
            normalized,
        )
        return normalized

    try:
        local_timezone = pytz.timezone(SYNCRO_TIMEZONE)
    except Exception as exc:
        logger.error("Invalid SYNCRO_TIMEZONE '%s': %s", SYNCRO_TIMEZONE, exc)
        local_timezone = None

    if parsed.tzinfo is None:
        if local_timezone:
            parsed = local_timezone.localize(parsed)
            logger.debug(
                "Localized naive timestamp '%s' to timezone %s.",
                normalized,
                SYNCRO_TIMEZONE,
            )
        else:
            parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        parsed = parsed.astimezone(timezone.utc)
    except Exception as exc:
        logger.error("Failed to convert timestamp '%s' to UTC: %s", parsed, exc)

    parsed = parsed.replace(second=0, microsecond=0)
    return parsed.isoformat()

def _make_timer_compare_signature(
    notes: Any,
    tech: Any,
    created_at: Any,
) -> Tuple[str, str, str]:
    return (
        _normalize_text(notes),
        _normalize_text_lower(tech),
        _normalize_timestamp(created_at),
    )

def _log_signature(signature: Tuple[str, str, str], prefix: str) -> None:
    logger.info(
        "%s notes='%s', tech='%s', timestamp='%s'",
        prefix,
        _truncate_for_log(signature[0]),
        signature[1],
        signature[2],
    )

def _make_remote_timer_signature(timer: Dict[str, Any]) -> Tuple[str, str, str]:
    notes = (
        timer.get("notes")
        or timer.get("body")
        or timer.get("description")
        or timer.get("comment")
        or timer.get("entry")
    )

    tech_value = (
        timer.get("tech")
        or timer.get("user_name")
        or timer.get("user")
        or timer.get("user_id")
    )
    if isinstance(tech_value, dict):
        tech_value = (
            tech_value.get("name")
            or tech_value.get("full_name")
            or tech_value.get("display_name")
            or tech_value.get("email")
            or tech_value.get("id")
        )

    resolved_tech = None
    tech_candidates = [
        tech_value,
        timer.get("user_id"),
        timer.get("userId"),
        timer.get("userID"),
    ]

    for candidate in tech_candidates:
        if candidate is None:
            continue
        lookup_name = get_syncro_tech_name_by_id(candidate)
        if lookup_name:
            resolved_tech = lookup_name
            break
        candidate_text = _normalize_text(candidate)
        if candidate_text:
            resolved_tech = candidate_text
            break
    if resolved_tech is None:
        resolved_tech = ""

    created_value = (
        timer.get("start_at")
        or timer.get("start_time")
        or timer.get("created_at")
        or timer.get("timer_start")
        or timer.get("created")
    )

    return _make_timer_compare_signature(notes, resolved_tech, created_value)

def _make_entry_timer_signature(entry: Dict[str, Any]) -> Tuple[str, str, str]:
    return _make_timer_compare_signature(
        notes=entry.get("notes"),
        tech=entry.get("tech"),
        created_at=entry.get("created at"),
    )


def _make_entry_signature(entry: Dict[str, Any]) -> Tuple[str, str, str, str, str, str]:
    """Create a normalized signature used to detect duplicate labor rows."""

    def normalize(value: Any, lower: bool = False) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        return text.lower() if lower else text

    ticket_number_raw = entry.get("ticket number") or ""
    ticket_number_clean = clean_syncro_ticket_number(ticket_number_raw) or ticket_number_raw

    return (
        ticket_number_clean,
        normalize(entry.get("entry sequence")),
        normalize(entry.get("created at")),
        normalize(entry.get("duration minutes")),
        normalize(entry.get("tech"), lower=True),
        normalize(entry.get("notes")),
    )


def _get_existing_timer_signatures(
    config,
    ticket_id: int,
    cache: Dict[int, Set[Tuple[str, str, str]]],
) -> Set[Tuple[str, str, str]]:
    if ticket_id in cache:
        return cache[ticket_id]

    try:
        logger.info("Fetching existing timer entries for ticket ID %s.", ticket_id)
        existing_entries = syncro_get_ticket_timer_entries(config, ticket_id)
    except Exception as exc:
        logger.error(
            "Failed to fetch existing timer entries for ticket ID %s: %s",
            ticket_id,
            exc,
        )
        cache[ticket_id] = set()
        return cache[ticket_id]

    signatures: Set[Tuple[str, str, str]] = set()
    for timer in existing_entries or []:
        if not isinstance(timer, dict):
            continue
        signature = _make_remote_timer_signature(timer)
        if not any(signature):
            logger.debug(
                "Skipping remote timer without comparable fields for ticket ID %s: %s",
                ticket_id,
                timer,
            )
            continue
        signatures.add(signature)
        logger.debug(
            "Existing timer signature for ticket ID %s -> notes='%s', tech='%s', timestamp='%s'.",
            ticket_id,
            _truncate_for_log(signature[0]),
            signature[1],
            signature[2],
        )

    cache[ticket_id] = signatures
    logger.debug(
        "Cached %s existing timer signatures for ticket ID %s.",
        len(signatures),
        ticket_id,
    )
    return signatures


def run_ticket_labor(config, *, interactive: bool = False, max_entries: Optional[int] = None) -> None:
    """Import ticket labor (timer) entries from CSV into Syncro."""

    try:
        load_or_fetch_temp_data(config)
        labor_entries = syncro_get_all_ticket_labor_entries_from_csv()
        logger.info(f"Loaded labor entries: {len(labor_entries)}")
    except Exception as e:
        logger.critical(f"Failed to load labor entries: {e}")
        return

    ticket_cache: Dict[str, Optional[Dict[str, Optional[str]]]] = {}
    ticket_timer_cache: Dict[int, Set[Tuple[str, str, str]]] = {}
    sorted_entries = _sort_labor_entries(labor_entries)
    processed_signatures: Set[Tuple[str, str, str, str, str, str]] = set()
    entries_attempted = 0

    for entry in sorted_entries:
        ticket_number_raw = entry.get("ticket number")
        if not ticket_number_raw:
            logger.error("Labor entry missing ticket number; skipping entry.")
            continue

        entry_signature = _make_entry_signature(entry)
        if entry_signature in processed_signatures:
            logger.warning(
                "Duplicate labor entry detected within CSV for ticket %s (sequence %s, created %s); skipping entry.",
                ticket_number_raw,
                entry.get("entry sequence"),
                entry.get("created at"),
            )
            continue

        processed_signatures.add(entry_signature)

        ticket = _ensure_ticket(config, ticket_cache, ticket_number_raw)
        if not ticket:
            continue

        payload = syncro_prepare_ticket_labor_json(config, entry, ticket)
        if not payload:
            logger.error(
                f"Unable to prepare labor payload for ticket {ticket_number_raw}; entry skipped."
            )
            continue

        ticket_id = ticket.get("id")
        if not ticket_id:
            logger.error(f"Ticket data missing ID for ticket {ticket_number_raw}; skipping entry.")
            continue

        entry_was_attempted = False

        try:
            try:
                ticket_id_int = int(ticket_id)
            except (TypeError, ValueError):
                logger.error(
                    "Ticket ID '%s' for ticket %s is not a valid integer; skipping entry.",
                    ticket_id,
                    ticket_number_raw,
                )
                continue

            entry_was_attempted = True

            entry_timer_signature = _make_entry_timer_signature(entry)
            logger.debug(
                "Prepared entry timer signature for ticket %s -> notes='%s', tech='%s', timestamp='%s'.",
                ticket_number_raw,
                _truncate_for_log(entry_timer_signature[0]),
                entry_timer_signature[1],
                entry_timer_signature[2],
            )
            if interactive:
                _log_signature(entry_timer_signature, "[Interactive] Entry signature ->")
                if not _interactive_pause(
                    interactive,
                    f"Review entry for ticket {ticket_number_raw}.",
                ):
                    return

            existing_timer_signatures = _get_existing_timer_signatures(
                config, ticket_id_int, ticket_timer_cache
            )
            logger.debug(
                "Comparing against %s existing timer signatures for ticket ID %s.",
                len(existing_timer_signatures),
                ticket_id_int,
            )

            if interactive:
                if existing_timer_signatures:
                    logger.info("Existing timer signatures for ticket %s:", ticket_number_raw)
                    for index, signature in enumerate(existing_timer_signatures, start=1):
                        _log_signature(signature, f"[Interactive] Existing #{index} ->")
                else:
                    logger.info("No existing timer signatures for ticket %s.", ticket_number_raw)
                if not _interactive_pause(
                    interactive,
                    "Press Enter to compare this entry against the signatures above.",
                ):
                    return

            if entry_timer_signature in existing_timer_signatures:
                logger.warning(
                    "Labor entry for ticket %s matches an existing Syncro timer (same notes/tech/date); skipping entry. Signature: notes='%s', tech='%s', timestamp='%s'.",
                    ticket_number_raw,
                    _truncate_for_log(entry_timer_signature[0]),
                    entry_timer_signature[1],
                    entry_timer_signature[2],
                )
                if interactive:
                    logger.info(
                        "Comparison result: existing timer detected for ticket %s.",
                        ticket_number_raw,
                    )
                    if not _interactive_pause(
                        interactive,
                        "Entry skipped; press Enter to continue to the next ticket.",
                    ):
                        return
                continue

            if interactive:
                logger.info(
                    "Comparison result: no matching timers found for ticket %s.",
                    ticket_number_raw,
                )
                if not _interactive_pause(
                    interactive,
                    "Press Enter to create and optionally charge this timer entry.",
                ):
                    return

            should_charge = parse_charge_flag(entry.get("charge?"))

            try:
                response = syncro_create_ticket_timer_entry(config, ticket_id_int, payload)
                if response:
                    logger.info(
                        f"Successfully created labor entry for ticket {ticket_number_raw} with payload {payload}."
                    )

                    if should_charge:
                        timer_entry_id = _extract_timer_entry_id(response)
                        if timer_entry_id is None:
                            logger.error(
                                f"Timer entry ID missing from response for ticket {ticket_number_raw}; unable to charge entry."
                            )
                        else:
                            charged = syncro_charge_ticket_timer_entry(
                                config, ticket_id_int, timer_entry_id
                            )
                            if charged:
                                logger.info(
                                    f"Timer entry {timer_entry_id} for ticket {ticket_number_raw} charged successfully."
                                )
                            else:
                                logger.error(
                                    f"Failed to charge timer entry {timer_entry_id} for ticket {ticket_number_raw}."
                                )
                    else:
                        logger.info(
                            f"Charge flag disabled for labor entry on ticket {ticket_number_raw}; timer left uncharged."
                        )

                    existing_timer_signatures.add(entry_timer_signature)
                    logger.debug(
                        "Added new timer signature to cache for ticket ID %s -> notes='%s', tech='%s', timestamp='%s'.",
                        ticket_id_int,
                        _truncate_for_log(entry_timer_signature[0]),
                        entry_timer_signature[1],
                        entry_timer_signature[2],
                    )
                else:
                    logger.error(
                        f"Failed to create labor entry for ticket {ticket_number_raw}. Payload: {payload}"
                    )
            except Exception as e:
                logger.error(
                    f"Unexpected error while creating labor entry for ticket {ticket_number_raw}: {e}"
                )
        finally:
            if entry_was_attempted:
                entries_attempted += 1
                if interactive:
                    if not _interactive_pause(
                        interactive,
                        "Entry complete; press Enter to inspect the next ticket.",
                    ):
                        return
                if max_entries is not None and entries_attempted >= max_entries:
                    logger.info(
                        "Processed %s labor entry(ies); stopping early as requested.",
                        entries_attempted,
                    )
                    return

    api_call_count = get_api_call_count()
    logger.info(f"Total API calls made during program run: {api_call_count}")


if __name__ == "__main__":
    setup_logging()
    logger.info("Running ticket labor importer in interactive debug mode.")
    direct_config = _load_direct_run_config()
    if direct_config:
        run_ticket_labor(direct_config, interactive=True, max_entries=5)
    else:
        logger.error("Unable to initialize configuration for interactive run.")
