from typing import Dict, List, Optional

from syncro_utils import (
    clean_syncro_ticket_number,
    load_or_fetch_temp_data,
    syncro_get_all_ticket_labor_entries_from_csv,
    syncro_prepare_ticket_labor_json,
)
from syncro_write import syncro_create_ticket_timer_entry
from syncro_read import get_api_call_count, get_syncro_ticket_by_number
from syncro_configs import get_logger

logger = get_logger(__name__)


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


def run_ticket_labor(config) -> None:
    """Import ticket labor (timer) entries from CSV into Syncro."""

    try:
        load_or_fetch_temp_data(config)
        labor_entries = syncro_get_all_ticket_labor_entries_from_csv()
        logger.info(f"Loaded labor entries: {len(labor_entries)}")
    except Exception as e:
        logger.critical(f"Failed to load labor entries: {e}")
        return

    ticket_cache: Dict[str, Optional[Dict[str, Optional[str]]]] = {}
    sorted_entries = _sort_labor_entries(labor_entries)

    for entry in sorted_entries:
        ticket_number_raw = entry.get("ticket number")
        if not ticket_number_raw:
            logger.error("Labor entry missing ticket number; skipping entry.")
            continue

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

        try:
            response = syncro_create_ticket_timer_entry(config, ticket_id, payload)
            if response:
                logger.info(
                    f"Successfully created labor entry for ticket {ticket_number_raw} with payload {payload}."
                )
            else:
                logger.error(
                    f"Failed to create labor entry for ticket {ticket_number_raw}. Payload: {payload}"
                )
        except Exception as e:
            logger.error(
                f"Unexpected error while creating labor entry for ticket {ticket_number_raw}: {e}"
            )

    api_call_count = get_api_call_count()
    logger.info(f"Total API calls made during program run: {api_call_count}")


if __name__ == "__main__":
    print("This is main_ticket_labor.py")
