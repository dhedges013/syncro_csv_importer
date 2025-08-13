"""Utility functions for working with CSV data used by Syncro importer.

This module was split out from ``syncro_utils`` to separate the routines
that solely deal with loading and validating CSV content.  Functions here
are focused on reading CSV files and preparing data structures used by the
rest of the application.
"""

from typing import Any, Dict, List
import csv
import json
import logging

from syncro_configs import (
    get_logger,
    TICKETS_CSV_PATH,
    COMMENTS_CSV_PATH,
    COMBINED_TICKETS_COMMENTS_CSV_PATH,
)

# ``DEFAULTS`` is defined in ``syncro_utils`` and contains default values for
# various CSV columns.  It is imported here so the behaviour of ``load_csv``
# remains unchanged after moving these helpers into their own module.
from syncro_utils import DEFAULTS  # pylint: disable=wrong-import-position


logger = get_logger(__name__)


def extract_nested_key(data: dict, key_path: str):
    """Extract a nested key from a dictionary using dot notation."""

    keys = key_path.split(".")
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return None
    return data


def load_csv(
    filepath: str,
    required_fields: List[str] | None = None,
    logger: logging.Logger | None = None,
) -> List[Dict[str, Any]]:
    """Load data from a CSV file with validation for required fields.

    Blank values for keys present in ``DEFAULTS`` are filled with their
    configured defaults instead of raising a validation error.
    """

    if logger is None:
        logger = logging.getLogger("syncro")

    try:
        logger.debug(f"Loading data from CSV file: {filepath}")
        with open(filepath, mode="r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames or []
            headers_lower = [h.lower() for h in headers]

            if required_fields:
                required_map = {field.lower(): field for field in required_fields}
                required_lower = list(required_map.keys())
                missing_fields = [
                    required_map[field_lower]
                    for field_lower in required_lower
                    if field_lower not in headers_lower
                ]
                if missing_fields:
                    raise ValueError(
                        f"Missing required fields in CSV file: {missing_fields}"
                    )

            data: List[Dict[str, Any]] = []
            for row_number, row in enumerate(reader, start=1):
                cleaned_row: Dict[str, Any] = {}
                for key, value in row.items():
                    key_lower = key.lower()
                    if value is None or value.strip() == "":
                        default_value = DEFAULTS.get(key_lower)
                        if default_value is not None:
                            logger.info(
                                f"Row {row_number}: Field '{key}' is blank, applying default '{default_value}'."
                            )
                            value = default_value
                        else:
                            raise ValueError(
                                f"Row {row_number}: Empty value found in field '{key}'."
                            )
                    cleaned_row[key_lower] = value

                if required_fields:
                    for field_lower in required_lower:
                        if (
                            field_lower not in cleaned_row
                            or cleaned_row[field_lower].strip() == ""
                        ):
                            raise ValueError(
                                f"Row {row_number}: Missing or blank required field '{required_map[field_lower]}'."
                            )

                data.append(cleaned_row)

            logger.debug(
                f"Successfully loaded {len(data)} rows from {filepath}."
            )
            return data

    except FileNotFoundError:
        logger.error(f"CSV file not found: {filepath}")
        raise
    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise
    except Exception as e:  # pragma: no cover - safeguard for unexpected errors
        logger.error(f"Error reading CSV file {filepath}: {e}")
        raise


def validate_ticket_data(
    tickets: List[Dict[str, Any]],
    temp_data: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Validate ticket data against cached API data."""

    logger.debug("Validating ticket data...")

    techs = temp_data.get("techs", [])
    customers = temp_data.get("customers", [])
    issue_types = temp_data.get("issue_types", [])
    statuses = temp_data.get("statuses", [])
    contacts = temp_data.get("contacts", [])
    logger.debug(
        f"Retrieved techs: {techs}, issue_types: {issue_types}, statuses: {statuses}, Ignoring Customers and contacts due to long load"
    )

    try:
        tech_names = {t[1].lower() for t in techs}
    except Exception as e:  # pragma: no cover - defensive coding
        logger.error(f"Error extracting tech names: {e}")
        raise
    try:
        customer_names = {c["business_name"].lower() for c in customers}
    except Exception as e:
        logger.error(f"Error extracting customer names: {e}")
        raise
    try:
        issue_type_names = {i.lower() for i in issue_types}
    except Exception as e:
        logger.error(f"Error extracting issue type names: {e}")
        raise
    try:
        status_names = {s for s in statuses}
    except Exception as e:
        logger.error(f"Error extracting status names: {e}")
        raise
    try:
        contact_names = {c["name"].lower() for c in contacts if c["name"]}
    except Exception as e:
        logger.error(f"Error extracting contact names: {e}")
        raise

    use_default_issue_type_for_all: bool | None = None
    for row_num, ticket in enumerate(tickets, start=1):
        logger.debug(f"Validation for Row {row_num} - Raw ticket data: {ticket}")

        tech_val = ticket["tech"].strip().lower()
        customer_val = ticket["ticket customer"].strip().lower()
        issue_type_val = ticket["ticket issue type"].strip().lower()
        status_val = ticket["ticket status"]
        contact_val = ticket.get("ticket contact").strip().lower()

        logger.debug(
            f"Validation Row {row_num} - Checking tech='{tech_val}', customer='{customer_val}', "
            f"issue_type='{issue_type_val}', status='{status_val}', contact='{contact_val}'"
        )

        if tech_val not in tech_names:
            logger.error(
                f"Row {row_num}: Tech '{tech_val}' not found in API cache."
            )
            raise ValueError(
                f"Row {row_num}: Tech '{tech_val}' not found in API cache."
            )

        if customer_val not in customer_names:
            logger.error(
                f"Row {row_num}: Customer '{customer_val}' not found in API cache."
            )
            raise ValueError(
                f"Row {row_num}: Customer '{customer_val}' not found in API cache."
            )

        if issue_type_val not in issue_type_names:
            logger.error(
                f"Row {row_num}: Issue type '{issue_type_val}' not found in API cache."
            )
            default_issue_type = DEFAULTS.get("ticket issue type")
            if not (
                default_issue_type
                and default_issue_type.lower() in issue_type_names
            ):
                raise ValueError(
                    f"Row {row_num}: Issue type '{issue_type_val}' not found in API cache."
                )

            if use_default_issue_type_for_all is None:
                response = input(
                    f"Row {row_num}: Issue type '{issue_type_val}' not found. "
                    f"Use default '{default_issue_type}' for all missing issue types in this import? (y/N): "
                ).strip().lower()
                use_default_issue_type_for_all = response == "y"

            if use_default_issue_type_for_all:
                ticket["ticket issue type"] = default_issue_type
                logger.info(
                    f"Row {row_num}: Issue type set to default '{default_issue_type}'."
                )
            else:
                raise ValueError(
                    f"Row {row_num}: Issue type '{issue_type_val}' not found in API cache."
                )

        if status_val not in status_names:
            logger.warning("Status names cannot be normalized. Must be perfect match")
            logger.error(
                f"Row {row_num}: Status '{status_val}' not found in API cache."
            )
            raise ValueError(
                f"Row {row_num}: Status '{status_val}' not found in API cache."
            )

        if contact_val not in contact_names:
            logger.warning(
                f"Row {row_num}: Contact '{contact_val}' not found in API cache."
            )

        logger.debug(
            f"Validation Row {row_num} - Validation passed for this ticket."
        )

    logger.info("All tickets validated successfully.")


def syncro_get_all_tickets_from_csv(
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Load all tickets from a CSV file and validate them."""

    required_fields = [
        "ticket customer",
        "ticket number",
        "ticket subject",
        "tech",
        "ticket initial issue",
        "ticket status",
        "ticket issue type",
        "ticket created",
    ]

    try:
        from syncro_utils import load_or_fetch_temp_data  # local import to avoid cycle

        logger.info("Checking and Creating _temp_data_cache from API...")
        temp_data = load_or_fetch_temp_data(config)

        logger.info("Attempting to load tickets from CSV...")
        tickets = load_csv(
            TICKETS_CSV_PATH, required_fields=required_fields, logger=logger
        )

        validate_ticket_data(tickets, temp_data, logger)

        logger.debug(
            f"Successfully loaded {len(tickets)} tickets from {TICKETS_CSV_PATH}."
        )
        return tickets

    except FileNotFoundError:
        logger.error(f"CSV file not found: {TICKETS_CSV_PATH}")
        raise
    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise
    except Exception as e:  # pragma: no cover
        logger.error(
            f"An unexpected error occurred while loading tickets: {e}"
        )
        raise


def syncro_get_all_comments_from_csv() -> List[Dict[str, Any]]:
    """Load all comments from a CSV file."""

    required_fields = [
        "ticket number",
        "ticket comment",
        "comment contact",
        "comment created",
    ]

    try:
        logger.info("Attempting to load comments from CSV...")
        comments = load_csv(
            COMMENTS_CSV_PATH, required_fields=required_fields, logger=logger
        )
        logger.info(
            f"Successfully loaded {len(comments)} comments from {COMMENTS_CSV_PATH}."
        )
        return comments

    except FileNotFoundError:
        logger.error(f"CSV file not found: {COMMENTS_CSV_PATH}")
        raise
    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise
    except Exception as e:  # pragma: no cover
        logger.error(
            f"An unexpected error occurred while loading comments: {e}"
        )
        raise


def syncro_get_all_tickets_and_comments_from_combined_csv():
    """Load combined ticket and comment data from a single CSV file."""

    required_fields = [
        "ticket customer",
        "ticket number",
        "user",
        "ticket subject",
        "ticket description",
        "ticket response",
        "timestamp",
        "email body",
        "ticket status",
        "ticket issue type",
        "ticket created date",
        "ticket priority",
    ]

    try:
        from syncro_utils import (  # local import to avoid circular dependency
            group_comments_by_ticket_number,
        )

        logger.info("Attempting to load comments from CSV...")
        comments = load_csv(
            COMBINED_TICKETS_COMMENTS_CSV_PATH,
            required_fields=required_fields,
            logger=logger,
        )
        grouped_comments_by_ticket_number = group_comments_by_ticket_number(
            comments
        )
        logger.info(
            f"Successfully loaded {len(comments)} comments from {COMBINED_TICKETS_COMMENTS_CSV_PATH}."
        )
        return grouped_comments_by_ticket_number

    except FileNotFoundError:
        logger.error(
            f"CSV file not found: {COMBINED_TICKETS_COMMENTS_CSV_PATH}"
        )
        raise
    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise


__all__ = [
    "extract_nested_key",
    "load_csv",
    "validate_ticket_data",
    "syncro_get_all_tickets_from_csv",
    "syncro_get_all_comments_from_csv",
    "syncro_get_all_tickets_and_comments_from_combined_csv",
]

