from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from syncro_configs import get_logger, INVOICE_IMPORT_CSV_PATH
from syncro_utils import (
    load_or_fetch_temp_data,
    syncro_get_invoice_rows_from_csv,
    syncro_prepare_invoice_payload,
    sanitize_invoice_number,
)
from syncro_read import get_api_call_count, syncro_get_all_invoices
from syncro_write import syncro_create_invoice

logger = get_logger(__name__)


def _normalize_invoice_number(value: Optional[str]) -> Optional[str]:
    """Return a numeric-only invoice number for comparison."""

    return sanitize_invoice_number(value)


def _group_invoice_rows(rows: List[Dict[str, str]]) -> "OrderedDict[str, List[Dict[str, str]]]":
    """Group CSV rows by invoice number while preserving original order."""

    grouped: "OrderedDict[str, List[Dict[str, str]]]" = OrderedDict()
    for row in rows:
        invoice_number = (row.get("invoice number") or "").strip()
        grouped.setdefault(invoice_number, []).append(row)
    return grouped


def run_invoice_import(config) -> None:
    """Create Syncro invoices based on the invoice import CSV."""

    logger.info("Starting invoice import using %s.", INVOICE_IMPORT_CSV_PATH)

    try:
        load_or_fetch_temp_data(config=config)
    except Exception as exc:
        logger.critical("Unable to load Syncro reference data required for invoices: %s", exc)
        return

    try:
        invoice_rows = syncro_get_invoice_rows_from_csv()
    except Exception as exc:
        logger.critical("Unable to read invoice CSV: %s", exc)
        return

    if not invoice_rows:
        logger.warning("Invoice CSV %s did not provide any rows to import.", INVOICE_IMPORT_CSV_PATH)
        return

    grouped_rows = _group_invoice_rows(invoice_rows)

    existing_invoices = syncro_get_all_invoices(config)
    existing_invoice_numbers = {
        normalized
        for normalized in (
            _normalize_invoice_number(invoice.get("number"))
            for invoice in existing_invoices or []
        )
        if normalized
    }
    logger.info("Loaded %s existing invoices for duplicate detection.", len(existing_invoice_numbers))

    contact_cache: Dict[Tuple[int, str], Optional[int]] = {}

    imported = 0
    skipped_duplicates = 0
    failed = 0

    for invoice_number, rows in grouped_rows.items():
        primary_row_number = rows[0].get("invoice number")
        normalized_invoice_number = _normalize_invoice_number(primary_row_number)
        invoice_label = normalized_invoice_number or invoice_number or "<auto>"

        if normalized_invoice_number and normalized_invoice_number in existing_invoice_numbers:
            logger.warning(
                "Invoice %s already exists in Syncro; skipping to avoid duplicates.",
                normalized_invoice_number,
            )
            skipped_duplicates += 1
            continue

        customer_names = {
            (row.get("customer") or "").strip()
            for row in rows
            if row.get("customer")
        }
        if len(customer_names) > 1:
            logger.error(
                "Invoice %s rows reference multiple customers %s; skipping this invoice.",
                invoice_label,
                sorted(customer_names),
            )
            failed += 1
            continue

        payload = syncro_prepare_invoice_payload(
            config,
            rows,
            contact_cache=contact_cache,
        )
        if not payload:
            failed += 1
            continue

        try:
            response = syncro_create_invoice(config, payload)
        except Exception as exc:
            logger.error("Unexpected error occurred while creating invoice %s: %s", invoice_label, exc)
            failed += 1
            continue

        if not response:
            failed += 1
            continue

        imported += 1
        created_number = _normalize_invoice_number(
            (response.get("invoice") or response).get("number")
        )
        if created_number:
            existing_invoice_numbers.add(created_number)
        elif normalized_invoice_number:
            existing_invoice_numbers.add(normalized_invoice_number)

    logger.info(
        "Invoice import completed: %s created, %s skipped as duplicates, %s failed.",
        imported,
        skipped_duplicates,
        failed,
    )
    logger.info("Total API calls made during invoice import: %s", get_api_call_count())


if __name__ == "__main__":
    print("This is main_invoice_import.py")
