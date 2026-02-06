from __future__ import annotations

from typing import Any, Dict


def observed_schema_for_xero() -> Dict[str, Any]:
    # Minimal “shape” snapshot; Xero responses are large and nested.
    return {
        "streams": {
            "organisation": {"primary_key": ["OrganisationID"], "fields": {"OrganisationID": "string"}},
            "users": {"primary_key": ["UserID"], "fields": {"UserID": "string"}},
            "currencies": {"primary_key": ["Code"], "fields": {"Code": "string"}},
            "tax_rates": {"primary_key": ["TaxType"], "fields": {"TaxType": "string"}},
            "tracking_categories": {"primary_key": ["TrackingCategoryID"], "fields": {"TrackingCategoryID": "string"}},
            "accounts": {"primary_key": ["AccountID"], "fields": {"AccountID": "string"}},
            "contacts": {"primary_key": ["ContactID"], "fields": {"ContactID": "string"}},
            "items": {"primary_key": ["ItemID"], "fields": {"ItemID": "string"}},
            "invoices": {"primary_key": ["InvoiceID"], "fields": {"InvoiceID": "string"}},
            "credit_notes": {"primary_key": ["CreditNoteID"], "fields": {"CreditNoteID": "string"}},
            "payments": {"primary_key": ["PaymentID"], "fields": {"PaymentID": "string"}},
            "bank_transactions": {"primary_key": ["BankTransactionID"], "fields": {"BankTransactionID": "string"}},
            "manual_journals": {"primary_key": ["ManualJournalID"], "fields": {"ManualJournalID": "string"}},
            "purchase_orders": {"primary_key": ["PurchaseOrderID"], "fields": {"PurchaseOrderID": "string"}},
            "prepayments": {"primary_key": ["PrepaymentID"], "fields": {"PrepaymentID": "string"}},
            "overpayments": {"primary_key": ["OverpaymentID"], "fields": {"OverpaymentID": "string"}},
        }
    }
