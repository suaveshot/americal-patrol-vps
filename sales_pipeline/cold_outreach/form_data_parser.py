"""
Sales Pipeline — Cold Outreach: Form Data Parser
Extracts structured fields from a GHL contact's customFields array.
Property type is normalized to one of: commercial, industrial, retail, hoa, other.
"""

import logging

log = logging.getLogger(__name__)

# Keyword -> normalized type mapping (case-insensitive substring match)
PROPERTY_TYPE_MAP = [
    ("commercial",  "commercial"),
    ("industrial",  "industrial"),
    ("warehouse",   "industrial"),
    ("retail",      "retail"),
    ("shopping",    "retail"),
    ("hoa",         "hoa"),
    ("apartment",   "hoa"),
    ("residential", "hoa"),
]

# GHL custom field key names (may vary by account setup)
FIELD_ALIASES = {
    "property_type":    ["property_type", "propertytype", "type_of_property", "account_type"],
    "property_address": ["property_address", "address", "street_address", "propertyaddress"],
    "property_city":    ["property_city", "city", "propertycity"],
    "inquiry_details":  ["details", "inquiry_details", "message", "comments", "notes", "description"],
}


def _normalize_property_type(raw: str) -> str:
    """Map raw GHL form value to canonical property type."""
    if not raw:
        return "other"
    lower = raw.lower()
    for keyword, normalized in PROPERTY_TYPE_MAP:
        if keyword in lower:
            return normalized
    return "other"


def _extract_custom_field(custom_fields: list, aliases: list) -> str:
    """Return first matching custom field value from a list of key aliases."""
    if not custom_fields:
        return ""
    for field in custom_fields:
        key = (field.get("key") or field.get("name") or "").lower().replace(" ", "_")
        if key in aliases:
            return str(field.get("field_value") or field.get("value") or "").strip()
    return ""


def parse_contact(contact: dict) -> dict:
    """
    Return a parsed contact dict with structured fields extracted from GHL customFields.
    All fields default to empty string — never crashes on missing data.
    """
    custom = contact.get("customFields") or contact.get("custom_fields") or []

    property_type_raw = _extract_custom_field(custom, FIELD_ALIASES["property_type"])
    property_type = _normalize_property_type(property_type_raw)

    # City: prefer custom field, fall back to standard GHL field
    city = (_extract_custom_field(custom, FIELD_ALIASES["property_city"])
            or contact.get("city") or "").strip()

    return {
        "id":               contact.get("id", ""),
        "first_name":       contact.get("firstName") or contact.get("first_name") or "",
        "last_name":        contact.get("lastName") or contact.get("last_name") or "",
        "email":            contact.get("email", ""),
        "phone":            contact.get("phone", ""),
        "organization":     contact.get("companyName") or contact.get("company_name") or "",
        "property_type":    property_type,
        "property_address": _extract_custom_field(custom, FIELD_ALIASES["property_address"]),
        "property_city":    city,
        "inquiry_details":  _extract_custom_field(custom, FIELD_ALIASES["inquiry_details"]),
        "days_since_contact": contact.get("days_since_contact", 0),
        "last_contact_at":  contact.get("last_contact_at", ""),
    }
