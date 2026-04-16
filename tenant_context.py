"""
WC Solns Platform -- Tenant Context (AP VPS)
Loads tenant_config.json and provides accessor functions for all
company-specific values. Every pipeline imports from here instead
of hardcoding business identity.

Stdlib-only. No external dependencies.
"""

import json
import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent
_CONFIG_PATH = _PROJECT_ROOT / "tenant_config.json"

_config: dict | None = None


def _load() -> dict:
    """Load and cache tenant_config.json."""
    global _config
    if _config is None:
        if not _CONFIG_PATH.exists():
            # Fallback defaults for AP
            _config = {
                "active": True,
                "client_id": "americal_patrol",
                "company": {
                    "name": "Americal Patrol",
                    "legal_name": "Americal Patrol, Inc.",
                    "phone": "(805) 515-3834",
                    "address": "Ventura County, CA",
                    "city": "Ventura",
                    "state": "CA",
                    "website": "americalpatrol.com",
                    "website_url": "https://americalpatrol.com",
                    "industry": "security",
                    "founded_year": 1986,
                    "tagline": "Professional Security Services Since 1986",
                    "service_areas": ["Ventura County", "Santa Barbara County", "Los Angeles County"],
                },
                "contact": {
                    "owner_name": "Sam Alarcon",
                    "owner_title": "Operations Manager",
                    "owner_email": "salarcon@americalpatrol.com",
                    "owner_phone": "(805) 515-3834",
                },
            }
        else:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                _config = json.load(f)
    return _config


def reload():
    """Force reload config (useful for testing)."""
    global _config
    _config = None
    return _load()


# ---------------------------------------------------------------------------
# Account status & identity
# ---------------------------------------------------------------------------

def is_active() -> bool:
    """Whether this client's automations are active. False = payment paused."""
    return _load().get("active", True)


def client_id() -> str:
    """Unique client identifier used for usage tracking and payment guard."""
    return _load().get("client_id", "americal_patrol")


def usage_thresholds() -> dict:
    """Per-tier monthly API cost thresholds."""
    return _load().get("usage_thresholds", {})


# ---------------------------------------------------------------------------
# Company
# ---------------------------------------------------------------------------

def get_company() -> dict:
    """Return the full company section."""
    return _load().get("company", {"name": "Americal Patrol"})


def company_name() -> str:
    return get_company().get("name", "Americal Patrol")


def company_legal_name() -> str:
    return get_company().get("legal_name", company_name())


def company_phone() -> str:
    return get_company().get("phone", "(805) 515-3834")


def company_address() -> str:
    return get_company().get("address", "Ventura County, CA")


def company_city() -> str:
    """Return city -- explicit field preferred, fallback to address parsing."""
    city = get_company().get("city")
    if city:
        return city
    return get_company().get("address", "Ventura").split(",")[0].strip()


def company_state() -> str:
    """Return state abbreviation if available."""
    return get_company().get("state", "CA")


def company_website() -> str:
    return get_company().get("website", "americalpatrol.com")


def company_website_url() -> str:
    return get_company().get("website_url", "https://americalpatrol.com")


def company_industry() -> str:
    return get_company().get("industry", "security")


def company_tagline() -> str:
    return get_company().get("tagline", "")


def service_areas() -> list[str]:
    return get_company().get("service_areas", [])


def founded_year() -> int:
    return get_company().get("founded_year", 1986)


# ---------------------------------------------------------------------------
# Contact (owner / primary contact)
# ---------------------------------------------------------------------------

def get_contact() -> dict:
    """Return the full contact section."""
    return _load().get("contact", {
        "owner_name": "Sam Alarcon",
        "owner_title": "Operations Manager",
        "owner_email": "salarcon@americalpatrol.com",
        "owner_phone": "(805) 515-3834",
    })


def owner_name() -> str:
    return get_contact().get("owner_name", "Sam Alarcon")


def owner_title() -> str:
    return get_contact().get("owner_title", "Operations Manager")


def owner_email() -> str:
    return get_contact().get("owner_email", "salarcon@americalpatrol.com")


def owner_phone() -> str:
    return get_contact().get("owner_phone", company_phone())


# ---------------------------------------------------------------------------
# Branding
# ---------------------------------------------------------------------------

def get_branding() -> dict:
    """Return the full branding section."""
    return _load().get("branding", {})


def logo_url() -> str:
    return get_branding().get("logo_url", "")


def banner_url() -> str:
    return get_branding().get("banner_url", "")


def headshot_url() -> str:
    return get_branding().get("headshot_url", "")


def primary_color() -> str:
    return get_branding().get("primary_color", "#1a1a2e")


def signature_html_override() -> str | None:
    return get_branding().get("signature_html_override")


# ---------------------------------------------------------------------------
# AI Context (used in Claude system prompts)
# ---------------------------------------------------------------------------

def get_ai_context() -> dict:
    """Return the full ai_context section."""
    return _load().get("ai_context", {})


def company_description() -> str:
    return get_ai_context().get("company_description", "")


def services_list() -> list[str]:
    return get_ai_context().get("services_list", [])


def selling_points() -> list[str]:
    return get_ai_context().get("selling_points", [])


def voice_agent_personality() -> str:
    return get_ai_context().get("voice_agent_personality", "")


def email_response_guidelines() -> str:
    return get_ai_context().get("email_response_guidelines", "")


def property_angles() -> dict:
    """Industry-specific talking points per property/client type."""
    return get_ai_context().get("property_angles", {})


def location_rules() -> str:
    """Location-specific rules for outreach."""
    return get_ai_context().get("location_rules", "")


# ---------------------------------------------------------------------------
# Integrations
# ---------------------------------------------------------------------------

def get_integrations() -> dict:
    """Return the full integrations section."""
    return _load().get("integrations", {})


def ghl_location_id() -> str:
    env_key = get_integrations().get("ghl_location_id_env", "GHL_LOCATION_ID")
    return os.getenv(env_key, "")


def ghl_api_key() -> str:
    env_key = get_integrations().get("ghl_api_key_env", "GHL_API_KEY")
    return os.getenv(env_key, "")


def gmail_account() -> str:
    return get_integrations().get("gmail_account", "americalpatrol@gmail.com")


def gbp_account_id() -> str:
    return get_integrations().get("gbp_account_id", "")


def gbp_location_id() -> str:
    return get_integrations().get("gbp_location_id", "")


# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------

def pipelines_enabled() -> list[str]:
    """List of pipeline names enabled for this tenant."""
    return _load().get("pipelines_enabled", [])


def is_pipeline_enabled(pipeline_name: str) -> bool:
    return pipeline_name in pipelines_enabled()


# ---------------------------------------------------------------------------
# Outreach settings
# ---------------------------------------------------------------------------

def get_outreach() -> dict:
    """Return the outreach settings section (optional)."""
    return _load().get("outreach", {})


def sending_domain() -> str:
    return get_outreach().get("sending_domain", company_website())


def sender_email() -> str:
    return get_outreach().get("sender_email", owner_email())


def sender_name() -> str:
    return get_outreach().get("sender_name", owner_name())


def unsubscribe_fallback_email() -> str:
    return get_outreach().get("unsubscribe_fallback_email", owner_email())


# ---------------------------------------------------------------------------
# Providers (which tools are selected per category)
# ---------------------------------------------------------------------------

def get_providers() -> dict:
    """Return the providers section (tool selections per category)."""
    return _load().get("providers", {})


def provider_name(category: str, default: str = "") -> str:
    """Get the selected provider for a category (crm, email, sms, reviews)."""
    return get_providers().get(category, default)


def get_provider_config(provider: str) -> dict:
    """Get provider-specific config block."""
    return _load().get("provider_config", {}).get(provider, {})


# ---------------------------------------------------------------------------
# Review engine extended configuration
# ---------------------------------------------------------------------------

def get_review_engine_config() -> dict:
    """Return the review engine reputation config section."""
    return _load().get("review_engine_config", {})


# ---------------------------------------------------------------------------
# Win-back configuration
# ---------------------------------------------------------------------------

def get_win_back() -> dict:
    """Return the full win_back config section."""
    return _load().get("win_back", {})


def win_back_enabled() -> bool:
    return get_win_back().get("enabled", False)


# ---------------------------------------------------------------------------
# Owner digest configuration
# ---------------------------------------------------------------------------

def get_owner_digest() -> dict:
    """Return the owner digest config section."""
    return _load().get("owner_digest", {})


# ---------------------------------------------------------------------------
# ROI tracking configuration
# ---------------------------------------------------------------------------

def get_roi_tracking() -> dict:
    """Return the ROI tracking config section."""
    return _load().get("roi_tracking", {})
