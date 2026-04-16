"""
WC Solns Platform -- Provider Factory

Reads tenant_config.json to determine which adapter to use for each
provider category (CRM, email, SMS, reviews), then returns a
singleton instance.

Usage:
    from providers import get_crm, get_email, get_sms, get_reviews

    crm = get_crm()
    contacts = crm.list_contacts()

    email = get_email()
    email.send_email("to@example.com", "Subject", "<p>Body</p>")

    sms = get_sms()
    sms.send_sms("+18055551234", "Hello!")

    reviews = get_reviews()
    reviews.get_reviews()
"""

import logging

from providers.base import (
    CRMProvider,
    EmailProvider,
    SMSProvider,
    ReviewProvider,
)

log = logging.getLogger(__name__)

_instances: dict[str, object] = {}


def _get_tenant_config() -> dict:
    """Load tenant_config.json via tenant_context."""
    from tenant_context import _load
    return _load()


def _provider_name(category: str, default: str = "") -> str:
    """Read which provider is selected for a category."""
    config = _get_tenant_config()
    return config.get("providers", {}).get(category, default)


def _provider_config(name: str) -> dict:
    """Read provider-specific config block."""
    config = _get_tenant_config()
    return config.get("provider_config", {}).get(name, {})


# -- CRM ---------------------------------------------------------------

def get_crm() -> CRMProvider:
    """Return the configured CRM provider instance."""
    if "crm" not in _instances:
        name = _provider_name("crm", default="ghl")
        cfg = _provider_config(name)
        log.info("Initializing CRM provider: %s", name)

        if name == "ghl":
            from providers.crm.ghl import GHLCRMProvider
            _instances["crm"] = GHLCRMProvider(cfg)
        else:
            raise ValueError(f"Unknown CRM provider: {name}")

    return _instances["crm"]


# -- Email --------------------------------------------------------------

def get_email() -> EmailProvider:
    """Return the configured email provider instance."""
    if "email" not in _instances:
        name = _provider_name("email", default="gmail")
        cfg = _provider_config(name)
        log.info("Initializing email provider: %s", name)

        if name == "gmail":
            from providers.email.gmail import GmailProvider
            _instances["email"] = GmailProvider(cfg)
        else:
            raise ValueError(f"Unknown email provider: {name}")

    return _instances["email"]


# -- SMS ----------------------------------------------------------------

def get_sms() -> SMSProvider:
    """Return the configured SMS provider instance."""
    if "sms" not in _instances:
        name = _provider_name("sms", default="ghl")
        cfg = _provider_config(name)
        log.info("Initializing SMS provider: %s", name)

        if name == "ghl":
            from providers.sms.ghl_sms import GHLSMSProvider
            _instances["sms"] = GHLSMSProvider(cfg)
        else:
            raise ValueError(f"Unknown SMS provider: {name}")

    return _instances["sms"]


# -- Reviews ------------------------------------------------------------

def get_reviews(platform: str = "") -> ReviewProvider:
    """Return the configured review provider instance."""
    if not platform:
        platform = _provider_name("reviews", default="gbp")

    key = f"reviews_{platform}"
    if key not in _instances:
        cfg = _provider_config(platform)
        log.info("Initializing review provider: %s", platform)

        if platform == "gbp":
            from providers.reviews.gbp_reviews import GBPReviewProvider
            _instances[key] = GBPReviewProvider(cfg)
        else:
            raise ValueError(f"Unknown review provider: {platform}")

    return _instances[key]


# -- Utility ------------------------------------------------------------

def reset():
    """Clear all cached provider instances (for testing)."""
    _instances.clear()
