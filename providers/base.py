"""
WC Solns Platform -- Provider Base Classes

Abstract interfaces that all provider adapters must implement.
Pipelines import from providers/__init__.py (the factory), never
from these base classes directly.

Each method returns standardized dicts -- adapters translate between
the provider's native API shape and these standard shapes.
"""

from abc import ABC, abstractmethod


class CRMProvider(ABC):
    """Contact/lead management, pipeline stages, deals, conversations, calls."""

    # -- Contacts -------------------------------------------------------

    @abstractmethod
    def list_contacts(self, limit: int = 100, offset: int = 0,
                      **filters) -> list[dict]:
        """Return contacts matching filters.

        Standard fields per contact:
            id, name, email, phone, company, tags, created_at, updated_at,
            custom_fields (dict)
        """

    @abstractmethod
    def get_contact(self, contact_id: str) -> dict:
        """Get a single contact by ID. Same standard fields as list_contacts."""

    @abstractmethod
    def create_contact(self, data: dict) -> dict:
        """Create a contact. Required keys: name, email.
        Returns the created contact dict."""

    @abstractmethod
    def update_contact(self, contact_id: str, data: dict) -> dict:
        """Update contact fields. Returns updated contact."""

    @abstractmethod
    def search_contacts(self, query: str) -> list[dict]:
        """Full-text search across contacts."""

    # -- Pipeline / Deals -----------------------------------------------

    @abstractmethod
    def list_pipeline_stages(self, pipeline_id: str = "") -> list[dict]:
        """Return pipeline stages.

        Standard fields per stage: id, name, order
        """

    @abstractmethod
    def get_opportunities(self, pipeline_id: str = "",
                          stage_id: str = "",
                          **filters) -> list[dict]:
        """Get deals/opportunities.

        Standard fields per opportunity:
            id, name, value, stage_id, stage_name, contact_id,
            status (open/won/lost), created_at, updated_at
        """

    @abstractmethod
    def create_opportunity(self, data: dict) -> dict:
        """Create a deal. Required keys: name, contact_id, stage_id.
        Returns the created opportunity dict."""

    @abstractmethod
    def update_opportunity(self, opp_id: str, data: dict) -> dict:
        """Update deal (stage, value, status, etc.)."""

    # -- Conversations / Messages ---------------------------------------

    @abstractmethod
    def get_conversations(self, contact_id: str) -> list[dict]:
        """Get message history for a contact.

        Standard fields per message:
            id, direction (inbound/outbound), channel (email/sms/call),
            body, timestamp
        """

    # -- Calls ----------------------------------------------------------

    @abstractmethod
    def get_calls(self, since: str = "", limit: int = 50) -> list[dict]:
        """Get recent call records.

        Standard fields per call:
            id, from_number, to_number, duration_seconds, status (answered/missed/voicemail),
            timestamp, contact_id (if matched)
        """

    # -- Feature detection ----------------------------------------------

    def supports_feature(self, feature: str) -> bool:
        """Check if this CRM supports an optional feature.

        Known features:
            'estimates'      - can create/manage price estimates
            'conversations'  - has built-in messaging/conversation history
            'calls'          - has built-in call tracking
            'sms'            - can send SMS through the CRM
            'email_sending'  - can send email through the CRM
            'pipelines'      - has built-in sales pipeline stages
        """
        return False


class EmailProvider(ABC):
    """Transactional and outreach email sending."""

    @abstractmethod
    def send_email(self, to, subject, html_body,
                   from_email="", from_name="",
                   reply_to="", bcc=None):
        """Send an email.

        Returns: {success: bool, message_id: str, error: str (if failed)}
        """

    @abstractmethod
    def create_draft(self, to, subject, html_body,
                     from_email="", from_name=""):
        """Create a draft email for review.

        Returns: {success: bool, draft_id: str}
        Raises NotImplementedError if the provider doesn't support drafts.
        """

    @abstractmethod
    def send_with_attachments(self, to, subject, html_body,
                              attachments,
                              from_email="",
                              from_name=""):
        """Send with file attachments.

        Each attachment dict: {filename: str, content: bytes, mime_type: str}
        Returns: {success: bool, message_id: str}
        """


class SMSProvider(ABC):
    """SMS / text message sending."""

    @abstractmethod
    def send_sms(self, to, message, from_number=""):
        """Send an SMS message.

        Returns: {success: bool, message_id: str, error: str (if failed)}
        """


class ReviewProvider(ABC):
    """Review platform management -- fetch reviews, post responses."""

    @abstractmethod
    def get_reviews(self, since: str = "", limit: int = 50) -> list[dict]:
        """Get reviews.

        Standard fields per review:
            id, reviewer_name, star_rating (int 1-5), text, timestamp,
            responded (bool), response_text (str or None)
        """

    @abstractmethod
    def post_response(self, review_id: str, response_text: str) -> dict:
        """Post a response to a review.

        Returns: {success: bool, error: str (if failed)}
        """
