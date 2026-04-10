"""
Email Assistant (Larry) — Feedback Parser
Parses Sam's reply to an escalation email and determines the intended action.
"""

import re


# Patterns that indicate "send the proposed response as-is"
_SEND_PATTERNS = [
    r"^\s*1\s*$",
    r"\bsend\s+as[\s-]?is\b",
    r"\bsend\s+it\b",
    r"\blooks?\s+good\b",
    r"\bapproved?\b",
    r"\bgo\s+ahead\b",
    r"\byes\b",
    r"\bperfect\b",
    r"\bthat\s*(?:'s|s)?\s*(?:fine|good|great)\b",
]

# Patterns that indicate "skip / I'll handle it"
_SKIP_PATTERNS = [
    r"^\s*3\s*$",
    r"\bskip\b",
    r"\bi(?:'ll| will)\s+handle\b",
    r"\bignore\b",
    r"\bno\s+action\b",
    r"\bdon(?:'t|t)\s+(?:send|reply|respond)\b",
    r"\bi(?:'ll| will)\s+(?:take\s+care|deal\s+with|respond|reply)\b",
]


def parse_sam_response(reply_body):
    """
    Parse Sam's reply to determine what action to take.

    Returns dict with:
        action: "send_proposed" | "send_custom" | "skip" | "draft"
        custom_body: str (only if action is "send_custom")
        raw_text: str (the extracted reply text, for logging)
    """
    # Extract just Sam's new text (above the quoted reply)
    new_text = _extract_new_text(reply_body)

    if not new_text.strip():
        # Empty reply — can't determine intent, fail safe
        return {"action": "draft", "raw_text": "", "reason": "empty reply"}

    clean = new_text.strip()

    # Check for send-as-is patterns
    for pattern in _SEND_PATTERNS:
        if re.search(pattern, clean, re.IGNORECASE | re.MULTILINE):
            return {"action": "send_proposed", "raw_text": clean}

    # Check for skip patterns
    for pattern in _SKIP_PATTERNS:
        if re.search(pattern, clean, re.IGNORECASE | re.MULTILINE):
            return {"action": "skip", "raw_text": clean}

    # Check for "2" (modify) — if it's just "2", Sam probably wants to provide edits
    # but the actual edits might be in the next line or the reply itself
    if re.match(r"^\s*2\s*$", clean, re.MULTILINE):
        # Check if there's more text after the "2"
        lines = [l.strip() for l in clean.split("\n") if l.strip()]
        if len(lines) > 1:
            # Text after "2" is the custom response
            custom = "\n".join(lines[1:])
            return {"action": "send_custom", "custom_body": custom, "raw_text": clean}
        else:
            # Just "2" with no edits — fail safe to draft
            return {"action": "draft", "raw_text": clean, "reason": "option 2 selected but no edits provided"}

    # If substantial text (more than a short phrase), treat as custom response
    word_count = len(clean.split())
    if word_count >= 8:
        return {"action": "send_custom", "custom_body": clean, "raw_text": clean}

    # Short ambiguous text — fail safe to draft
    return {"action": "draft", "raw_text": clean, "reason": "ambiguous reply"}


def _extract_new_text(body):
    """
    Extract only the new text Sam wrote, stripping quoted content.

    Gmail-style quoting markers:
    - Lines starting with ">"
    - "On ... wrote:" line (start of quote block)
    - "---------- Forwarded message ----------"
    - "--- REFERENCE (do not edit below this line) ---"
    """
    lines = body.split("\n")
    new_lines = []

    for line in lines:
        # Stop at quote markers
        stripped = line.strip()

        # Gmail "On <date> <person> wrote:" pattern
        if re.match(r"^On\s+.+wrote:\s*$", stripped):
            break

        # Outlook-style "From: ... Sent: ..."
        if re.match(r"^-+\s*Original Message\s*-+", stripped, re.IGNORECASE):
            break

        # Our reference block
        if stripped.startswith("--- REFERENCE"):
            break

        # Our escalation block markers
        if stripped.startswith("--- ORIGINAL EMAIL ---"):
            break
        if stripped.startswith("--- MY ANALYSIS ---"):
            break
        if stripped.startswith("--- PROPOSED RESPONSE ---"):
            break
        if stripped.startswith("--- WHAT I NEED ---"):
            break

        # Standard quote prefix
        if stripped.startswith(">"):
            break

        # Gmail forwarded message
        if "Forwarded message" in stripped and stripped.startswith("-"):
            break

        new_lines.append(line)

    return "\n".join(new_lines).strip()
