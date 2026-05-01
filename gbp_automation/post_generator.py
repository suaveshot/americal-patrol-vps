"""
Americal Patrol - GBP Post Generator
Uses Claude API to draft a "What's New" Google Business Profile post.
Max 1500 characters. Plain text only. Ends with a call to action.
"""

import json
import os
from pathlib import Path

import anthropic

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'gbp_config.json'


def _get_anthropic_api_key() -> str:
    key = os.environ.get('ANTHROPIC_API_KEY')
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not set. Add it to the .env file in the project root.")
    return key


GBP_POST_TOOL = {
    "name": "submit_gbp_post",
    "description": "Submit a completed Google Business Profile What's New post.",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": (
                    "Post body text. Plain text only — no HTML, no markdown, no hashtags, "
                    "no bullet points, no asterisks. Must be under 1500 characters total. "
                    "Must mention Americal Patrol by name at least once. "
                    "Must reference Ventura County or a specific local city. "
                    "Must end with a clear call to action."
                )
            }
        },
        "required": ["summary"]
    }
}

_TYPE_GUIDANCE = {
    'company_update':    "Write a company update highlighting Americal Patrol's services, expertise, or community presence.",
    'security_tip':      "Share a practical, actionable security tip for businesses or property owners in Ventura County.",
    'local_news':        "Write a post about a local safety or security topic relevant to the Oxnard / Ventura County area.",
    'service_highlight': "Highlight a specific Americal Patrol service and its concrete benefits for local clients.",
    'seo_priority':      "Write a locally relevant post targeting search visibility for the given subject.",
}


def _build_prompt(topic: dict) -> str:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)

    topic_type = topic.get('type', 'company_update')
    subject    = topic.get('subject', 'security services Ventura County')
    cta_url    = config.get('post_cta_url', 'https://americalpatrol.com/contact-us')
    guidance   = _TYPE_GUIDANCE.get(topic_type, _TYPE_GUIDANCE['company_update'])

    return f"""You are writing a Google Business Profile "What's New" post for Americal Patrol, Inc. — a veteran-owned, licensed security patrol company serving Ventura County, California since 1986.

POST TOPIC: {subject}
POST TYPE: {guidance}

STRICT REQUIREMENTS:
1. Plain text only — no HTML, no markdown, no hashtags, no bullet points, no asterisks
2. Maximum 1,500 characters total (count carefully — Google enforces this hard limit)
3. Mention "Americal Patrol" by name at least once
4. Reference Ventura County, Oxnard, Camarillo, or another specific local city naturally
5. Weave in "veteran-owned" or "since 1986" to build credibility
6. End with a clear call to action pointing readers to: {cta_url}
7. Professional, trustworthy tone — not salesy or generic
8. No competitor names
9. No hashtags (they are not clickable on Google Business Profile)

Write the post now."""


def generate_post(topic: dict) -> dict:
    """
    Generate a GBP What's New post for the given topic dict.
    Returns: {'summary': str}
    """
    prompt = _build_prompt(topic)
    client = anthropic.Anthropic(api_key=_get_anthropic_api_key())
    response = client.messages.create(
        model='claude-sonnet-4-6',
        max_tokens=600,
        tools=[GBP_POST_TOOL],
        tool_choice={"type": "tool", "name": "submit_gbp_post"},
        messages=[{'role': 'user', 'content': prompt}]
    )

    for block in response.content:
        if block.type == 'tool_use' and block.name == 'submit_gbp_post':
            return block.input

    raise ValueError("Claude did not return a tool use response.")
