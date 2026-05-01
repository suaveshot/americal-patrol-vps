"""
Americal Patrol — Social Media Content Generator
Uses Claude API to generate unique, platform-specific social media posts.

Each platform gets its own independent generation call with a platform-specific
system prompt and content strategy. Posts are NOT reworded versions of each other —
they are completely different pieces of content.
"""

import json
import os
from pathlib import Path

import anthropic

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "social_config.json"


def _load_config() -> dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


PLATFORM_PROMPTS = {
    "facebook": """You are a social media manager for Americal Patrol, Inc., a veteran-owned
security patrol company in Oxnard, CA serving Ventura County since 1986.

Write a Facebook post. Facebook style guidelines:
- Community-focused, approachable tone
- 150-400 characters (short and engaging)
- Can include a link if promoting a blog post
- Use questions, polls, "did you know" formats to drive engagement
- Focus on local community safety and neighborhood awareness
- End with a clear call-to-action when appropriate
- NO hashtags on Facebook (they hurt reach)""",

    "instagram": """You are a social media manager for Americal Patrol, Inc., a veteran-owned
security patrol company in Oxnard, CA serving Ventura County since 1986.

Write an Instagram caption. Instagram style guidelines:
- Visual-first platform — describe what the accompanying image should show
- Caption: 100-300 characters for the main message
- Add a line break, then 10-15 relevant hashtags
- Mix broad hashtags (#SecurityGuard) with local (#OxnardCA, #VenturaCounty)
- Polished, professional visual storytelling tone
- No clickable links in body — use "Link in bio" for CTAs
- Use line breaks for readability""",

    "linkedin": """You are a social media manager for Americal Patrol, Inc., a veteran-owned
security patrol company in Oxnard, CA serving Ventura County since 1986.

Write a LinkedIn post. LinkedIn style guidelines:
- Professional, authoritative thought leadership tone
- 200-600 characters
- Target audience: property managers, HOA boards, commercial real estate professionals
- Data-driven when possible (reference public crime stats, industry trends)
- Position Americal Patrol as an industry authority
- Can include links for blog promotion
- Use 3-5 professional hashtags at the end""",

    "gbp": """You are writing a Google Business Profile "What's New" post for Americal Patrol, Inc.,
a veteran-owned, licensed security patrol company serving Ventura County, California since 1986.

GBP post requirements:
- Plain text only — no HTML, no markdown, no hashtags, no bullet points, no asterisks
- Maximum 1,500 characters (Google enforces this hard limit)
- Mention "Americal Patrol" by name at least once
- Reference Ventura County, Oxnard, Camarillo, or another specific local city naturally
- Weave in "veteran-owned" or "since 1986" to build credibility
- End with a clear call to action
- Professional, trustworthy tone — not salesy or generic
- No competitor names
- No hashtags (they are not clickable on Google Business Profile)""",
}


SUBMIT_POST_TOOL = {
    "name": "submit_social_post",
    "description": "Submit the generated social media post content.",
    "input_schema": {
        "type": "object",
        "properties": {
            "post_text": {
                "type": "string",
                "description": "The complete post text including any hashtags.",
            },
            "image_prompt": {
                "type": "string",
                "description": (
                    "A detailed prompt for generating a professional, realistic AI image "
                    "to accompany this post. Describe a security-themed scene: patrol vehicles, "
                    "professional guards, well-lit commercial properties, residential communities. "
                    "NEVER include weapons, violence, surveillance footage, or anything threatening. "
                    "Empty string if no image is needed for this post."
                ),
            },
            "image_tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "3-5 short tags describing the image for the image library catalog "
                    "(e.g., 'patrol_vehicle', 'commercial_property', 'night_security'). "
                    "Empty array if no image."
                ),
            },
        },
        "required": ["post_text", "image_prompt", "image_tags"],
    },
}


def generate_post(platform: str, plan: dict, brand: dict,
                  seo_context: dict, log=None) -> dict:
    """
    Generate a unique social media post for a single platform.

    Args:
        platform: "facebook", "instagram", or "linkedin"
        plan: The platform-specific plan from content_planner
        brand: Brand guidelines from social_config.json
        seo_context: SEO trending keywords and topics
        log: Optional logging function

    Returns:
        {"post_text": str, "image_prompt": str, "image_tags": list[str]}
    """
    config = _load_config()

    system_prompt = PLATFORM_PROMPTS.get(platform, PLATFORM_PROMPTS["facebook"])

    # Build the user message with context
    content_type = plan.get("content_type", "general")
    description  = plan.get("description", "")
    context      = plan.get("context", {})
    max_chars    = plan.get("max_chars", 2000)

    user_parts = [
        f"Content type: {content_type}",
        f"Description: {description}",
        f"Max characters: {max_chars}",
        "",
        "Brand info:",
        f"  Company: {brand.get('company_name', 'Americal Patrol, Inc.')}",
        f"  Tagline: {brand.get('tagline', '')}",
        f"  Location: {brand.get('location', 'Oxnard, CA')}",
        f"  Phone: {brand.get('phone', '')}",
        f"  Website: {brand.get('website', '')}",
        f"  Service areas: {', '.join(brand.get('service_areas', []))}",
        f"  Cities: {', '.join(brand.get('cities', []))}",
    ]

    # Add SEO context for keyword-aware content
    if seo_context.get("top_keywords"):
        user_parts.append(f"\nTrending search keywords: {', '.join(seo_context['top_keywords'][:5])}")
    if seo_context.get("trending_topics"):
        user_parts.append(f"Rising topics: {', '.join(seo_context['trending_topics'][:3])}")

    # Add blog event context if this is a blog promotion slot
    blog_event = context.get("blog_event")
    if blog_event:
        user_parts.extend([
            "",
            "NEW BLOG POST to promote:",
            f"  Title: {blog_event.get('title', '')}",
            f"  URL: {brand.get('website', '')}/blog/{blog_event.get('slug', '')}",
            f"  Topic: {blog_event.get('account_type', '')} security in {blog_event.get('city', '')}",
        ])

    # Add seasonal context
    seasonal = context.get("seasonal_event")
    if seasonal:
        user_parts.extend([
            "",
            f"SEASONAL EVENT: {seasonal['name']}",
            f"Description: {seasonal['description']}",
            f"Days until event: {seasonal['days_until']}",
            "Create a themed post for this event.",
        ])

    # Instagram always needs an image
    if platform == "instagram":
        user_parts.append("\nThis post MUST have an accompanying image. Include a detailed image_prompt.")

    # GBP: no image, include subject keyword and CTA URL
    if platform == "gbp":
        user_parts.append("\nThis is a Google Business Profile post. No image is needed.")
        user_parts.append("Set image_prompt to an empty string and image_tags to an empty array.")
        subject = plan.get("subject", "")
        if subject:
            user_parts.append(f"\nTarget subject/keyword: {subject}")
        gbp_cta = config.get("platforms", {}).get("gbp", {}).get("post_cta_url", "")
        if gbp_cta:
            user_parts.append(f"End with a call to action pointing readers to: {gbp_cta}")

    user_message = "\n".join(user_parts)

    if log:
        log(f"  Generating {platform} post: {content_type}...")

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=system_prompt,
        tools=[SUBMIT_POST_TOOL],
        tool_choice={"type": "tool", "name": "submit_social_post"},
        messages=[{"role": "user", "content": user_message}],
    )

    # Extract tool use result
    for block in response.content:
        if block.type == "tool_use" and block.name == "submit_social_post":
            result = block.input
            if log:
                text_preview = result["post_text"][:80].replace("\n", " ")
                log(f"  {platform} post generated: {text_preview}...")
            return {
                "post_text": result["post_text"],
                "image_prompt": result.get("image_prompt", ""),
                "image_tags": result.get("image_tags", []),
            }

    raise RuntimeError(f"Claude did not return a tool_use response for {platform}")


def generate_all_posts(plans: dict, log=None) -> dict:
    """
    Generate unique posts for all enabled platforms.

    Args:
        plans: Full plan dict from content_planner.plan_posts()
        log: Optional logging function

    Returns:
        {
            "facebook": {"post_text": ..., "image_prompt": ..., "image_tags": ...},
            "instagram": {"post_text": ..., "image_prompt": ..., "image_tags": ...},
            "linkedin": {"post_text": ..., "image_prompt": ..., "image_tags": ...},
        }
    """
    config = _load_config()
    brand  = config.get("brand", {})
    seo_context = plans.get("seo_context", {})

    results = {}
    for platform in ["facebook", "instagram", "linkedin", "gbp"]:
        if platform not in plans:
            continue

        try:
            result = generate_post(
                platform=platform,
                plan=plans[platform],
                brand=brand,
                seo_context=seo_context,
                log=log,
            )
            results[platform] = result
        except Exception as e:
            if log:
                log(f"  ERROR generating {platform} post: {e}")
            results[platform] = None

    return results
