"""
Americal Patrol - SEO Blog Generator
Uses the Claude API to generate keyword-optimized blog posts targeting
Ventura County cities and specific account types (commercial, HOA,
industrial, retail).

SEO guidelines built into every post:
  - Target keyword in the first sentence
  - 1,800-2,200 words
  - 3 question-based H2 subheadings (optimized for People Also Ask)
  - 2-3 citations from approved .gov/.org sources only
  - City name used 5-8 times naturally
  - Ventura County referenced at least 3 times
  - CTA highlighting 35+ years experience and veteran ownership
"""

import json
import os
from pathlib import Path

import anthropic

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'blog_config.json'


def _get_anthropic_api_key() -> str:
    key = os.environ.get('ANTHROPIC_API_KEY')
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not set. Add it to the .env file in the project root.")
    return key

ACCOUNT_TYPE_CONTEXT = {
    "Commercial": {
        "description": "commercial office buildings, business parks, and corporate properties",
        "concerns": "unauthorized access, after-hours intrusion, vandalism, parking enforcement, and liability reduction",
        "audience": "property managers, building owners, and facility directors",
        "local_hook": {
            "Port Hueneme": "home to significant commercial activity serving the naval base community",
            "Santa Paula":  "a growing commercial corridor along Highway 126",
            "Ojai":         "a thriving downtown with boutique retail and professional offices",
            "Camarillo":    "a business-friendly city with major commercial corridors along Daily Drive",
            "Oxnard":       "one of Ventura County's largest commercial centers",
            "Ventura":      "a vibrant downtown and Harbor area with diverse commercial properties",
        }
    },
    "HOA": {
        "description": "homeowners associations, gated communities, and residential complexes",
        "concerns": "gate access control, parking enforcement, vandalism, trespassing, and resident safety",
        "audience": "HOA board members, community managers, and property management companies",
        "local_hook": {
            "Port Hueneme": "a coastal community with several established residential associations",
            "Santa Paula":  "a historic city with growing residential communities",
            "Ojai":         "an upscale community with privacy-conscious residents",
            "Camarillo":    "one of Ventura County's fastest-growing residential markets",
            "Oxnard":       "home to some of Ventura County's largest HOA communities",
            "Ventura":      "a coastal city with premium residential developments",
        }
    },
    "Industrial": {
        "description": "warehouses, manufacturing facilities, distribution centers, and industrial parks",
        "concerns": "equipment theft, cargo security, perimeter monitoring, after-hours intrusion, and worker safety",
        "audience": "operations managers, facility directors, and logistics company owners",
        "local_hook": {
            "Port Hueneme": "home to a major deep-water port and significant warehousing and logistics operations",
            "Santa Paula":  "an agricultural and light industrial zone with growing storage and distribution activity",
            "Ojai":         "surrounded by agricultural operations requiring perimeter and equipment security",
            "Camarillo":    "a major industrial and logistics hub with significant warehouse space along Las Posas Road",
            "Oxnard":       "Ventura County's largest industrial corridor, including the Oxnard Business Park and Port of Hueneme area",
            "Ventura":      "an active industrial zone near the harbor with manufacturing and distribution facilities",
        }
    },
    "Retail": {
        "description": "retail stores, shopping centers, strip malls, and restaurants",
        "concerns": "shoplifting, organized retail crime, customer and employee safety, and loss prevention",
        "audience": "retail store owners, shopping center managers, and loss prevention directors",
        "local_hook": {
            "Port Hueneme": "a neighborhood retail market serving the local naval and residential community",
            "Santa Paula":  "a revitalized downtown retail district with growing foot traffic",
            "Ojai":         "a destination shopping and dining corridor that draws visitors from across Ventura County",
            "Camarillo":    "home to the Camarillo Premium Outlets and significant retail density",
            "Oxnard":       "a major retail market including The Collection at RiverPark and numerous shopping centers",
            "Ventura":      "a thriving retail and restaurant scene anchored by downtown and the Pacific View Mall area",
        }
    }
}


def _build_prompt(city: str, account_type: str, approved_sources: list,
                  keyword_intel: dict | None = None) -> str:
    ctx = ACCOUNT_TYPE_CONTEXT[account_type]
    local_hook = ctx["local_hook"].get(city, f"a key city in Ventura County")
    sources_text = "\n".join(
        f'  - {s["name"]}: {s["url"]} ({s["use_for"]})'
        for s in approved_sources
    )

    # Build keyword intelligence section if real analytics data is available
    kw_intel_section = ""
    if keyword_intel:
        secondary_kws = keyword_intel.get("secondary_keywords", [])
        impressions   = keyword_intel.get("impressions", 0)
        position      = keyword_intel.get("position", "N/A")
        sec_kw_lines  = "\n".join(f'  * "{kw}"' for kw in secondary_kws) if secondary_kws else "  (none identified)"
        kw_intel_section = f"""

REAL SEARCH DATA FROM GOOGLE ANALYTICS (use this to inform the post):
- This topic currently has {impressions} monthly impressions in Google Search but ranks at position {position}.
  Your post needs to be comprehensive and authoritative enough to push into the top 5.
- Secondary keyword variations people are actually searching — weave each in naturally 1-2 times:
{sec_kw_lines}
- Do NOT force these keywords in awkwardly. Use them only where they fit naturally in context.
"""

    return f"""You are a professional SEO content writer for Americal Patrol, Inc., a veteran-owned security patrol company based in Ventura County, California, that has been serving Southern California since 1986.{kw_intel_section}

Write a complete, SEO-optimized blog post targeting the following:
- PRIMARY KEYWORD: "{account_type.lower()} security patrol {city} CA"
- CITY: {city}, California
- ACCOUNT TYPE: {ctx["description"]}
- LOCAL CONTEXT: {city} is {local_hook}
- TARGET AUDIENCE: {ctx["audience"]}
- KEY SECURITY CONCERNS: {ctx["concerns"]}

STRICT SEO REQUIREMENTS — follow every one of these exactly:

1. FIRST SENTENCE: The very first sentence of the blog body must naturally contain the phrase "{account_type.lower()} security" and "{city}" (or "{city}, CA"). Answer the reader's search query immediately.

2. LENGTH: The blog body must be between 1,800 and 2,200 words. Count carefully.

3. HEADINGS: Include exactly 3 H2 subheadings. Each must be phrased as a question that a property manager or business owner in {city} would actually search on Google. Examples of the format (create original ones):
   - "Why Do [Account Type] Properties in {city} Need Professional Security Patrol?"
   - "What Should [Audience] in {city} Look for in a Licensed Security Company?"
   - "How Does Mobile Patrol Reduce Risk for [Account Type] in {city}, CA?"

4. CITY MENTIONS: Use "{city}" naturally between 5 and 8 times throughout the post. Do not stuff it awkwardly.

5. VENTURA COUNTY: Mention "Ventura County" at least 3 times naturally.

6. CITATIONS: Include exactly 2 or 3 citations from this approved source list ONLY. Do NOT cite any commercial websites (.com), competitor security companies, or industry associations. Only use these:
{sources_text}

   Format citations as natural in-text references like: "According to the California Department of Justice (oag.ca.gov/crime), property crimes in California..." Then include the URL as a hyperlink anchor in the HTML.

7. NO COMPETITOR MENTIONS: Never name or link to any other security company. Never reference any commercial (.com) website as a source.

8. INTERNAL LINKS: Naturally link to these Americal Patrol pages within the body of the post (not just the CTA). Use descriptive anchor text — never "click here":
   - Service-specific page for this post's account type:
     * Commercial: <a href="https://americalpatrol.com/commercial">commercial security services</a>
     * HOA: <a href="https://americalpatrol.com/hoa">HOA security patrol</a>
     * Industrial: <a href="https://americalpatrol.com/industrial">industrial security services</a>
     * Retail: <a href="https://americalpatrol.com/retail">retail security patrol</a>
   - At least 2 of these additional pages, placed naturally in context:
     * <a href="https://americalpatrol.com/patrol-services">mobile patrol services</a>
     * <a href="https://americalpatrol.com/guard-services">guard services</a>
     * <a href="https://americalpatrol.com/service-areas">service areas in Ventura County</a>
     * <a href="https://americalpatrol.com/about-us">about Americal Patrol</a>
     * <a href="https://americalpatrol.com/faqs">security service FAQs</a>

9. CTA (CALL TO ACTION): The final paragraph must be a strong call to action that:
   - Mentions Americal Patrol by name
   - References "over 35 years" or "since 1986"
   - References veteran-owned
   - Invites readers to contact Americal Patrol for a free security assessment
   - Includes the phone number (805) 515-3834 as a clickable tel: link: <a href="tel:8055153834">(805) 515-3834</a>
   - Includes a clickable link to the contact form: <a href="https://americalpatrol.com/contact-us">Request a Free Security Assessment</a>

10. TONE: Professional, informative, and trustworthy. Written for the target audience, not for Google bots. No keyword stuffing.

11. E-E-A-T SIGNALS: Weave in real operational language — terms like PPO (Private Patrol Operator), mobile patrol routes, post orders, security assessment, incident reporting, deterrence, and response time — to demonstrate genuine expertise.

12. LINK SUMMARY: Every post must contain at least 4 internal links total: 1 service-specific page + 2 additional site pages + 1 contact-us CTA link.

Return your response as a single JSON object with these exact keys:
{{
  "title": "SEO-optimized blog title (include {city} and {account_type.lower()} security)",
  "slug": "url-friendly-slug-with-hyphens-no-special-chars",
  "meta_description": "150-160 character meta description including the primary keyword",
  "html_content": "Complete blog post HTML using <h2>, <p>, <strong>, <a href=\\"..\\"> tags. No <html>, <head>, or <body> wrappers."
}}

Return ONLY the JSON object. No markdown code fences, no explanation before or after.
"""


BLOG_POST_TOOL = {
    "name": "submit_blog_post",
    "description": "Submit the completed SEO blog post with all required fields.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "description": "SEO-optimized blog title including the city and account type keyword"
            },
            "slug": {
                "type": "string",
                "description": "URL-friendly slug with hyphens only, no special characters"
            },
            "meta_description": {
                "type": "string",
                "description": "150-160 character meta description including the primary keyword"
            },
            "html_content": {
                "type": "string",
                "description": "Complete blog post HTML using h2, p, strong, and anchor tags. No html/head/body wrappers."
            }
        },
        "required": ["title", "slug", "meta_description", "html_content"]
    }
}


def generate_blog_post(city: str, account_type: str) -> dict:
    """
    Generate a complete SEO blog post for the given city and account type.
    Uses Claude tool use to guarantee valid structured output.
    Returns a dict with: title, slug, meta_description, html_content
    """
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)

    approved_sources = config.get('approved_sources', [])
    topic_key    = f"{city}_{account_type}"
    keyword_intel = config.get('keyword_intelligence', {}).get(topic_key)
    prompt = _build_prompt(city, account_type, approved_sources, keyword_intel=keyword_intel)

    client = anthropic.Anthropic(api_key=_get_anthropic_api_key())
    response = client.messages.create(
        model='claude-sonnet-4-6',
        max_tokens=4096,
        tools=[BLOG_POST_TOOL],
        tool_choice={"type": "tool", "name": "submit_blog_post"},
        messages=[{'role': 'user', 'content': prompt}]
    )

    # Extract the tool use result — always valid, no JSON parsing issues
    for block in response.content:
        if block.type == 'tool_use' and block.name == 'submit_blog_post':
            return block.input

    raise ValueError("Claude did not return a tool use response. Try running again.")


if __name__ == '__main__':
    # Quick test — generates Week 1 post and prints the title + first 500 chars
    post = generate_blog_post('Port Hueneme', 'Industrial')
    print(f"Title: {post['title']}")
    print(f"Slug:  {post['slug']}")
    print(f"Meta:  {post['meta_description']}")
    print(f"\n--- HTML preview (first 500 chars) ---")
    print(post['html_content'][:500])
