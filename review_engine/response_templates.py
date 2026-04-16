"""
Review Response Templates
System prompts for Claude to generate review responses
tailored to star rating and brand voice.
"""


def get_response_prompt(
    star_rating: int,
    reviewer_name: str,
    review_text: str,
    company_name: str,
    company_phone: str,
    tone: str = "professional_warm",
    company_description: str = "",
) -> str:
    tone_guide = {
        "professional_warm": "Professional but warm and genuine. Not corporate or stiff.",
        "casual_friendly": "Casual and friendly, like a neighbor. Use first names.",
        "formal": "Polite and formal. Suitable for professional services.",
    }
    tone_desc = tone_guide.get(tone, tone_guide["professional_warm"])

    rating_instructions = {
        5: (
            "This is a 5-star review. The customer is very happy. "
            "Thank them sincerely. Reference something specific they mentioned. "
            "End with a warm invitation to come back or refer friends/family."
        ),
        4: (
            "This is a 4-star review. The customer is mostly satisfied. "
            "Thank them warmly. If they mentioned any area for improvement, "
            "acknowledge it briefly and mention your commitment to getting better. "
            "Invite them back."
        ),
        3: (
            "This is a 3-star review. The customer had a mixed experience. "
            "Thank them for the honest feedback. Empathize with their concern. "
            "Offer to make it right. Provide a phone number or email to continue "
            "the conversation privately."
        ),
        2: (
            "This is a 2-star review. The customer is disappointed. "
            "Empathize first. Apologize for falling short. Do NOT be defensive. "
            "Ask them to contact you directly so you can make it right. "
            f"Provide the phone number: {company_phone}"
        ),
        1: (
            "This is a 1-star review. The customer is very unhappy. "
            "Lead with empathy and a sincere apology. Do NOT argue or explain. "
            "Take responsibility. Ask them to reach out directly. "
            f"Provide the phone number: {company_phone}. "
            "Keep it short -- long responses to 1-star reviews look defensive."
        ),
    }

    instruction = rating_instructions.get(star_rating, rating_instructions[3])

    return (
        f"You are writing a Google review response on behalf of {company_name}. "
        f"{company_description}\n\n"
        f"Tone: {tone_desc}\n"
        f"No em dashes. No exclamation marks more than once. No generic phrases like "
        f"'We appreciate your feedback' or 'Your satisfaction is our priority'. "
        f"Sound like a real person, not a corporate template.\n\n"
        f"{instruction}\n\n"
        f"Reviewer name: {reviewer_name}\n"
        f"Review text: {review_text}\n\n"
        f"Write ONLY the response text. No greeting like 'Dear {reviewer_name}' -- "
        f"Google review responses don't start that way. Just start naturally. "
        f"Keep it under 150 words."
    )
