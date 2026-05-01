"""
Americal Patrol — AI Image Generator
Uses Google Gemini API (Nano Banana / gemini-2.5-flash-image) to generate
professional security-themed images for social media posts.

Safety constraints are built into every prompt:
  - Professional, well-lit scenes only
  - No weapons, violence, surveillance footage, or threatening imagery
  - Security guards in professional uniforms
  - Clean, modern commercial/residential properties
"""

import os
from datetime import datetime
from io import BytesIO
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent


def generate_image(prompt: str, log=None) -> tuple[bytes, str]:
    """
    Generate an image using Google Gemini API.

    Args:
        prompt: Image description prompt (from content_generator)
        log: Optional logging function

    Returns:
        Tuple of (image_bytes, filename)
    """
    from google import genai
    from google.genai import types

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set in .env")

    # Wrap the prompt with safety constraints
    safe_prompt = (
        f"{prompt}\n\n"
        "Style: Professional, photorealistic, well-lit, high-quality photograph. "
        "The scene should look like a real professional security company's marketing photo. "
        "DO NOT include any weapons, violence, surveillance footage, threatening imagery, "
        "or anything that could be perceived as aggressive. "
        "Focus on safety, professionalism, and community trust."
    )

    if log:
        log(f"  Calling Gemini API for image generation...")

    client = genai.Client(api_key=api_key)

    response = client.models.generate_content(
        model="gemini-2.5-flash-image",
        contents=safe_prompt,
        config=types.GenerateContentConfig(
            response_modalities=["Image"],
        ),
    )

    # Extract image from response
    for part in response.candidates[0].content.parts:
        if part.inline_data is not None:
            image_bytes = part.inline_data.data

            # Generate a unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            ext = "png" if "png" in part.inline_data.mime_type else "jpg"
            filename = f"ai_{timestamp}.{ext}"

            if log:
                log(f"  Image generated: {filename} ({len(image_bytes)} bytes)")

            return image_bytes, filename

    raise RuntimeError("Gemini API did not return an image in the response")
