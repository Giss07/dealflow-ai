"""
DealFlow Photo Analyzer — Uses Claude vision to grade property condition zones.
"""

import os
import json
import base64
import logging
import requests

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL = "claude-sonnet-4-20250514"

ZONES = ["Roof", "HVAC", "Plumbing", "Interior", "Kitchen", "Bath", "Foundation"]
GRADES = ["Good", "Fair", "Poor", "Unknown"]


def get_anthropic_client():
    try:
        import anthropic
        return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    except ImportError:
        logger.error("anthropic package not installed")
        raise


def download_image(url, timeout=15):
    """Download image and return base64 encoded data."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "image/jpeg")
        if "jpeg" in content_type or "jpg" in content_type:
            media_type = "image/jpeg"
        elif "png" in content_type:
            media_type = "image/png"
        elif "webp" in content_type:
            media_type = "image/webp"
        else:
            media_type = "image/jpeg"
        return base64.standard_b64encode(resp.content).decode("utf-8"), media_type
    except Exception as e:
        logger.warning(f"Failed to download image {url}: {e}")
        return None, None


def analyze_photos(listing, client=None, max_photos=6):
    """Analyze listing photos and grade each zone."""
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set, returning Unknown grades")
        return {zone: "Unknown" for zone in ZONES}

    if client is None:
        client = get_anthropic_client()

    photos = listing.get("photos", [])
    if not photos:
        logger.info(f"No photos for {listing.get('address', 'Unknown')}")
        return {zone: "Unknown" for zone in ZONES}

    # Download up to max_photos images
    image_content = []
    for url in photos[:max_photos]:
        if not url or not isinstance(url, str):
            continue
        data, media_type = download_image(url)
        if data:
            image_content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": data},
            })

    if not image_content:
        return {zone: "Unknown" for zone in ZONES}

    prompt_text = f"""You are a real estate inspector analyzing property listing photos.

Property: {listing.get('address', 'Unknown')}, built {listing.get('year_built', 'Unknown')}

Grade each of these zones based on what you can see in the photos:
- Roof: Look for missing shingles, sagging, damage, age
- HVAC: Look for visible units, age, condition
- Plumbing: Look for water damage, stains, pipe condition
- Interior: Overall interior condition, walls, floors, paint
- Kitchen: Cabinets, counters, appliances condition
- Bath: Fixtures, tile, vanity condition
- Foundation: Visible cracks, settling, structural issues

Grade each zone as one of: Good, Fair, Poor, Unknown
Use "Unknown" only if that zone is not visible in any photo.

Respond with ONLY a JSON object:
{{"Roof": "grade", "HVAC": "grade", "Plumbing": "grade", "Interior": "grade", "Kitchen": "grade", "Bath": "grade", "Foundation": "grade"}}
"""

    messages_content = image_content + [{"type": "text", "text": prompt_text}]

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": messages_content}],
        )
        text = response.content[0].text.strip()
        grades = json.loads(text)

        # Validate grades
        validated = {}
        for zone in ZONES:
            grade = grades.get(zone, "Unknown")
            validated[zone] = grade if grade in GRADES else "Unknown"
        return validated

    except Exception as e:
        logger.error(f"Photo analysis failed: {e}")
        return {zone: "Unknown" for zone in ZONES}


def analyze_all_photos(listings):
    """Analyze photos for all listings."""
    client = None
    if ANTHROPIC_API_KEY:
        try:
            client = get_anthropic_client()
        except Exception:
            pass

    for i, listing in enumerate(listings):
        logger.info(f"Analyzing photos {i+1}/{len(listings)}: {listing.get('address', 'Unknown')}")
        grades = analyze_photos(listing, client)
        listing["photo_grades"] = grades

        # Calculate overall condition
        grade_scores = {"Good": 3, "Fair": 2, "Poor": 1, "Unknown": 0}
        known_grades = [grade_scores[g] for g in grades.values() if g != "Unknown"]
        if known_grades:
            avg = sum(known_grades) / len(known_grades)
            if avg >= 2.5:
                listing["overall_condition"] = "Good"
            elif avg >= 1.5:
                listing["overall_condition"] = "Fair"
            else:
                listing["overall_condition"] = "Poor"
        else:
            listing["overall_condition"] = "Unknown"

    return listings


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test = {
        "address": "123 Main St",
        "year_built": 1985,
        "photos": [],
    }
    grades = analyze_photos(test)
    print(json.dumps(grades, indent=2))
