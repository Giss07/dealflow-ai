"""
DealFlow AI Scorer — Uses Claude API to score deals 1-100 on profit potential.
"""

import os
import json
import logging

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL = "claude-sonnet-4-20250514"


def get_anthropic_client():
    """Get Anthropic client."""
    try:
        import anthropic
        return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    except ImportError:
        logger.error("anthropic package not installed. Run: pip install anthropic")
        raise


def build_scoring_prompt(listing):
    """Build the prompt for Claude to score a deal."""
    photo_grades = listing.get("photo_grades", {})
    overall_condition = listing.get("overall_condition", "Unknown")

    return f"""You are a real estate investment analyst specializing in the Inland Empire, CA market.

Score this property listing from 1 to 100 based on its profit potential for a fix-and-flip investor.

IMPORTANT SCORING RULES:
- This is for FIX-AND-FLIP investing. We want DISTRESSED properties that need work.
- Properties that are already renovated, remodeled, updated, or move-in ready are BAD deals (score under 40).
- If photo grades show "Good" condition across most zones, the property is likely already renovated — score UNDER 40.
- If the listing mentions remodeled/renovated/updated/new kitchen/new floors/turnkey, score UNDER 30.
- Best deals (80-100) are clearly distressed: old, needs work, motivated seller, as-is, fixer.

Consider these factors:
- Price relative to area comps (lower = better opportunity)
- Age and likely condition (older homes in this price range = more upside)
- Deal keywords suggesting motivated seller or distressed property
- Location (zip code demand in IE)
- Square footage and lot size value
- Photo condition grades (if available) — Good condition = already fixed = BAD for flip

Property Details:
- Address: {listing.get('address', 'N/A')}, {listing.get('city', '')}, {listing.get('state', 'CA')} {listing.get('zip_code', '')}
- Price: ${listing.get('price', 'N/A'):,}
- Bedrooms: {listing.get('bedrooms', 'N/A')}
- Bathrooms: {listing.get('bathrooms', 'N/A')}
- Sqft: {listing.get('sqft', 'N/A')}
- Year Built: {listing.get('year_built', 'N/A')}
- Home Type: {listing.get('home_type', 'N/A')}
- Has Deal Keywords: {listing.get('has_deal_keywords', False)}
- Matched Keywords: {listing.get('matched_keywords', [])}
- Photo Condition Grades: {photo_grades if photo_grades else 'Not analyzed'}
- Overall Condition: {overall_condition}
- Description: {(listing.get('description', '') or '')[:500]}

Respond with ONLY a JSON object in this exact format:
{{"score": <number 1-100>, "reasoning": "<2-3 sentence explanation>"}}
"""


def score_deal(listing, client=None):
    """Score a single deal using Claude API. Returns score and reasoning."""
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set, using keyword-based scoring fallback")
        return fallback_score(listing)

    if client is None:
        client = get_anthropic_client()

    prompt = build_scoring_prompt(listing)

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()

        # Parse JSON from response
        result = json.loads(text)
        score = int(result.get("score", 50))
        score = max(1, min(100, score))
        reasoning = result.get("reasoning", "")

        return {"score": score, "reasoning": reasoning}

    except json.JSONDecodeError:
        logger.warning(f"Failed to parse Claude response: {text[:200]}")
        return fallback_score(listing)
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return fallback_score(listing)


RENOVATED_PENALTY_KEYWORDS = {
    # -30 each
    "turnkey": -30, "move-in ready": -30, "move in ready": -30,
    "updated": -30, "remodeled": -30, "renovated": -30,
    "fresh": -30, "clean interior": -30,
    "new kitchen": -30, "new bath": -30, "new floors": -30,
    "new flooring": -30, "new roof": -30, "new hvac": -30,
    "fully updated": -30, "brand new": -30,
    # -40 for new construction
    "new construction": -40, "new build": -40, "built in 202": -40,
}


def fallback_score(listing):
    """Smart fallback scoring using ARV gap, $/sqft, days on market, repairs ratio."""
    score = 30  # base score
    reasons = []

    price = listing.get("price", 0) or 0
    arv = listing.get("arv", 0) or 0
    sqft = listing.get("sqft", 0) or 0
    days = listing.get("days_on_zillow")
    repairs_mid = listing.get("repairs_mid", 0) or 0
    repairs_worst = listing.get("repairs_worst", 0) or 0
    repairs = round((repairs_mid + repairs_worst) / 2) if (repairs_mid or repairs_worst) else 0

    # === ARV vs Price gap (30 points max) ===
    if arv and price and arv > 0:
        margin = (arv - price) / arv
        if margin >= 0.30:
            score += 30
            reasons.append(f"Strong ARV margin ({margin:.0%})")
        elif margin >= 0.20:
            score += 20
            reasons.append(f"Good ARV margin ({margin:.0%})")
        elif margin >= 0.10:
            score += 10
            reasons.append(f"Moderate ARV margin ({margin:.0%})")
        elif margin < 0:
            score -= 20
            reasons.append(f"Negative margin ({margin:.0%})")

    # === Price per sqft (20 points max) ===
    if price and sqft and sqft > 0:
        ppsf = price / sqft
        if ppsf < 200:
            score += 20
            reasons.append(f"Low $/sqft (${ppsf:.0f})")
        elif ppsf < 300:
            score += 10
            reasons.append(f"Moderate $/sqft (${ppsf:.0f})")
        elif ppsf > 400:
            score -= 10
            reasons.append(f"High $/sqft (${ppsf:.0f})")

    # === Days on market (15 points max) ===
    if days is not None:
        if days >= 30:
            score += 15
            reasons.append(f"Stale listing ({days} days)")
        elif days >= 15:
            score += 10
            reasons.append(f"On market {days} days")
        elif days >= 1:
            score += 5

    # === Repairs vs ARV ratio (15 points max) ===
    if repairs and arv and arv > 0:
        repair_ratio = repairs / arv
        if repair_ratio < 0.10:
            score += 15
            reasons.append(f"Low repair ratio ({repair_ratio:.0%})")
        elif repair_ratio < 0.20:
            score += 10
            reasons.append(f"Moderate repairs ({repair_ratio:.0%})")
        elif repair_ratio > 0.30:
            score -= 10
            reasons.append(f"Heavy repairs ({repair_ratio:.0%})")

    # === Price bonus (10 points max, reduced weight) ===
    if price < 300000:
        score += 10
    elif price < 500000:
        score += 5

    # === Year built bonus (5 points max, reduced weight) ===
    year = listing.get("year_built")
    if year and year < 1980:
        score += 5
    elif year and year < 2000:
        score += 3

    # === Deal keywords bonus (10 points max) ===
    if listing.get("has_deal_keywords"):
        score += 7
    keywords = listing.get("matched_keywords", [])
    score += min(len(keywords) * 2, 3)

    # === Renovation penalty ===
    desc = (listing.get("description") or "").lower()
    for kw, penalty in RENOVATED_PENALTY_KEYWORDS.items():
        if kw in desc:
            score += penalty  # penalty is negative
            reasons.append(f"Penalty: '{kw}'")
            break

    # === Photo grades penalty ===
    grades = listing.get("photo_grades", {})
    good_count = sum(1 for g in grades.values() if g == "Good")
    if good_count >= 4:
        score -= 20
        reasons.append("Photos show renovated condition")

    score = max(1, min(100, score))
    reasoning = ". ".join(reasons[:4]) if reasons else "Scored on price, location, and market data."
    return {"score": score, "reasoning": reasoning}


def score_deals(listings):
    """Score all deals. Returns listings with scores added."""
    client = None
    if ANTHROPIC_API_KEY:
        try:
            client = get_anthropic_client()
        except Exception:
            pass

    scored = []
    for i, listing in enumerate(listings):
        logger.info(f"Scoring deal {i+1}/{len(listings)}: {listing.get('address', 'Unknown')}")
        result = score_deal(listing, client)
        listing["score"] = result["score"]
        listing["score_reasoning"] = result["reasoning"]
        scored.append(listing)

    scored.sort(key=lambda x: x.get("score", 0), reverse=True)
    logger.info(f"Scored {len(scored)} deals. Top score: {scored[0]['score'] if scored else 'N/A'}")
    return scored


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test = {
        "address": "123 Main St", "city": "Fontana", "state": "CA", "zip_code": "92335",
        "price": 450000, "bedrooms": 3, "bathrooms": 2, "sqft": 1500,
        "year_built": 1985, "home_type": "SINGLE_FAMILY", "days_on_zillow": 45,
        "has_deal_keywords": True, "matched_keywords": ["fixer", "as-is"],
        "description": "Great fixer opportunity, sold as-is. Investor special!",
    }
    result = score_deal(test)
    print(f"Score: {result['score']}/100")
    print(f"Reasoning: {result['reasoning']}")
