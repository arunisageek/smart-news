def normalize_intents(value):
    if not isinstance(value, list):
        return []

    allowed = {"category", "score", "search", "source", "nearby"}
    result = []
    seen = set()

    for item in value:
        if not isinstance(item, str):
            continue

        cleaned = item.strip().lower()
        if cleaned not in allowed:
            continue

        if cleaned in seen:
            continue

        seen.add(cleaned)
        result.append(cleaned)

    return result


def clean_string(value):
    if not isinstance(value, str):
        return None

    cleaned = value.strip()
    return cleaned if cleaned else None


def coerce_float(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    return None


def build_filters(resolved, intents):
    filters = {}

    source = clean_string(resolved.get("source"))
    category = clean_string(resolved.get("category"))
    topic_query = clean_string(resolved.get("topicQuery"))
    location = clean_string(resolved.get("location"))
    radius_km = coerce_float(resolved.get("radiusKm"))
    score_threshold = coerce_float(resolved.get("scoreThreshold"))
    latitude = coerce_float(resolved.get("latitude"))
    longitude = coerce_float(resolved.get("longitude"))

    if "search" in intents and topic_query:
        filters["searchText"] = topic_query

    if "source" in intents and source:
        filters["source"] = source

    if "category" in intents and category:
        filters["category"] = category

    if "score" in intents and score_threshold is not None:
        filters["scoreThreshold"] = score_threshold

    if "nearby" in intents:
        if latitude is not None and longitude is not None:
            filters["latitude"] = latitude
            filters["longitude"] = longitude
            filters["radiusKm"] = radius_km if radius_km is not None else 10.0
        elif location:
            filters["location"] = location
            if radius_km is not None:
                filters["radiusKm"] = radius_km

    return filters


def choose_primary_intent(intents, filters):
    # Prefer intents that define the ranking behavior most strongly.
    if "nearby" in intents and "latitude" in filters and "longitude" in filters:
        return "nearby"

    if "search" in intents and "searchText" in filters:
        return "search"

    if "score" in intents and "scoreThreshold" in filters:
        return "score"

    if "category" in intents and "category" in filters:
        return "category"

    if "source" in intents and "source" in filters:
        return "source"

    # Fallback: if nothing structured was extracted well, do generic search on query text later.
    return "search"


def get_ranking_mode(primary_intent):
    if primary_intent == "nearby":
        return "distance"

    if primary_intent == "search":
        return "search"

    if primary_intent == "score":
        return "score"

    if primary_intent in {"category", "source"}:
        return "recency"

    return "search"


def lambda_handler(event, context):
    query_id = event.get("queryId")
    query_text = event.get("queryText")
    intents = normalize_intents(event.get("intents"))
    resolved = event.get("resolved")

    if not isinstance(query_id, str) or not query_id.strip():
        raise ValueError("queryId is required")

    if not isinstance(query_text, str) or not query_text.strip():
        raise ValueError("queryText is required")

    if not isinstance(resolved, dict):
        raise ValueError("resolved must be an object")

    filters = build_filters(resolved, intents)
    primary_intent = choose_primary_intent(intents, filters)
    ranking_mode = get_ranking_mode(primary_intent)

    return {
        "primaryIntent": primary_intent,
        "rankingMode": ranking_mode,
        "filters": filters
    }