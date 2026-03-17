def normalize_candidates(value):
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


def clean_string_list(value):
    if not isinstance(value, list):
        return []

    result = []
    seen = set()

    for item in value:
        if not isinstance(item, str):
            continue

        cleaned = item.strip()
        if not cleaned:
            continue

        lowered = cleaned.lower()
        if lowered in seen:
            continue

        seen.add(lowered)
        result.append(cleaned)

    return result


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


def first_or_none(values):
    if not values:
        return None
    return values[0]


def build_filters(entities, search_query, score_threshold, location, radius_km):
    if not isinstance(entities, dict):
        entities = {}

    sources = clean_string_list(entities.get("sources"))
    categories = clean_string_list(entities.get("categories"))
    locations = clean_string_list(entities.get("locations"))

    filters = {}

    source = first_or_none(sources)
    category = first_or_none(categories)
    derived_location = first_or_none(locations)

    search_query = clean_string(search_query)
    explicit_location = clean_string(location)
    final_location = explicit_location or derived_location
    score_threshold = coerce_float(score_threshold)
    radius_km = coerce_float(radius_km)

    if search_query:
        filters["query"] = search_query

    if source:
        filters["source"] = source

    if category:
        filters["category"] = category

    if score_threshold is not None:
        filters["min_score"] = score_threshold

    if final_location:
        filters["location"] = final_location

    if radius_km is not None:
        filters["radius"] = radius_km

    return filters


def choose_primary_endpoint(endpoint_candidates, filters):
    # Prefer the endpoint that most directly matches the required retrieval mode.
    # Mixed queries still pick one primary endpoint and pass the rest as filters.

    if "nearby" in endpoint_candidates and "location" in filters:
        return "nearby"

    if "score" in endpoint_candidates and "min_score" in filters:
        return "score"

    if "search" in endpoint_candidates and "query" in filters:
        return "search"

    if "category" in endpoint_candidates and "category" in filters:
        return "category"

    if "source" in endpoint_candidates and "source" in filters:
        return "source"

    # Fallbacks if the LLM returned candidates but filters are partially missing.
    if "search" in endpoint_candidates:
        return "search"

    if "category" in endpoint_candidates:
        return "category"

    if "source" in endpoint_candidates:
        return "source"

    if "score" in endpoint_candidates:
        return "score"

    if "nearby" in endpoint_candidates:
        return "nearby"

    raise ValueError("Unable to determine primary endpoint")


def get_ranking_mode(primary_endpoint):
    if primary_endpoint in {"category", "source"}:
        return "recency"

    if primary_endpoint == "score":
        return "score"

    if primary_endpoint == "search":
        return "search"

    if primary_endpoint == "nearby":
        return "distance"

    raise ValueError(f"Unsupported primary endpoint: {primary_endpoint}")


def lambda_handler(event, context):
    query_id = event.get("queryId")
    query_text = event.get("queryText")
    endpoint_candidates = normalize_candidates(event.get("endpointCandidates"))
    entities = event.get("entities")
    search_query = event.get("searchQuery")
    score_threshold = event.get("scoreThreshold")
    location = event.get("location")
    radius_km = event.get("radiusKm")

    if not isinstance(query_id, str) or not query_id.strip():
        raise ValueError("queryId is required")

    if not isinstance(query_text, str) or not query_text.strip():
        raise ValueError("queryText is required")

    if not endpoint_candidates:
        raise ValueError("endpointCandidates must contain at least one valid endpoint")

    filters = build_filters(
        entities=entities,
        search_query=search_query,
        score_threshold=score_threshold,
        location=location,
        radius_km=radius_km
    )

    primary_endpoint = choose_primary_endpoint(endpoint_candidates, filters)
    print(f"Selected primary endpoint: {primary_endpoint}")
    ranking_mode = get_ranking_mode(primary_endpoint)
    print(f"Set ranking mode: {ranking_mode}")

    return {
        "primaryEndpoint": primary_endpoint,
        "rankingMode": ranking_mode,
        "filters": filters
    }