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


def first_or_none(values):
    if not values:
        return None
    return values[0]


def build_topic_query(topic_values, query_text):
    cleaned_topics = clean_string_list(topic_values)

    if cleaned_topics:
        return " ".join(cleaned_topics)

    if isinstance(query_text, str) and query_text.strip():
        return query_text.strip()

    return None


def coerce_score(value):
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


def lambda_handler(event, context):
    query_id = event.get("queryId")
    query_text = event.get("queryText")
    intents = event.get("intents")
    entities = event.get("entities")

    if not isinstance(query_id, str) or not query_id.strip():
        raise ValueError("queryId is required")

    if not isinstance(query_text, str) or not query_text.strip():
        raise ValueError("queryText is required")

    if not isinstance(intents, list):
        raise ValueError("intents must be a list")

    if not isinstance(entities, dict):
        raise ValueError("entities must be an object")

    topic_values = clean_string_list(entities.get("topic"))
    source_values = clean_string_list(entities.get("source"))
    category_values = clean_string_list(entities.get("category"))
    location_values = clean_string_list(entities.get("location"))
    score_value = coerce_score(entities.get("score"))

    resolved = {
        "topicQuery": build_topic_query(topic_values, query_text),
        "source": first_or_none(source_values),
        "category": first_or_none(category_values),
        "location": first_or_none(location_values),
        "radiusKm": None,
        "scoreThreshold": score_value
    }

    return {
        "resolved": resolved
    }