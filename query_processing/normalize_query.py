import json
import os

import boto3
from botocore.exceptions import ClientError

MODEL_ID = os.environ["MODEL_ID"]
BEDROCK_REGION = os.environ.get("BEDROCK_REGION") or os.environ.get("AWS_REGION", "us-west-2")

bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

ALLOWED_ENDPOINTS = ["category", "score", "search", "source", "nearby"]


def extract_text_from_converse_response(response):
    content = response.get("output", {}).get("message", {}).get("content", [])

    text_parts = []
    for block in content:
        text = block.get("text")
        if text:
            text_parts.append(text)

    if not text_parts:
        raise ValueError("No text returned from Bedrock model")

    return "\n".join(text_parts).strip()


def strip_code_fence(text):
    stripped = text.strip()

    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            return "\n".join(lines[1:-1]).strip()

    return stripped


def coerce_string_list(value):
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


def normalize_endpoint_candidates(value):
    if not isinstance(value, list):
        return []

    result = []
    seen = set()

    for item in value:
        if not isinstance(item, str):
            continue

        cleaned = item.strip().lower()
        if cleaned not in ALLOWED_ENDPOINTS:
            continue

        if cleaned in seen:
            continue

        seen.add(cleaned)
        result.append(cleaned)

    return result


def validate_model_output(payload, query_text):
    if not isinstance(payload, dict):
        raise ValueError("Model output must be a JSON object")

    endpoint_candidates = normalize_endpoint_candidates(payload.get("endpointCandidates"))
    entities = payload.get("entities")
    search_query = payload.get("searchQuery")
    score_threshold = payload.get("scoreThreshold")
    location = payload.get("location")
    radius_km = payload.get("radiusKm")

    if not endpoint_candidates:
        raise ValueError(
            f"Model output must contain at least one valid endpoint candidate from: {ALLOWED_ENDPOINTS}"
        )

    if not isinstance(entities, dict):
        entities = {}

    normalized_entities = {
        "people": coerce_string_list(entities.get("people")),
        "organizations": coerce_string_list(entities.get("organizations")),
        "locations": coerce_string_list(entities.get("locations")),
        "events": coerce_string_list(entities.get("events")),
        "sources": coerce_string_list(entities.get("sources")),
        "categories": coerce_string_list(entities.get("categories"))
    }

    if isinstance(search_query, str):
        search_query = search_query.strip()
    else:
        search_query = None

    if not search_query and "search" in endpoint_candidates:
        fallback_parts = []
        fallback_parts.extend(normalized_entities["people"])
        fallback_parts.extend(normalized_entities["organizations"])
        fallback_parts.extend(normalized_entities["events"])

        if fallback_parts:
            search_query = " ".join(fallback_parts)
        else:
            search_query = query_text.strip()

    if isinstance(location, str):
        location = location.strip() or None
    else:
        location = None

    score_threshold = coerce_float(score_threshold)
    radius_km = coerce_float(radius_km)

    return {
        "endpointCandidates": endpoint_candidates,
        "entities": normalized_entities,
        "searchQuery": search_query,
        "scoreThreshold": score_threshold,
        "location": location,
        "radiusKm": radius_km
    }


def lambda_handler(event, context):
    query_id = event.get("queryId")
    query_text = event.get("queryText")

    if not isinstance(query_id, str) or not query_id.strip():
        raise ValueError("queryId is required")

    if not isinstance(query_text, str) or not query_text.strip():
        raise ValueError("queryText is required")

    query_text = query_text.strip()

    system_prompt = f"""
You are a query understanding service for a contextual news retrieval system.

Return ONLY valid JSON in this exact shape:
{{
  "endpointCandidates": ["string"],
  "entities": {{
    "people": ["string"],
    "organizations": ["string"],
    "locations": ["string"],
    "events": ["string"],
    "sources": ["string"],
    "categories": ["string"]
  }},
  "searchQuery": "string",
  "scoreThreshold": 0.0,
  "location": "string",
  "radiusKm": 0.0
}}

Rules:
- Output valid JSON only.
- Do not wrap the JSON in markdown.
- Do not return any extra fields.
- endpointCandidates must contain one or more values from:
{json.dumps(ALLOWED_ENDPOINTS)}
- Do not invent any other endpoint names.
- "category": use when the user asks for a specific category like Technology, Business, Sports, General.
- "score": use when the user asks for highly relevant / top scored / above-threshold items by relevance score.
- "search": use when the user asks about a topic, event, person, organization, or free-text subject that should be searched in title/description.
- "source": use when the user asks for a particular publisher or source, such as Reuters or New York Times.
- "nearby": use when the user asks for news near a location or includes location-based nearby intent.
- Multiple endpointCandidates are allowed if the query implies filters in addition to the main retrieval style.
- Put people names in entities.people.
- Put company, publication, and organization names in entities.organizations unless they are clearly news publishers requested as filters, in which case also include them in entities.sources if appropriate.
- Put place names in entities.locations.
- Put named incidents/topics/acquisitions/tournaments/etc. in entities.events.
- Put news publishers in entities.sources.
- Put category names in entities.categories.
- searchQuery should be the text query to use for the search endpoint. Use a concise query derived from the user's topic.
- scoreThreshold should be numeric only when the user clearly requests a score cutoff; otherwise null.
- location should be a single location string only when relevant for nearby retrieval; otherwise null.
- radiusKm should be numeric only when the user clearly specifies a radius; otherwise null.

Examples:
- Query: "Latest developments in the Elon Musk Twitter acquisition near Palo Alto"
  Return endpointCandidates including "nearby" and extract entities for Elon Musk, Twitter, Palo Alto.
- Query: "Top technology news from the New York Times"
  Return endpointCandidates including "category" and "source".
- Query: "Show articles with relevance score above 0.7 about EVs"
  Return endpointCandidates including "score" and "search".
"""

    user_prompt = f"Analyze this user query for news retrieval:\n\n{query_text}"

    try:
        response = bedrock.converse(
            modelId=MODEL_ID,
            system=[
                {
                    "text": system_prompt.strip()
                }
            ],
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "text": user_prompt
                        }
                    ]
                }
            ],
            inferenceConfig={
                "maxTokens": 350,
                "temperature": 0,
                "topP": 0.9
            }
        )
    except ClientError as exc:
        raise Exception(f"Bedrock converse call failed: {str(exc)}")

    raw_text = extract_text_from_converse_response(response)
    raw_text = strip_code_fence(raw_text)

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise Exception(f"Model returned invalid JSON: {raw_text}") from exc

    return validate_model_output(parsed, query_text)