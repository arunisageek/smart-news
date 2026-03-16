import json
import os

import boto3
from botocore.exceptions import ClientError

MODEL_ID = os.environ["MODEL_ID"]
BEDROCK_REGION = os.environ.get("BEDROCK_REGION") or os.environ.get("AWS_REGION", "us-west-2")

bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

ALLOWED_INTENTS = ["category", "score", "search", "source", "nearby"]


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


def validate_model_output(payload):
    if not isinstance(payload, dict):
        raise ValueError("Model output must be a JSON object")

    intents = payload.get("intents")
    entities = payload.get("entities")

    if not isinstance(intents, list) or not intents:
        raise ValueError("Model output must contain a non-empty intents array")

    cleaned_intents = []
    seen = set()

    for intent in intents:
        if not isinstance(intent, str):
            continue

        cleaned = intent.strip()
        if cleaned not in ALLOWED_INTENTS:
            raise ValueError(
                f"Invalid intent returned by model: {cleaned}. Allowed values are: {ALLOWED_INTENTS}"
            )

        if cleaned not in seen:
            seen.add(cleaned)
            cleaned_intents.append(cleaned)

    if not cleaned_intents:
        raise ValueError("Model output did not contain any valid intents")

    if not isinstance(entities, dict):
        entities = {}

    return {
        "intents": cleaned_intents,
        "entities": {
            "topic": coerce_string_list(entities.get("topic")),
            "source": coerce_string_list(entities.get("source")),
            "category": coerce_string_list(entities.get("category")),
            "location": coerce_string_list(entities.get("location")),
            "score": coerce_score(entities.get("score"))
        }
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
You are a query understanding service for a news system.

Return ONLY valid JSON in this exact shape:
{{
  "intents": ["string"],
  "entities": {{
    "topic": ["string"],
    "source": ["string"],
    "category": ["string"],
    "location": ["string"],
    "score": 0.0
  }}
}}

Rules:
- Output valid JSON only.
- Do not wrap the JSON in markdown.
- Do not return any extra fields.
- intents must contain one or more values from:
{json.dumps(ALLOWED_INTENTS)}
- Do not invent any other intent values.
- Multiple intents are allowed when needed.
- Use "search" for topic/free-text queries derived from the user's input.
- Use "source" when the query asks for a specific publisher/source.
- Use "category" when the query asks for a category like technology, business, sports, general.
- Use "score" when the query asks for high scoring / top relevance score / score threshold style retrieval.
- Use "nearby" when the query includes a location-based nearby request.
- Put topical search terms into entities.topic.
- Put publisher names into entities.source.
- Put category names into entities.category.
- Put places/locations into entities.location.
- Put a numeric threshold into entities.score only if the query clearly asks for a score cutoff.
- If a field has no values, return an empty array for list fields and null for score.
"""

    user_prompt = f"Classify this query and extract entities:\n\n{query_text}"

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
                "maxTokens": 250,
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

    return validate_model_output(parsed)