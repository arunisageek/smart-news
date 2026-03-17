import json
import os
import urllib.parse
import urllib.request
import urllib.error


BASE_URL = os.environ["RETRIEVE_BASE_URL"].rstrip("/")

ENDPOINT_PATHS = {
    "category": "/category",
    "score": "/score",
    "search": "/search",
    "source": "/source",
    "nearby": "/nearby"
}

DEFAULT_LIMIT = int(os.environ.get("DEFAULT_LIMIT", "20"))
HTTP_TIMEOUT_SECONDS = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "10"))


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


def build_query_params(primary_endpoint, filters):
    if not isinstance(filters, dict):
        filters = {}

    limit = filters.get("limit", DEFAULT_LIMIT)

    if primary_endpoint == "category":
        category = clean_string(filters.get("category"))
        if not category:
            raise ValueError("category endpoint requires filters.category")

        params = {
            "category": category,
            "limit": limit
        }

        source = clean_string(filters.get("source"))
        if source:
            params["source"] = source

        return params

    if primary_endpoint == "score":
        threshold = coerce_float(filters.get("min_score"))
        if threshold is None:
            raise ValueError("score endpoint requires filters.min_score")

        params = {
            "threshold": threshold,
            "limit": limit
        }

        query = clean_string(filters.get("query"))
        source = clean_string(filters.get("source"))
        category = clean_string(filters.get("category"))

        if query:
            params["query"] = query
        if source:
            params["source"] = source
        if category:
            params["category"] = category

        return params

    if primary_endpoint == "search":
        query = clean_string(filters.get("query"))
        if not query:
            raise ValueError("search endpoint requires filters.query")

        params = {
            "query": query,
            "limit": limit
        }

        source = clean_string(filters.get("source"))
        category = clean_string(filters.get("category"))
        location = clean_string(filters.get("location"))

        if source:
            params["source"] = source
        if category:
            params["category"] = category
        if location:
            params["location"] = location

        return params

    if primary_endpoint == "source":
        source = clean_string(filters.get("source"))
        if not source:
            raise ValueError("source endpoint requires filters.source")

        params = {
            "source": source,
            "limit": limit
        }

        category = clean_string(filters.get("category"))
        if category:
            params["category"] = category

        return params

    if primary_endpoint == "nearby":
        lat = coerce_float(filters.get("lat"))
        lon = coerce_float(filters.get("lon"))
        radius = coerce_float(filters.get("radius"))

        if lat is None or lon is None or radius is None:
            raise ValueError("nearby endpoint requires filters.lat, filters.lon, and filters.radius")

        params = {
            "lat": lat,
            "lon": lon,
            "radius": radius,
            "limit": limit
        }

        query = clean_string(filters.get("query"))
        source = clean_string(filters.get("source"))
        category = clean_string(filters.get("category"))

        if query:
            params["query"] = query
        if source:
            params["source"] = source
        if category:
            params["category"] = category

        return params

    raise ValueError(f"Unsupported primaryEndpoint: {primary_endpoint}")


def build_url(primary_endpoint, query_params):
    path = ENDPOINT_PATHS.get(primary_endpoint)
    if not path:
        raise ValueError(f"No path configured for endpoint: {primary_endpoint}")

    encoded = urllib.parse.urlencode(query_params, doseq=True)
    return f"{BASE_URL}{path}?{encoded}"


def http_get_json(url):
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={
            "Accept": "application/json"
        }
    )

    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8")
            status_code = response.getcode()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise Exception(f"Endpoint call failed with status {exc.code}: {body}")
    except urllib.error.URLError as exc:
        raise Exception(f"Endpoint call failed: {str(exc)}")

    if status_code < 200 or status_code >= 300:
        raise Exception(f"Endpoint call returned unexpected status {status_code}: {body}")

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise Exception(f"Endpoint returned invalid JSON: {body}") from exc


def normalize_response(payload):
    if isinstance(payload, list):
        return {
            "count": len(payload),
            "articles": payload
        }

    if isinstance(payload, dict):
        articles = payload.get("articles")
        if isinstance(articles, list):
            return {
                "count": len(articles),
                "articles": articles
            }

    raise ValueError("Endpoint response must be either a list or an object with an 'articles' list")


def lambda_handler(event, context):
    query_id = event.get("queryId")
    query_text = event.get("queryText")
    primary_endpoint = event.get("primaryEndpoint")
    filters = event.get("filters", {})

    if not isinstance(query_id, str) or not query_id.strip():
        raise ValueError("queryId is required")

    if not isinstance(query_text, str) or not query_text.strip():
        raise ValueError("queryText is required")

    if not isinstance(primary_endpoint, str) or not primary_endpoint.strip():
        raise ValueError("primaryEndpoint is required")

    primary_endpoint = primary_endpoint.strip().lower()

    query_params = build_query_params(primary_endpoint, filters)
    url = build_url(primary_endpoint, query_params)
    payload = http_get_json(url)
    normalized = normalize_response(payload)

    return {
        "count": normalized["count"],
        "articles": normalized["articles"]
    }