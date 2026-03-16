import os
from datetime import timezone

import psycopg2


DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

MAX_RESULTS = int(os.environ.get("MAX_RESULTS", "20"))
ARTICLES_TABLE = os.environ.get("ARTICLES_TABLE", "articles")
conn_uri = os.environ.get('POSTGRES_URI')


def get_connection():
    return psycopg2.connect(conn_uri)


def clean_entities(entities):
    if not isinstance(entities, list):
        return []

    result = []
    seen = set()

    for item in entities:
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


def build_primary_search_text(query_text, entities):
    cleaned_entities = clean_entities(entities)

    if cleaned_entities:
        return " ".join(cleaned_entities)

    if isinstance(query_text, str) and query_text.strip():
        return query_text.strip()

    raise ValueError("At least one of queryText or entities must be present")


def to_iso_utc(value):
    if value is None:
        return None

    if hasattr(value, "astimezone"):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    return str(value)


def execute_search(cursor, table_name, search_text, limit_value):
    sql = f"""
        WITH query_input AS (
            SELECT websearch_to_tsquery('english', %s) AS ts_query
        )
        SELECT
            a.id,
            a.title,
            COALESCE(NULLIF(a.llm_summary, ''), a.description) AS summary,
            a.source_name,
            a.publication_date,
            a.url,
            a.category,
            a.relevance_score,
            ts_rank_cd(a.search_document, q.ts_query) AS text_rank,
            (
                ts_rank_cd(a.search_document, q.ts_query) * 0.80
                + COALESCE(a.relevance_score, 0) * 0.20
            ) AS combined_score
        FROM public.{table_name} a
        CROSS JOIN query_input q
        WHERE a.search_document @@ q.ts_query
        ORDER BY combined_score DESC, a.publication_date DESC
        LIMIT %s
    """

    cursor.execute(sql, (search_text, limit_value))
    return cursor.fetchall()


def row_to_article(row):
    return {
        "articleId": str(row[0]),
        "title": row[1] or "",
        "summary": row[2] or "",
        "source": row[3] or "",
        "publishedAt": to_iso_utc(row[4]),
        "url": row[5] or "",
        "category": row[6] or [],
        "relevanceScore": row[7] if row[7] is not None else 0,
        "textRank": row[8] if row[8] is not None else 0,
        "combinedScore": row[9] if row[9] is not None else 0
    }


def lambda_handler(event, context):
    query_id = event.get("queryId")
    query_text = event.get("queryText")
    intent = event.get("intent")
    entities = event.get("entities", [])

    if not isinstance(query_id, str) or not query_id.strip():
        raise ValueError("queryId is required")

    if query_text is not None and not isinstance(query_text, str):
        raise ValueError("queryText must be a string if provided")

    if intent is not None and not isinstance(intent, str):
        raise ValueError("intent must be a string if provided")

    cleaned_entities = clean_entities(entities)
    primary_search_text = build_primary_search_text(query_text, cleaned_entities)

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        rows = execute_search(cursor, ARTICLES_TABLE, primary_search_text, MAX_RESULTS)

        if not rows and cleaned_entities and isinstance(query_text, str) and query_text.strip():
            fallback_search_text = query_text.strip()

            if fallback_search_text != primary_search_text:
                rows = execute_search(cursor, ARTICLES_TABLE, fallback_search_text, MAX_RESULTS)

        articles = [row_to_article(row) for row in rows]

        return {
            "count": len(articles),
            "articles": articles
        }

    except Exception as exc:
        raise Exception(f"Failed to search articles: {str(exc)}")

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()