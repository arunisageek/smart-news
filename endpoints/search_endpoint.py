import json
import os
from datetime import timezone

import psycopg2
from psycopg2 import sql

ARTICLES_TABLE = os.environ.get("ARTICLES_TABLE", "articles")
DEFAULT_LIMIT = int(os.environ.get("DEFAULT_LIMIT", "20"))
DB_URI = os.environ["DB_URI"]
MAX_LIMIT = int(os.environ.get("MAX_LIMIT", "50"))


def get_connection():
    return psycopg2.connect(DB_URI)


def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }


def clean_string(value):
    if not isinstance(value, str):
        return None

    cleaned = value.strip()
    return cleaned if cleaned else None


def parse_limit(value):
    if value is None:
        return DEFAULT_LIMIT

    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError("limit must be an integer")

    if parsed < 1:
        raise ValueError("limit must be greater than 0")

    return min(parsed, MAX_LIMIT)


def to_iso_utc(value):
    if value is None:
        return None

    if hasattr(value, "astimezone"):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    return str(value)


def row_to_article(row):
    return {
        "id": str(row[0]),
        "title": row[1] or "",
        "description": row[2] or "",
        "url": row[3] or "",
        "publication_date": to_iso_utc(row[4]),
        "source_name": row[5] or "",
        "category": row[6] or [],
        "relevance_score": float(row[7]) if row[7] is not None else 0.0,
        "llm_summary": row[8] or "",
        "latitude": row[9],
        "longitude": row[10],
        "text_rank": float(row[11]) if row[11] is not None else 0.0
    }


def search_articles(cursor, query, source, category, limit_value):
    print(f"DEBUG: Search params - query: {query}, source: {source}, category: {category}")
    
    # Define all possible parameters in a dictionary
    param_dict = {
        "q": query,
        "like_q": f"%{query.lower()}%",
        "src": source,
        "cat": category,
        "limit": limit_value
    }

    # Use named placeholders %(name)s instead of %s
    where_clauses = [
        """
        (
            a.search_document @@ websearch_to_tsquery('english', %(q)s)
            OR lower(coalesce(a.title, '')) LIKE %(like_q)s
            OR lower(coalesce(a.description, '')) LIKE %(like_q)s
        )
        """
    ]

    if source:
        where_clauses.append("lower(a.source_name) = lower(%(src)s)")

    # if category:
    #     where_clauses.append("""
    #         EXISTS (
    #             SELECT 1
    #             FROM unnest(a.category) AS c
    #             WHERE lower(c) = lower(%(cat)s)
    #         )
    #     """)
    # if category:
    #     # Use ILIKE inside the unnest to allow partial matches like 'Tech' matching 'Technology'
    #     where_clauses.append("""
    #         EXISTS (
    #             SELECT 1
    #             FROM unnest(a.category) AS c
    #             WHERE c ILIKE %(cat_pattern)s
    #         )
    #     """)
    # param_dict["cat_pattern"] = f"%{category}%"

    query_sql = sql.SQL("""
        SELECT
            a.id, a.title, a.description, a.url, a.publication_date,
            a.source_name, a.category, a.relevance_score, a.llm_summary,
            a.latitude, a.longitude,
            CASE
                WHEN a.search_document @@ websearch_to_tsquery('english', %(q)s)
                THEN ts_rank_cd(a.search_document, websearch_to_tsquery('english', %(q)s))
                ELSE 0
            END AS text_rank
        FROM {table_name} AS a
        WHERE {where_conditions}
        ORDER BY
            (
                CASE
                    WHEN a.search_document @@ websearch_to_tsquery('english', %(q)s)
                    THEN ts_rank_cd(a.search_document, websearch_to_tsquery('english', %(q)s))
                    ELSE 0
                END
            ) DESC,
            COALESCE(a.relevance_score, 0) DESC,
            a.publication_date DESC
        LIMIT %(limit)s
    """).format(
        table_name=sql.Identifier("public", ARTICLES_TABLE),
        where_conditions=sql.SQL(" AND ").join(sql.SQL(clause) for clause in where_clauses)
    )

    # Psycopg2 maps the dictionary keys to the %(key)s placeholders automatically
    cursor.execute(query_sql, param_dict)
    results = cursor.fetchall()
    
    print(f"DEBUG: Found {len(results)} matches.")
    return results



def lambda_handler(event, context):
    query_params = event.get("queryStringParameters") or {}

    query = clean_string(query_params.get("query"))
    source = clean_string(query_params.get("source"))
    category = clean_string(query_params.get("category"))

    try:
        limit_value = parse_limit(query_params.get("limit"))
    except ValueError as exc:
        return build_response(400, {"message": str(exc)})

    if not query:
        return build_response(400, {
            "message": "query is required"
        })

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        rows = search_articles(
            cursor=cursor,
            query=query,
            source=source,
            category=category,
            limit_value=limit_value
        )

        articles = [row_to_article(row) for row in rows]

        return build_response(200, {
            "count": len(articles),
            "articles": articles
        })

    except Exception as exc:
        return build_response(500, {
            "message": f"Failed to retrieve search results: {str(exc)}"
        })

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()