import os
import json
import psycopg2
from psycopg2 import extras

def lambda_handler(event, context):
    # 1. Parse the incoming API Gateway body
    try:
        # If coming from API Gateway Proxy, the body is a string
        body = event.get('body', '[]')
        items = json.loads(body)
        
        # Ensure items is a list even if a single object is sent
        if isinstance(items, dict):
            items = [items]
            
    except Exception as e:
        return {
            'statusCode': 400, 
            'body': json.dumps({'error': f"Invalid JSON format: {str(e)}"})
        }

    # 2. Database Connection details from Environment Variables
    conn_uri = os.environ.get('POSTGRES_URI')
    if not conn_uri:
        return {
            'statusCode': 500, 
            'body': json.dumps({'error': "Database URI not configured in Environment Variables"})
        }

    # 3. Prepare data for the multi-row INSERT
    # We map JSON fields to the table columns defined in your SQL script
    data_to_insert = []
    for i in items:
        data_to_insert.append((
            i.get('id'),
            i.get('title'),
            i.get('description'),
            i.get('url'),
            i.get('publication_date'),
            i.get('source_name'),
            i.get('category', []), # Postgres TEXT[] mapping
            i.get('relevance_score', 0.0),
            i.get('llm_summary'),
            i.get('latitude'),
            i.get('longitude'),
            json.dumps(i)          # full object into raw_payload (JSONB)
        ))

    # 4. Define the SQL Query
    # Note: location and search_document are omitted because your TRIGGER handles them
    query = """
        INSERT INTO articles (
            id, title, description, url, publication_date, 
            source_name, category, relevance_score, llm_summary, 
            latitude, longitude, raw_payload
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            url = EXCLUDED.url,
            publication_date = EXCLUDED.publication_date,
            category = EXCLUDED.category,
            relevance_score = EXCLUDED.relevance_score,
            llm_summary = EXCLUDED.llm_summary,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            raw_payload = EXCLUDED.raw_payload,
            updated_at = NOW();
    """

    # 5. Execute Database Operations
    conn = None
    try:
        conn = psycopg2.connect(conn_uri)
        cur = conn.cursor()
        
        # execute_values is much faster than a standard loop for multiple rows
        extras.execute_values(cur, query, data_to_insert)
        
        conn.commit()
        cur.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'message': f'Processed {len(items)} articles successfully.'
            })
        }

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Database insertion failed', 'details': str(e)})
        }
    finally:
        if conn:
            conn.close()
