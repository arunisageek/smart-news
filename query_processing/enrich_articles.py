import os

import boto3
from botocore.exceptions import ClientError

MODEL_ID = os.environ["MODEL_ID"]
BEDROCK_REGION = os.environ.get("BEDROCK_REGION") or os.environ.get("AWS_REGION", "us-west-2")

bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)


def generate_summary(title, description, query_text):
    prompt = (
        f"You are a news summarization assistant.\n"
        f"The user searched for: {query_text}\n\n"
        f"Summarize the following news article in 2-3 sentences, "
        f"highlighting why it is relevant to the user's query.\n\n"
        f"Title: {title}\n"
        f"Description: {description}\n\n"
        f"Return only the summary text, no labels or extra formatting."
    )

    try:
        response = bedrock.converse(
            modelId=MODEL_ID,
            messages=[
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ],
            inferenceConfig={
                "maxTokens": 150,
                "temperature": 0.3,
                "topP": 0.9
            }
        )
        content = response.get("output", {}).get("message", {}).get("content", [])
        for block in content:
            text = block.get("text", "").strip()
            if text:
                return text
    except ClientError:
        pass

    # Fallback: return description as-is if Bedrock call fails
    return description or ""


def lambda_handler(event, context):
    query_text = event.get("queryText", "")
    articles = event.get("articles", [])

    enriched = []
    for article in articles:
        summary = generate_summary(
            title=article.get("title", ""),
            description=article.get("description", ""),
            query_text=query_text
        )
        enriched.append({**article, "llm_summary": summary})

    return {
        "count": len(enriched),
        "articles": enriched
    }
