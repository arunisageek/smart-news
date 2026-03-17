def lambda_handler(event, context):
    articles = event.get("articles", [])
    return {
        "count": len(articles),
        "articles": articles
    }
