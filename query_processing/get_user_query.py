import os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["TABLE_NAME"]
table = dynamodb.Table(TABLE_NAME)


def convert_decimals(value):
    if isinstance(value, list):
        return [convert_decimals(item) for item in value]

    if isinstance(value, dict):
        return {key: convert_decimals(val) for key, val in value.items()}

    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)

    return value


def lambda_handler(event, context):
    query_id = event.get("queryId")

    if not query_id or not isinstance(query_id, str):
        raise ValueError("queryId is required")

    try:
        response = table.get_item(
            Key={
                "queryId": query_id
            }
        )
    except ClientError as exc:
        raise Exception(f"Failed to fetch query from DynamoDB: {str(exc)}")

    item = response.get("Item")

    if not item:
        raise Exception(f"Query not found for queryId: {query_id}")

    return convert_decimals(item)