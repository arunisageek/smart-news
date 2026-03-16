import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


TABLE_NAME = os.environ["TABLE_NAME"]

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def to_dynamodb_compatible(value):
    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, list):
        return [to_dynamodb_compatible(item) for item in value]

    if isinstance(value, dict):
        return {key: to_dynamodb_compatible(val) for key, val in value.items()}

    return value


def lambda_handler(event, context):
    query_id = event.get("queryId")
    status = event.get("status")
    result = event.get("result")
    error = event.get("error")

    if not isinstance(query_id, str) or not query_id.strip():
        raise ValueError("queryId is required")

    if not isinstance(status, str) or not status.strip():
        raise ValueError("status is required")

    status = status.strip().upper()
    if status not in {"COMPLETED", "FAILED", "PROCESSING", "RECEIVED"}:
        raise ValueError(f"Unsupported status: {status}")

    now = utc_now()

    expression_attribute_names = {
        "#status": "status",
        "#result": "result",
        "#error": "error"
    }

    expression_attribute_values = {
        ":status": status,
        ":updatedAt": now
    }

    update_parts = [
        "#status = :status",
        "updatedAt = :updatedAt"
    ]

    remove_parts = []

    if status == "COMPLETED":
        expression_attribute_values[":result"] = to_dynamodb_compatible(result if result is not None else {})
        update_parts.append("#result = :result")
        remove_parts.append("#error")

    elif status == "FAILED":
        expression_attribute_values[":error"] = to_dynamodb_compatible(error if error is not None else {})
        update_parts.append("#error = :error")
        remove_parts.append("#result")

    else:
        if result is not None:
            expression_attribute_values[":result"] = to_dynamodb_compatible(result)
            update_parts.append("#result = :result")

        if error is not None:
            expression_attribute_values[":error"] = to_dynamodb_compatible(error)
            update_parts.append("#error = :error")

    update_expression = "SET " + ", ".join(update_parts)

    if remove_parts:
        update_expression += " REMOVE " + ", ".join(remove_parts)

    try:
        table.update_item(
            Key={
                "queryId": query_id
            },
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ConditionExpression="attribute_exists(queryId)"
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        if error_code == "ConditionalCheckFailedException":
            raise Exception(f"Query not found for queryId: {query_id}")

        raise Exception(f"Failed to update query in DynamoDB: {str(exc)}")

    return {
        "queryId": query_id,
        "status": status,
        "updatedAt": now
    }