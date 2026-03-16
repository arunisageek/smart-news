import json
import os
import base64
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
stepfunctions = boto3.client("stepfunctions")

TABLE_NAME = os.environ["TABLE_NAME"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

table = dynamodb.Table(TABLE_NAME)


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }


def parse_body(event):
    body = event.get("body")

    if body is None:
        return {}

    if isinstance(body, dict):
        return body

    if not isinstance(body, str):
        raise ValueError("Request body must be a JSON string")

    if event.get("isBase64Encoded") is True:
        body = base64.b64decode(body).decode("utf-8")

    if not body.strip():
        return {}

    return json.loads(body)


def get_query_id(event, context):
    request_context = event.get("requestContext", {}) or {}

    return (
        request_context.get("extendedRequestId")
        or request_context.get("requestId")
        or context.aws_request_id
    )


def lambda_handler(event, context):
    try:
        body = parse_body(event)
    except json.JSONDecodeError:
        return build_response(400, {
            "message": "Invalid JSON in request body"
        })
    except Exception as exc:
        return build_response(400, {
            "message": f"Unable to parse request body: {str(exc)}"
        })

    query_text = body.get("queryText")
    user_id = body.get("userId")
    session_id = body.get("sessionId")
    source = body.get("source", "api")

    if not isinstance(query_text, str) or not query_text.strip():
        return build_response(400, {
            "message": "queryText is required and must be a non-empty string"
        })

    query_text = query_text.strip()
    now = utc_now()
    request_context = event.get("requestContext", {}) or {}
    query_id = get_query_id(event, context)

    item = {
        "queryId": query_id,
        "queryText": query_text,
        "userId": user_id,
        "sessionId": session_id,
        "source": source,
        "status": "RECEIVED",
        "createdAt": now,
        "updatedAt": now,
        "requestId": request_context.get("requestId"),
        "extendedRequestId": request_context.get("extendedRequestId"),
        "lambdaRequestId": context.aws_request_id,
        "result": None,
        "error": None
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(queryId)"
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        if error_code == "ConditionalCheckFailedException":
            return build_response(409, {
                "message": "A query with this request ID already exists",
                "queryId": query_id
            })

        return build_response(500, {
            "message": "Failed to store query in DynamoDB",
            "queryId": query_id,
            "error": str(exc)
        })

    execution_input = {
        "queryId": query_id
    }

    try:
        sfn_response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=query_id,
            input=json.dumps(execution_input)
        )
    except ClientError as exc:
        error_message = str(exc)
        error_code = exc.response.get("Error", {}).get("Code")

        try:
            table.update_item(
                Key={"queryId": query_id},
                UpdateExpression="SET #status = :status, #error = :error, updatedAt = :updatedAt",
                ExpressionAttributeNames={
                    "#status": "status",
                    "#error": "error"
                },
                ExpressionAttributeValues={
                    ":status": "FAILED",
                    ":error": error_message,
                    ":updatedAt": utc_now()
                }
            )
        except ClientError:
            pass

        if error_code == "ExecutionAlreadyExists":
            return build_response(409, {
                "message": "A workflow execution with this request ID already exists",
                "queryId": query_id
            })

        return build_response(500, {
            "message": "Query stored but failed to start workflow",
            "queryId": query_id,
            "error": error_message
        })

    try:
        table.update_item(
            Key={"queryId": query_id},
            UpdateExpression="SET workflowExecutionArn = :executionArn, #status = :status, updatedAt = :updatedAt",
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={
                ":executionArn": sfn_response["executionArn"],
                ":status": "PROCESSING",
                ":updatedAt": utc_now()
            }
        )
    except ClientError:
        pass

    return build_response(202, {
        "queryId": query_id,
        "status": "PROCESSING",
        "message": "Query accepted for processing"
    })