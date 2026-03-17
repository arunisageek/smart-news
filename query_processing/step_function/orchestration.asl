{
  "Comment": "News query workflow using primary simulated endpoint selection, retrieval, ranking, enrichment, and persistence",
  "StartAt": "GetUserQuery",
  "TimeoutSeconds": 900,
  "States": {
    "GetUserQuery": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "GetUserQueryFn",
        "Payload": {
          "queryId.$": "$.queryId"
        }
      },
      "ResultSelector": {
        "query.$": "$.Payload"
      },
      "ResultPath": "$.queryRecord",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "States.Timeout"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "PrepareFailureUpdate"
        }
      ],
      "Next": "NormalizeQuery"
    },
    "NormalizeQuery": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "NormalizeQueryFn",
        "Payload": {
          "queryId.$": "$.queryId",
          "queryText.$": "$.queryRecord.query.queryText"
        }
      },
      "ResultSelector": {
        "endpointCandidates.$": "$.Payload.endpointCandidates",
        "entities.$": "$.Payload.entities",
        "searchQuery.$": "$.Payload.searchQuery",
        "scoreThreshold.$": "$.Payload.scoreThreshold",
        "location.$": "$.Payload.location",
        "radiusKm.$": "$.Payload.radiusKm"
      },
      "ResultPath": "$.normalized",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "States.Timeout"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "PrepareFailureUpdate"
        }
      ],
      "Next": "SelectEndpoint"
    },
    "SelectEndpoint": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "SelectEndpointFn",
        "Payload": {
          "queryId.$": "$.queryId",
          "queryText.$": "$.queryRecord.query.queryText",
          "endpointCandidates.$": "$.normalized.endpointCandidates",
          "entities.$": "$.normalized.entities",
          "searchQuery.$": "$.normalized.searchQuery",
          "scoreThreshold.$": "$.normalized.scoreThreshold",
          "location.$": "$.normalized.location",
          "radiusKm.$": "$.normalized.radiusKm"
        }
      },
      "ResultSelector": {
        "primaryEndpoint.$": "$.Payload.primaryEndpoint",
        "filters.$": "$.Payload.filters",
        "rankingMode.$": "$.Payload.rankingMode"
      },
      "ResultPath": "$.selection",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "States.Timeout"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "PrepareFailureUpdate"
        }
      ],
      "Next": "RetrieveArticles"
    },
    "RetrieveArticles": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "SearchArticlesFn",
        "Payload": {
          "queryId.$": "$.queryId",
          "queryText.$": "$.queryRecord.query.queryText",
          "primaryEndpoint.$": "$.selection.primaryEndpoint",
          "filters.$": "$.selection.filters"
        }
      },
      "ResultSelector": {
        "count.$": "$.Payload.count",
        "articles.$": "$.Payload.articles"
      },
      "ResultPath": "$.retrieval",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "States.Timeout"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "PrepareFailureUpdate"
        }
      ],
      "Next": "HaveResults"
    },
    "HaveResults": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.retrieval.count",
          "NumericEquals": 0,
          "Next": "BuildEmptyResult"
        }
      ],
      "Default": "RankArticles"
    },
    "RankArticles": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "RankArticlesFn",
        "Payload": {
          "queryId.$": "$.queryId",
          "queryText.$": "$.queryRecord.query.queryText",
          "primaryEndpoint.$": "$.selection.primaryEndpoint",
          "rankingMode.$": "$.selection.rankingMode",
          "filters.$": "$.selection.filters",
          "articles.$": "$.retrieval.articles"
        }
      },
      "ResultSelector": {
        "count.$": "$.Payload.count",
        "articles.$": "$.Payload.articles"
      },
      "ResultPath": "$.ranking",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "States.Timeout"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "PrepareFailureUpdate"
        }
      ],
      "Next": "EnrichTop5"
    },
    "EnrichTop5": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "EnrichTop5Fn",
        "Payload": {
          "queryId.$": "$.queryId",
          "queryText.$": "$.queryRecord.query.queryText",
          "articles.$": "$.ranking.articles"
        }
      },
      "ResultSelector": {
        "count.$": "$.Payload.count",
        "articles.$": "$.Payload.articles"
      },
      "ResultPath": "$.enriched",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "States.Timeout"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.error",
          "Next": "PrepareFailureUpdate"
        }
      ],
      "Next": "PrepareSuccessUpdate"
    },
    "BuildEmptyResult": {
      "Type": "Pass",
      "Parameters": {
        "count": 0,
        "articles": []
      },
      "ResultPath": "$.enriched",
      "Next": "PrepareSuccessUpdate"
    },
    "PrepareSuccessUpdate": {
      "Type": "Pass",
      "Parameters": {
        "queryId.$": "$.queryId",
        "status": "COMPLETED",
        "result": {
          "queryText.$": "$.queryRecord.query.queryText",
          "primaryEndpoint.$": "$.selection.primaryEndpoint",
          "filters.$": "$.selection.filters",
          "count.$": "$.enriched.count",
          "articles.$": "$.enriched.articles"
        }
      },
      "ResultPath": "$.updatePayload",
      "Next": "UpdateUserQuery"
    },
    "PrepareFailureUpdate": {
      "Type": "Pass",
      "Parameters": {
        "queryId.$": "$.queryId",
        "status": "FAILED",
        "error.$": "$.error"
      },
      "ResultPath": "$.updatePayload",
      "Next": "UpdateUserQuery"
    },
    "UpdateUserQuery": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "UpdateUserQueryFn",
        "Payload.$": "$.updatePayload"
      },
      "ResultPath": null,
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "States.Timeout"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "WasWorkflowFailure"
    },
    "WasWorkflowFailure": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.updatePayload.status",
          "StringEquals": "FAILED",
          "Next": "WorkflowFailed"
        }
      ],
      "Default": "WorkflowSucceeded"
    },
    "WorkflowSucceeded": {
      "Type": "Succeed"
    },
    "WorkflowFailed": {
      "Type": "Fail",
      "Error": "UserQueryProcessingFailed",
      "Cause": "One or more query processing steps failed"
    }
  }
}