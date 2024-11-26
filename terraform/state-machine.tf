resource "aws_sfn_state_machine" "state_machine_etl" {
  name     = "state-machine-etl"
  role_arn = aws_iam_role.state_machine_role.arn

  definition = <<EOF
{
  "Comment": "A description of my state machine",
  "StartAt": "extract_lambda_func",
  "States": {
    "extract_lambda_func": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.extract_lambda_func.arn}:$LATEST"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "transform_lambda_func"
    },
    "transform_lambda_func": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.transform_lambda_func.arn}:$LATEST"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "load_lambda_func"
    },
    "load_lambda_func": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "${aws_lambda_function.load_lambda_func.arn}:$LATEST"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "End": true
    }
  }
}
EOF
}

# IAM POLICY

resource "aws_iam_role" "state_machine_role" {
  name_prefix        = "state-machine-etl"
  assume_role_policy = <<EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "states.amazonaws.com"
                    ]
                }
        }
        
    ]
}
EOF
}

data "aws_iam_policy_document" "lambda_statemachine_permissions" {
  statement {
      
      actions = [
          "lambda:InvokeFunction"
      ]
      resources = [
          "${aws_lambda_function.extract_lambda_func.arn}:*",
          "${aws_lambda_function.transform_lambda_func.arn}:*",
          "${aws_lambda_function.load_lambda_func.arn}:*"
      ]
  }
}

resource "aws_iam_policy" "state_machine_policy" {
  name_prefix = "state-machine-policy"
  policy = data.aws_iam_policy_document.lambda_statemachine_permissions.json
}

resource "aws_iam_role_policy_attachment" "attach-lambda-statemachine-permissions" {
  role = aws_iam_role.state_machine_role.name
  policy_arn = aws_iam_policy.state_machine_policy.arn
}

