resource "aws_sfn_state_machine" "state_machine_etl" {
  name     = "state-machine-etl"
  role_arn = aws_iam_role.state_machine_role.arn

  definition = <<EOF
{
  "Comment": "ETL state machine",
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

data "aws_iam_policy_document" "state_machine_cw" {
    statement {
      actions = [ "states:StartExecution" ]
      resources = [ "${aws_sfn_state_machine.state_machine_etl.arn}" ]
    }
}

resource "aws_iam_policy" "cw_trigger_sm" {
    name_prefix =  "cw_trigger_sm"
    policy = data.aws_iam_policy_document.state_machine_cw.json
}

resource "aws_iam_role_policy_attachment" "attach_cw_trigger_sm" {
    role = aws_iam_role.eventbridge_to_stepfunctions.name
    policy_arn = aws_iam_policy.cw_trigger_sm.arn
}

resource "aws_iam_role" "eventbridge_to_stepfunctions" {
  name = "eventbridge-to-stepfunctions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "events.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Create rule to trigger every 5 mins
resource "aws_cloudwatch_event_rule" "trigger_extract_5_mins" {
    name = "trigger_sm_5_mins"
    description = "Triggers extract lambda func every 5 mins"
    schedule_expression = "rate(5 minutes)"
}

# Targets rule towards sm
resource "aws_cloudwatch_event_target" "check_extract_5_mins" {
    rule = aws_cloudwatch_event_rule.trigger_extract_5_mins.name
    target_id = "state_machine_etl"
    arn = aws_sfn_state_machine.state_machine_etl.arn
    role_arn = aws_iam_role.eventbridge_to_stepfunctions.arn
}

# # permissions to check sm
# resource "aws_lambda_permission" "allow_cloudwatch_execute_extract" {
#     statement_id = "AllowExecutionFromCloudWatch"
#     action = "states:StartExecution" 
#     function_name = aws_lambda_function.extract_lambda_func.function_name
#     principal = "events.amazonaws.com"
#     source_arn = aws_cloudwatch_event_rule.trigger_extract_5_mins.arn
# }

