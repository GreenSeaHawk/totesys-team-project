#Creation of SNS topic
resource "aws_sns_topic" "lambda_error_alert_extract" {
  name = "extract-lambda-error-alert"
}

#Set up email subscrition
resource "aws_sns_topic_subscription" "email_subscription_extract" {
  topic_arn = aws_sns_topic.lambda_error_alert_extract.arn
  protocol  = "email"
  endpoint  = "cimmerianc@yahoo.com" 
}

# Creation of cloudwatch metric alarm
resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm_extract" {
  alarm_name          = "LambdaErrorAlarmExtract"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60  
  statistic           = "Average"
  threshold           = 0.5 
  alarm_description   = "Alarm for extract lambda function errors"
  alarm_actions       = [aws_sns_topic.lambda_error_alert_extract.arn]

 dimensions = {
    FunctionName = aws_lambda_function.extract_lambda_func.function_name
  }
}

#cloudwatch permissions for alarm (might not be necessary)
# data "aws_iam_policy_document" "cw_alarm_action_document" {
#   statement {
#     actions = [
#       "cloudwatch:PutMetricAlarm",
#       "cloudwatch:DescribeAlarms"
#     ]
#     resources = [
#       aws_cloudwatch_metric_alarm.lambda_error_alarm.arn
#     ]
#   }
# }

# resource "aws_iam_policy" "cw_alarm_action_policy" {
#   name_prefix = "cw-alarm-action-policy"
#   policy = data.aws_iam_policy_document.cw_alarm_action_document.json
# }

# resource "aws_iam_role_policy_attachment" "lambda_cw_alarm_action_policy_attachment" {
#   role       = aws_iam_role.extract_lambda_role.name
#   policy_arn = aws_iam_policy.cw_alarm_action_policy.arn
# }