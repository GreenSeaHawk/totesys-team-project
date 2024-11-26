# #Creation of SNS topic
# resource "aws_sns_topic" "lambda_error_alert_load" {
#   name = "load-lambda-error-alert"
# }

# #Set up email subscrition
# resource "aws_sns_topic_subscription" "email_subscription_load" {
#   topic_arn = aws_sns_topic.lambda_error_alert_load.arn
#   protocol  = "email"
#   endpoint  = "cimmerianc@yahoo.com" 
# }

# # Creation of cloudwatch metric alarm
# resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm_load" {
#   alarm_name          = "LambdaErrorAlarmLoad"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "Errors"
#   namespace           = "AWS/Lambda"
#   period              = 60  
#   statistic           = "Average"
#   threshold           = 0.5 
#   alarm_description   = "Alarm for load lambda function errors"
#   alarm_actions       = [aws_sns_topic.lambda_error_alert_load.arn]

#  dimensions = {
#     FunctionName = aws_lambda_function.load_lambda_func.function_name
#   }
# }

# # SNS permissions for Lambda to publish to SNS
# data "aws_iam_policy_document" "sns_publish_document_load" {
#   statement {
#     actions = ["sns:Publish"]
#     resources = [
#       aws_sns_topic.lambda_error_alert_load.arn
#     ]
#   }
# }

# resource "aws_iam_policy" "sns_publish_policy_load" {
#   name_prefix = "sns-publish-policy"
#   policy = data.aws_iam_policy_document.sns_publish_document_load.json
# }

# resource "aws_iam_role_policy_attachment" "lambda_sns_publish_policy_attachment_load" {
#   role       = aws_iam_role.load_lambda_role.name
#   policy_arn = aws_iam_policy.sns_publish_policy_load.arn
# }
