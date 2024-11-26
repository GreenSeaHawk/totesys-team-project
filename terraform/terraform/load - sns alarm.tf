#Creation of SNS topic
resource "aws_sns_topic" "lambda_error_alert_load" {
  name = "load-lambda-error-alert"
}

#Set up email subscrition
resource "aws_sns_topic_subscription" "email_subscription_load" {
  topic_arn = aws_sns_topic.lambda_error_alert_load.arn
  protocol  = "email"
  endpoint  = "cimmerianc@yahoo.com" 
}

# Creation of cloudwatch metric alarm
resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm_load" {
  alarm_name          = "LambdaErrorAlarmLoad"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60  
  statistic           = "Average"
  threshold           = 0.5 
  alarm_description   = "Alarm for load lambda function errors"
  alarm_actions       = [aws_sns_topic.lambda_error_alert_load.arn]

 dimensions = {
    FunctionName = aws_lambda_function.load_lambda_func.function_name
  }
}