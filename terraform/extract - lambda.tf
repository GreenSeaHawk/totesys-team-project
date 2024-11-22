# CHANGE FILE PATH NAMES WHEN KNOWN

resource "aws_lambda_function" "extract_lambda_func" {
    function_name = "extract_lambda_func"
    filename = "${path.module}/../compressed_funcs/extract_lambda.zip"
    role = aws_iam_role.extract_lambda_role.arn
    handler = "handler.lambda_handler" # this might not be the correct path
    runtime = "python3.13"
    timeout = 600
    source_code_hash = data.archive_file.archive_extract_lambda.output_base64sha256
    layers = [aws_lambda_layer_version.extract_lambda_layer.arn]
}

# Zip extract lambda handler to local zip file
data "archive_file" "archive_extract_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../lambda/extract/src"
  output_path = "${path.module}/../compressed_funcs/extract_lambda.zip"
}

# ZIPPED MANUALLY
# # Zip extract layer requirements to local file
# data "archive_file" "extract_requirements_layer" {
#     type = "zip"
#     source_dir = "${path.module}/../lambda/extract/" #complete this when path to layer known
#     output_path = "${path.module}/../extract_lambda_layer.zip"
# }

# Attach zipped extract layer to extract lambda func
resource "aws_lambda_layer_version" "extract_lambda_layer" {
  filename   = "${path.module}/../layers/pg8000-panda-numpy.zip"
  layer_name = "extract_lambda_layer"
}

# Create rule to trigger every 5 mins
resource "aws_cloudwatch_event_rule" "trigger_extract_5_mins" {
    name = "trigger_extract_5_mins"
    description = "Triggers extract lambda func every 5 mins"
    schedule_expression = "rate(5 minutes)"
}

# Targets rule towards extract_lambda_func
resource "aws_cloudwatch_event_target" "check_extract_5_mins" {
    rule = aws_cloudwatch_event_rule.trigger_extract_5_mins.name
    target_id = "extract_lambda_func"
    arn = aws_lambda_function.extract_lambda_func.arn
}

# permissions to check extract_lambda_func
resource "aws_lambda_permission" "allow_cloudwatch_execute_extract" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_lambda_func.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.trigger_extract_5_mins.arn
}