resource "aws_lambda_function" "extract_lambda_func" {
    function_name = "extract_lambda_func"
    filename = "path to file"
    role = aws_iam_role.lambda_role.arn
    handler = "handler.lambda_handler" # change depending on lambda extract file
    runtime = "python3.9"
    timeout = 10
    source_code_hash = data.archive_file.archive_extract_lambda.output_base64sha256
}

data "archive_file" "archive_extract_lambda" {
  type        = "zip"
  source_file = "${path.module}/../lambda/extract/handler.py"
  output_path = "${path.module}/../extract_lambda_func.zip"
}