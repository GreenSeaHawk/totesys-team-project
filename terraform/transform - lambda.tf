# CHANGE FILE PATH NAMES WHEN KNOWN

resource "aws_lambda_function" "transform_lambda_func" {
    function_name = "transform_lambda_func"
    filename = "${path.module}/../lambda/transform/handler.py" #change depending on lambda transform file
    role = aws_iam_role.transform_lambda_role.arn
    handler = "handler.lambda_handler" # change depending on lambda transform file
    runtime = "python3.9"
    timeout = 10
    source_code_hash = data.archive_file.archive_transform_lambda.output_base64sha256
    layers = [aws_lambda_layer_version.transform_lambda_layer.arn]
}

# Zip transform lambda handler to local zip file
data "archive_file" "archive_transform_lambda" {
  type        = "zip"
  source_file = "${path.module}/../lambda/transform/handler.py"
  output_path = "${path.module}/../transform_lambda_func.zip"
}

# Zip transform layer requirements to local file
data "archive_file" "transform_requirements_layer" {
    type = "zip"
    source_dir = "${path.module}/../lambda/transform/" #complete this when path to layer known
    output_path = "${path.module}/../transform_lambda_layer.zip"
}

# Attach zipped transform layer to transform lambda func
resource "aws_lambda_layer_version" "transform_lambda_layer" {
  filename   = "${path.module}/../transform_lambda_layer.zip"
  layer_name = "transform_lambda_layer"
}
