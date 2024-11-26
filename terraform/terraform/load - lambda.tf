# # CHANGE FILE PATH NAMES WHEN KNOWN

# resource "aws_lambda_function" "load_lambda_func" {
#     function_name = "load_lambda_func"
#     filename = "${path.module}/../compressed_funcs/load_lambda_func.zip" #change depending on lambda load file
#     role = aws_iam_role.load_lambda_role.arn
#     handler = "handler.lambda_handler" # change depending on lambda load file
#     runtime = "python3.13"
#     timeout = 600
#     source_code_hash = data.archive_file.archive_load_lambda.output_base64sha256
#     layers = [aws_lambda_layer_version.load_lambda_layer.arn]
# }

# # Zip load lambda handler to local zip file
# data "archive_file" "archive_load_lambda" {
#   type        = "zip"
#   source_dir = "${path.module}/../lambda/load/src"
#   output_path = "${path.module}/../compressed_funcs/load_lambda_func.zip"
# }

# # Zip load layer requirements to local file
# data "archive_file" "load_requirements_layer" {
#     type = "zip"
#     source_dir = "${path.module}/../lambda/load/" #complete this when path to layer known
#     output_path = "${path.module}/../load_lambda_layer.zip"
# }

# # Attach zipped load layer to load lambda func
# resource "aws_lambda_layer_version" "load_lambda_layer" {
#   filename   = "${path.module}/../load_lambda_layer.zip"
#   layer_name = "load_lambda_layer"
# }


# # Trigger for load if something added to transformed data bucket
# resource "aws_s3_bucket_notification" "aws_load_lambda_trigger" {
#   bucket = aws_s3_bucket.totesys_transformed_data_bucket.id
#   lambda_function {
#     lambda_function_arn = aws_lambda_function.load_lambda_func.arn
#     events              = ["s3:ObjectCreated:*"]

#   }
# }

# # Permissions for s3 bucket to trigger load func
# resource "aws_lambda_permission" "allow_s3_bucket_to_execute_load" {
#   statement_id  = "AllowS3Invoke"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.load_lambda_func.function_name
#   principal     = "s3.amazonaws.com"
#   source_arn    = "arn:aws:s3:::${aws_s3_bucket.totesys_transformed_data_bucket.id}"
# }



resource "aws_lambda_function" "load_lambda_func" {
    function_name = "load_lambda_func"
    role = aws_iam_role.load_lambda_role.arn
    image_uri     = "${data.terraform_remote_state.ecr.outputs.load_ecr_url}@${data.aws_ecr_image.load_image.image_digest}"
    package_type = "Image"
    timeout = 600
    memory_size = 512
}

data "aws_ecr_image" "load_image" {
  repository_name =  data.terraform_remote_state.ecr.outputs.load_ecr_name
  image_tag       = "latest"
}
