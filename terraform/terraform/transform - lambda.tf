# # CHANGE FILE PATH NAMES WHEN KNOWN
resource "aws_lambda_function" "transform_lambda_func" {
    function_name = "transform_lambda_func"
    role = aws_iam_role.transform_lambda_role.arn
    image_uri     = "${data.terraform_remote_state.ecr.outputs.transform_ecr_url}@${data.aws_ecr_image.transform_image.image_digest}"
    package_type = "Image"
    timeout = 600
    memory_size = 512
}



data "aws_ecr_image" "transform_image" {
  repository_name = data.terraform_remote_state.ecr.outputs.transform_ecr_name
  image_tag       = "latest"
}

data "terraform_remote_state" "ecr" {
  backend = "s3"
  config = {
    bucket = "totesys-state-bucket-cimmeria"
    key    = "totesys/terraform_ecr/terraform.tfstate"
    region = "eu-west-2"
  }
}


# resource "aws_lambda_function" "transform_lambda_func" {
#     function_name = "transform_lambda_func"
#     filename = "${path.module}/../compressed_funcs/transform_lambda_func.zip" #change depending on lambda transform file
#     role = aws_iam_role.transform_lambda_role.arn
#     handler = "transform_handler.transform_handler" # change depending on lambda transform file
#     runtime = "python3.13"
#     timeout = 600
#     source_code_hash = data.archive_file.archive_transform_lambda.output_base64sha256
#     layers = [aws_lambda_layer_version.transform_lambda_layer_1.arn, aws_lambda_layer_version.transform_lambda_layer_2.arn ]
# }

# # Zip transform lambda handler to local zip file
# data "archive_file" "archive_transform_lambda" {
#   type        = "zip"
#   source_dir = "${path.module}/../lambda/transform/src"
#   output_path = "${path.module}/../compressed_funcs/transform_lambda_func.zip"
# }



# # Zip transform layer requirements to local file
# data "archive_file" "transform_requirements_layer" {
#     type = "zip"
#     source_dir = "${path.module}/../lambda/transform/" #complete this when path to layer known
#     output_path = "${path.module}/../transform_lambda_layer.zip"
# }

# # Attach zipped transform layer to transform lambda func
# resource "aws_lambda_layer_version" "transform_lambda_layer_1" {
#   filename   = "${path.module}/../layers/transform_1.zip"
#   layer_name = "transform_lambda_layer_1"
# }

# resource "aws_lambda_layer_version" "transform_lambda_layer_2" {
#   filename   = "${path.module}/../layers/transform_2.zip"
#   layer_name = "transform_lambda_layer_2"
# }


# # Trigger for transform if something added to raw data bucket
# resource "aws_s3_bucket_notification" "aws_transform_lambda_trigger" {
#   bucket = aws_s3_bucket.totesys_data_bucket.id
#   lambda_function {
#     lambda_function_arn = aws_lambda_function.transform_lambda_func.arn
#     events              = ["s3:ObjectCreated:*"]

#   }
# }

# # Permissions for s3 bucket to trigger transform func
# resource "aws_lambda_permission" "allow_s3_bucket_to_execute_transform" {
#   statement_id  = "AllowS3Invoke"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.transform_lambda_func.function_name
#   principal     = "s3.amazonaws.com"
#   source_arn    = "arn:aws:s3:::${aws_s3_bucket.totesys_data_bucket.id}"
# }

