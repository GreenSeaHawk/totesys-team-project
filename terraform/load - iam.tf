# #create extract lambda role, and attaches policy to role
# resource "aws_iam_role" "load_lambda_role" {
#     name = "role-lambda"
#     assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
# }


# #ADD permissions to upload transformed data to RDS warehouse