# #create extract lambda role, and attaches policy to role
# resource "aws_iam_role" "load_lambda_role" {
#     name = "role-lambda"
#     assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
# }

# Secrets manager access for load function
data "aws_iam_policy_document" "load_lambda_secrets_access" {
    statement {
        actions = ["secretsmanager:GetSecretValue"]
       resources = ["arn:aws:secretsmanager:eu-west-2:039612847146:secret:my-datawarehouse-connection-D7yGQK"]
    }
}

resource "aws_iam_policy" "sm_policy" {
  name_prefix = "sm_access_permissions"
  policy = data.aws_iam_policy_document.load_lambda_secrets_access.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_sm_access" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.sm_policy.arn
}

# ADD permissions to upload transformed data to RDS warehouse - potentially not required

