#create extract lambda role, and attaches policy to role
resource "aws_iam_role" "load_lambda_role" {
    name = "load-lambda-role"
    assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

# Secrets manager access for load function
data "aws_iam_policy_document" "load_lambda_secrets_access" {
    statement {
        actions = ["secretsmanager:GetSecretValue"]
       resources = ["arn:aws:secretsmanager:eu-west-2:039612847146:secret:my-datawarehouse-connection-D7yGQK"]
    }
}

resource "aws_iam_policy" "sm_policy_load" {
  name_prefix = "sm_access_permissions"
  policy = data.aws_iam_policy_document.load_lambda_secrets_access.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_sm_access" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.sm_policy_load.arn
}

resource "aws_iam_role_policy_attachment" "lambda_ecr_policy_load" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

data "aws_iam_policy_document" "cw_document_load" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]
    #"arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/transform_lambda_func:*"
    resources = ["*"
      
    ]
  }
}


resource "aws_iam_policy" "cw_policy_load" {
    name_prefix = "cw-policy-load_lambda_func"
    policy = data.aws_iam_policy_document.cw_document_load.json
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment_load" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.cw_policy_load.arn
}
# ADD permissions to upload transformed data to RDS warehouse - potentially not required

