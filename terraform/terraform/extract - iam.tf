
#create extract lambda role, and attaches policy to role
resource "aws_iam_role" "extract_lambda_role" {
    name = "role-lambda"
    assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}


#trust policy doc for lambda role
data "aws_iam_policy_document" "lambda_assume_role" {
    statement {
        effect = "Allow"

        principals {
            type = "Service"
            identifiers = ["lambda.amazonaws.com"]
          
        }
        actions = ["sts:AssumeRole"]
    }
}
################################################################################################################################################################################################################################
# s3 permissions
data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["s3:GetObject",
                "s3:ListObject",
                "s3:ListBucket",
                "s3:PutObject"]
    #"${aws_s3_bucket.totesys_transformed_data_bucket.arn}/*"
    resources = [
      "${aws_s3_bucket.totesys_data_bucket.arn}*"]
  }
}

resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-extract_lambda_func"
    policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

################################################################################################################################################################################################################################
# cloudwatch policy document
data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/extract_lambda_func:*"
    ]
  }
}


resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-extract_lambda_func"
    policy = data.aws_iam_policy_document.cw_document.json
}


resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

################################################################################################################################################################################################################################
#Secrets manager policy
data "aws_iam_policy_document" "extract_lambda_secrets_access" {
    statement {
        actions = ["secretsmanager:GetSecretValue"]
       resources = ["arn:aws:secretsmanager:eu-west-2:039612847146:secret:my-database-connection-K3XcnO"]
    }
}


resource "aws_iam_policy" "sm_policy" {
  name_prefix = "sm_access_permissions"
  policy = data.aws_iam_policy_document.extract_lambda_secrets_access.json
}

resource "aws_iam_role_policy_attachment" "extract_lambda_sm_access" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.sm_policy.arn
}
################################################################################################################################################################################################################################

# SNS permissions for Lambda to publish to SNS
data "aws_iam_policy_document" "sns_publish_document_extract" {
  statement {
    actions = ["sns:Publish"]
    resources = [
      aws_sns_topic.lambda_error_alert_extract.arn
    ]
  }
}

resource "aws_iam_policy" "sns_publish_policy_extract" {
  name_prefix = "sns-publish-policy"
  policy = data.aws_iam_policy_document.sns_publish_document_extract.json
}

resource "aws_iam_role_policy_attachment" "lambda_sns_publish_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.sns_publish_policy_extract.arn
}

