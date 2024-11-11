#policy doc for lambda role
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

#create extract lambda role, and attaches policy to role
resource "aws_iam_role" "extract_lambda_role" {
    name = "role-lambda"
    assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

# extract lambda policy document - putobject
data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["s3:PutObject"]

    resources = [
      "${aws_s3_bucket.totesys_data_bucket.arn}/*"
    ]
  }
}

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

resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-extract_lambda_func"
    policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-extract_lambda_func"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}