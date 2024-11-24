#create extract lambda role, and attaches policy to role
resource "aws_iam_role" "transform_lambda_role" {
    name = "transform-role-lambda"
    assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

####################################################################################################################################################################################################
#ECR permission
resource "aws_iam_role_policy_attachment" "lambda_ecr_policy" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}


################################################################################################################################################################################################################################
# s3 permissions
data "aws_iam_policy_document" "s3_document_transform" {
  statement {

    actions = ["s3:GetObject",
                "s3:ListObject",
                "s3:ListBucket",
                "s3:PutObject"]
    resources = [
      "${aws_s3_bucket.totesys_data_bucket.arn}*", "${aws_s3_bucket.totesys_transformed_data_bucket.arn}*"]
  }
}

resource "aws_iam_policy" "s3_policy_transform" {
    name_prefix = "s3-policy-extract_lambda_func"
    policy = data.aws_iam_policy_document.s3_document_transform.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment_tansform" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.s3_policy_transform.arn
}

################################################################################################################################################################################################################################
# cloudwatch policy document


# cloudwatch policy document
data "aws_iam_policy_document" "cw_document_transform" {
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


resource "aws_iam_policy" "cw_policy_transform" {
    name_prefix = "cw-policy-extract_lambda_func"
    policy = data.aws_iam_policy_document.cw_document_transform.json
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment_transform" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.cw_policy_transform.arn
}
