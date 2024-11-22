#create extract lambda role, and attaches policy to role
resource "aws_iam_role" "transform_lambda_role" {
    name = "transform-role-lambda"
    assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
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
      "${aws_s3_bucket.totesys_data_bucket.arn}/*", "${aws_s3_bucket.totesys_transformed_data_bucket.arn}/*"]
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

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment_transform" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}