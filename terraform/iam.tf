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

resource "aws_iam_role" "lambda_role" {
    name = "role-lambda"
    assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}