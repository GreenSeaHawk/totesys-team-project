resource "aws_ecr_repository" "transform_lambda_repo" {
  name = "transform_lambda_func"
  force_delete = true
}

output "transform_ecr_name" {
    value = aws_ecr_repository.transform_lambda_repo.name
  
}

output "transform_ecr_url" {
    value = aws_ecr_repository.transform_lambda_repo.repository_url
  
}


resource "aws_ecr_repository" "load_lambda_repo" {
  name = "load_lambda_func"
  force_delete = true
}

output "load_ecr_name" {
    value = aws_ecr_repository.load_lambda_repo.name
  
}

output "load_ecr_url" {
    value = aws_ecr_repository.load_lambda_repo.repository_url
  
}