terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "totesys-state-bucket-cimmeria"
    key = "totesys/terraform_ecr/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "Totesys Cimmeria Project"
      DeployedFrom = "Terraform"
      Repository   = "totesys-team-project"
    }
  }
}