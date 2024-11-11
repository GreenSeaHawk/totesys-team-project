terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "Totesys Group Project"
      DeployedFrom = "Terraform"
      Repository   = "totesys-team-project"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}