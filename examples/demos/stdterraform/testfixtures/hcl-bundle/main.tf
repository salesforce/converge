# Sample Terraform module for the stdterraform demo. It creates a trivial, real (against
# LocalStack) AWS resource: an S3 bucket plus one object. The terraform worker packs
# this dir into a .tar.gz, uploads it to S3, and a `terraform` resource points at it;
# the worker pulls it, `terraform apply`s it, and captures the outputs below into
# the resource's status.
#
# NOTE: there is intentionally no `backend` block here — the terraform worker writes an
# S3 backend override at apply time (state placement is the engine's concern). The
# aws provider IS configured for LocalStack: the endpoint comes from the standard
# AWS_ENDPOINT_URL env the worker sets, and the skip_* flags stop the provider
# calling STS/IAM/EC2-metadata (which a minimal S3-only LocalStack doesn't run) to
# validate credentials. Credentials themselves come from the env (default chain).

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  # Talk to LocalStack without the credential/account probes a real endpoint has.
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  skip_region_validation      = true
  # LocalStack needs path-style S3 addressing (no per-bucket virtual hosts).
  s3_use_path_style = true
}

variable "name" {
  type        = string
  description = "Bucket name to create (must be globally unique within LocalStack)."
  default     = "converge-demo"
}

resource "aws_s3_bucket" "demo" {
  bucket = var.name
}

resource "aws_s3_object" "hello" {
  bucket  = aws_s3_bucket.demo.id
  key     = "hello.txt"
  content = "hello from converge — bucket ${var.name}"
}

output "bucket" {
  description = "The created bucket name."
  value       = aws_s3_bucket.demo.id
}

output "bucket_arn" {
  description = "The created bucket ARN."
  value       = aws_s3_bucket.demo.arn
}

output "object_key" {
  description = "The key of the object written into the bucket."
  value       = aws_s3_object.hello.key
}
