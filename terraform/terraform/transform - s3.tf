# Create the data bucket for the processed data to be put into
resource "aws_s3_bucket" "totesys_transformed_data_bucket" {
  bucket = "totesys-transformed-data-bucket"
  force_destroy = true
}