
# Create the data bucket for the raw data to be put into
resource "aws_s3_bucket" "totesys_data_bucket" {
  bucket = "totesys_data_bucket_cimmeria"
  force_destroy = true
}

# Create the folders for each table to be put into
# This might not be necessary as when you save to a bucket
# the key will include the entire path to file which would
# include the folder names. Can remove if it creates
# conflict but have left it in in case there are any
# references to the folders in code somewhere.
resource "aws_s3_object" "counterparty" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "counterparty/"
}

resource "aws_s3_object" "currency" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "currency/"
}

resource "aws_s3_object" "department" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "department/"
}

resource "aws_s3_object" "design" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "design/"
}

resource "aws_s3_object" "staff" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "staff/"
}

resource "aws_s3_object" "sales_order" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "sales_order/"
}

resource "aws_s3_object" "address" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "address/"
}

resource "aws_s3_object" "payment" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "payment/"
}

resource "aws_s3_object" "purchase_order" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "purchase_order/"
}

resource "aws_s3_object" "payment_type" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "payment_type/"
}

resource "aws_s3_object" "transaction" {
  bucket = aws_s3_bucket.totesys_data_bucket.id
  key = "transaction/"
}