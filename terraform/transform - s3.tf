# # Create the data bucket for the processed data to be put into
# resource "aws_s3_bucket" "totesys_transformed_data_bucket" {
#   bucket = "totesys-transformed-data-bucket"
#   force_destroy = true
# }

# # Create the folders for each table to be put into
# # This might not be necessary as when you save to a bucket
# # the key will include the entire path to file which would
# # include the folder names. Can remove if it creates
# # conflict but have left it in in case there are any
# # references to the folders in code somewhere.
# resource "aws_s3_object" "fact_sales_order" {
#   bucket = aws_s3_bucket.totesys_transformed_data_bucket.id
#   key = "fact_sales_order/"
# }

# resource "aws_s3_object" "dim_staff" {
#   bucket = aws_s3_bucket.totesys_transformed_data_bucket.id
#   key = "dim_staff/"
# }

# resource "aws_s3_object" "dim_location" {
#   bucket = aws_s3_bucket.totesys_transformed_data_bucket.id
#   key = "dim_location/"
# }

# resource "aws_s3_object" "dim_design" {
#   bucket = aws_s3_bucket.totesys_transformed_data_bucket.id
#   key = "dim_design/"
# }

# resource "aws_s3_object" "dim_date" {
#   bucket = aws_s3_bucket.totesys_transformed_data_bucket.id
#   key = "dim_date/"
# }

# resource "aws_s3_object" "dim_currency" {
#   bucket = aws_s3_bucket.totesys_transformed_data_bucket.id
#   key = "dim_currency/"
# }

# resource "aws_s3_object" "dim_counterparty" {
#   bucket = aws_s3_bucket.totesys_transformed_data_bucket.id
#   key = "dim_counterparty/"
# }