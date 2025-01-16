#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Variables
FILE_PATH="files/story.yml"
S3_BUCKET="your-s3-bucket-name"
S3_KEY="path/in/bucket/story.yml"

# Update the file (example: append a timestamp)
echo "Updating $FILE_PATH"
echo "Updated on $(date)" >> "$FILE_PATH"

# Upload the file to S3
echo "Uploading $FILE_PATH to s3://$S3_BUCKET/$S3_KEY"
aws s3 cp "$FILE_PATH" "s3://$S3_BUCKET/$S3_KEY"

echo "File uploaded successfully."
