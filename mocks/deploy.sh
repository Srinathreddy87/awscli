#!/bin/bash

# Variables
SOURCE_DIR="path/to/your/source/directory"  # Directory containing the files to be copied
S3_BUCKET="your-s3-bucket-name"            # Your S3 bucket name
S3_PATH="path/in/s3/bucket"                # Path in the S3 bucket where files will be copied

# AWS CLI command to copy files to S3
aws s3 cp "$SOURCE_DIR" "s3://$S3_BUCKET/$S3_PATH" --recursive

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo "Files successfully copied to s3://$S3_BUCKET/$S3_PATH"
else
    echo "Failed to copy files to S3" >&2
    exit 1
fi
