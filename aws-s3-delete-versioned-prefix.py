#!/usr/bin/env python3

import sys
import boto3

if len(sys.argv) != 3:
    print("Usage: aws-s3-delete-versioned-prefix.py <bucket> <prefix>")
    sys.exit(1)

Bucket = sys.argv[1]
Prefix = sys.argv[2]

# Initialize the S3 client
s3 = boto3.client('s3')

IsTruncated = True
KeyMarker = None
VersionIdMarker = None

while IsTruncated:

    # Get the object versions.
    if KeyMarker is None and VersionIdMarker is None:
        objects = s3.list_object_versions(Bucket=Bucket, Prefix=Prefix)
    else:
        objects = s3.list_object_versions(Bucket=Bucket, Prefix=Prefix, KeyMarker=KeyMarker, VersionIdMarker=VersionIdMarker)

    IsTruncated = objects.get('IsTruncated', False)
    KeyMarker = objects.get('NextKeyMarker', None)
    VersionIdMarker = objects.get('NextVersionIdMarker', None)

    # Delete all versions.
    for obj in objects.get('Versions', []):
        print(f"Deleting: {obj['Key']};{obj['VersionId']}")
        s3.delete_object(Bucket=Bucket, Key=obj['Key'], VersionId=obj['VersionId'])

    # Delete all delete markers.
    for obj in objects.get('DeleteMarkers', []):
        print(f"Deleting: {obj['Key']};{obj['VersionId']}")
        s3.delete_object(Bucket=Bucket, Key=obj['Key'], VersionId=obj['VersionId'])
