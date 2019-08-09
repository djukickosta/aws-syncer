# -*- coding: utf-8 -*-

"""Classes for S3 buckets."""

import boto3
import mimetypes

from botocore.exceptions import ClientError
from pathlib import Path

class BucketManager:
  """Manage an S3 bucket."""

  def __init__(self, session):
    self.s3 = session.resource('s3')

  def all_buckets(self):
    """Get an iterator for all buckets."""
    return self.s3.buckets.all()

  def all_objects(self, bucket):
    """Get an iterator for all objects in the bucket."""
    return self.s3.Bucket(bucket).objects.all()

  def upload_file(self, bucket, path, key):
    """Upload path to s3_bucket at key."""
    content_type = mimetypes.guess_type(key)[0] or 'text/plain'
    return bucket.upload_file(
      path,
      key,
      ExtraArgs={
        'ContentType': content_type
      }
    )

  def sync(self, pathname, bucket_name):
    """Sync contents of path to bucket."""
    bucket = self.s3.Bucket(bucket_name)

    root = Path(pathname).expanduser().resolve()

    def handle_directory(target):
      for path in target.iterdir():
        if path.is_dir(): handle_directory(path)
        if path.is_file():
          print("Path: {}\n Key: {}".format(path, path.relative_to(root)))
          self.upload_file(bucket, str(path), str(path.relative_to(root)))

    handle_directory(root)