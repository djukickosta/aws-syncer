# -*- coding: utf-8 -*-

"""Classes for S3 buckets."""

import boto3
import mimetypes

from botocore.exceptions import ClientError
from functools import reduce
from hashlib import md5
from pathlib import Path

class BucketManager:
  """Manage an S3 bucket."""

  CHUNK_SIZE = 8 * 1024 * 1024

  def __init__(self, session):
    self.session = session
    self.s3 = session.resource('s3')
    self.transfer_config = boto3.s3.transfer.TransferConfig(
			multipart_chunksize = self.CHUNK_SIZE,
			multipart_threshold = self.CHUNK_SIZE
		)

    self.manifest = {}

  def all_buckets(self):
    """Get an iterator for all buckets."""
    return self.s3.buckets.all()

  def all_objects(self, bucket):
    """Get an iterator for all objects in the bucket."""
    return self.s3.Bucket(bucket).objects.all()

  def init_bucket(self, bucket_name):
    """Create new bucket, or return existing one"""
    s3_bucket = None
    try:
      if self.session.region_name == 'us-east-1':
        s3_bucket = self.s3.create_bucket(
          Bucket=bucket_name
        )
      else:
        s3_bucket = self.s3.create_bucket(
          Bucket=bucket_name,
          CreateBucketConfiguration={'LocationConstraint': self.session.region_name}
        )
    except ClientError as error:
      if error.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        print("You already own this S3 bucket.")
        #s3_bucket = self.s3.Bucket(bucket_name)
      elif error.response['Error']['Code'] == 'BucketAlreadyExists':
        print("The S3 bucket name is not available.")
      else:
        raise error

    return s3_bucket

  def load_manifest(self, bucket):
    """Load manifest for caching purposes."""
    paginator = self.s3.meta.client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket.name):
      for obj in page.get('Contents', []):
        self.manifest[obj['Key']] = obj['ETag']

  @staticmethod
  def hash_data(data):
    """Generate md5 hash for data."""
    hash = md5()
    hash.update(data)

    return hash

  def generate_etag(self, path):
    """Generate ETag for file to upload."""
    hashes = []

    with open(path, 'rb') as f:
      while True:
        data = f.read(self.CHUNK_SIZE)

        if not data:
          break

        hashes.append(self.hash_data(data))

    if not hashes:
      print("Not hashes")
      return
    elif len(hashes) == 1:
      return '"{}"'.format(hashes[0].hexdigest())
    else:
      digests = (h.digest() for h in hashes)
      hash = self.hash_data(reduce(lambda x, y: x + y, digests))
      return '"{}-{}"'.format(hash.hexdigest(), len(hashes))

  def upload_file(self, bucket, path, key):
    """Upload path to S3 bucket at key."""
    content_type = mimetypes.guess_type(key)[0] or 'text/plain'

    etag = self.generate_etag(path)
    if self.manifest.get(key, '') == etag:
      print("Skipping {}, etags match".format(key))
      return

    return bucket.upload_file(
      path,
      key,
      ExtraArgs={
        'ContentType': content_type
      },
      Config=self.transfer_config
    )

  def sync(self, pathname, bucket_name):
    """Sync contents of path to bucket."""
    bucket = self.s3.Bucket(bucket_name)
    self.load_manifest(bucket)

    root = Path(pathname).expanduser().resolve()

    def handle_directory(target):
      for path in target.iterdir():
        if path.is_dir():
          handle_directory(path)
        if path.is_file():
          print("Path: {}\n Key: {}".format(path, path.relative_to(root)))
          self.upload_file(bucket, str(path), str(path.relative_to(root)))

    handle_directory(root)