# -*- coding: utf-8 -*-

"""
AWS-Syncer: Syncs directories to AWS.

AWS-Syncer is a CLI tool that assists with the following processes in AWS:
-List S3 buckets
-List contents of an S3 bucket
-Create an S3 bucket
-Sync directory tree and its contents to an S3 bucket
"""

import boto3
import click

from bucket import BucketManager

session = boto3.Session(profile_name='kosta')
bucket_manager = BucketManager(session)

@click.group()
def cli():
  """AWS-Syncer syncs directories to AWS."""
  pass

@cli.command('list-buckets')
def list_buckets():
  """List all S3 buckets."""
  for bucket in bucket_manager.all_buckets():
    print(bucket)

@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket):
  """List objects in an S3 bucket."""
  for obj in bucket_manager.all_objects(bucket):
    print(obj)

@cli.command('setup-bucket')
@click.argument('bucket')
def setup_bucket(bucket):
  """Create and configure S3 bucket."""
  pass

@cli.command('sync')
@click.argument('pathname', type=click.Path(exists=True))
@click.argument('bucket')
def sync(pathname, bucket):
  "Sync contents of PATHNAME to BUCKET."
  bucket_manager.sync(pathname, bucket)

if __name__ == '__main__':
  cli()
