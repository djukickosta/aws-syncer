import boto3
import click
import mimetypes

from botocore.exceptions import ClientError
from pathlib import Path

session = boto3.Session(profile_name='kosta')
s3 = session.resource('s3')

@click.group()
def cli():
  """AWS-Syncer syncs directories to AWS."""
  pass

@cli.command('list-buckets')
def list_buckets():
  """List all S3 buckets."""
  for bucket in s3.buckets.all():
    print(bucket)

@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket):
  """List objects in an S3 bucket."""
  for obj in s3.Bucket(bucket).objects.all():
    print(obj)

@cli.command('setup-bucket')
@click.argument('bucket')
def setup_bucket(bucket):
  """Create and configure S3 bucket."""
  pass

def upload_file(s3_bucket, path, key):
  content_type = mimetypes.guess_type(key)[0] or 'text/plain'
  s3_bucket.upload_file(
    path,
    key,
    ExtraArgs={
      'ContentType': 'text/html'
    }
  )

@cli.command('sync')
@click.argument('pathname', type=click.Path(exists=True))
@click.argument('bucket')
def sync(pathname, bucket):
  "Sync contents of PATHNAME to BUCKET."
  s3_bucket = s3.Bucket(bucket)

  root = Path(pathname).expanduser().resolve()

  def handle_directory(target):
			for path in target.iterdir():
				if path.is_dir(): handle_directory(path)
				if path.is_file(): upload_file(s3_bucket, str(path), str(path.relative_to(root)))

          #print("Path: {}\n Key: {}".format(path, path.relative_to(root)))

  handle_directory(root)

if __name__ == '__main__':
  cli()
