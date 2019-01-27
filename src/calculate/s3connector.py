import boto3


class S3Connector:
    """
    Connect to AWS s3 bucket
    """
    def __init__(self, bucket_name):
        self.s3 = boto3.resource('s3')
        self.bucket_name = bucket_name

    def get_objects(self):
        bucket = self.s3.Bucket(self.bucket_name)
        for object in bucket.objects.all():
            yield object
