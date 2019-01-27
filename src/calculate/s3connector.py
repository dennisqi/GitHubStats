import boto3


class S3Connector:

    def __init__(self, bucket_name):
        self.s3 = boto3.resource('s3')
        self.bucket_name = bucket_name

    def connect(self):
        return self.s3.Bucket(self.bucket_name)

    def get_bucket(self):
        return self.connect()

    def get_objects(self):
        bucket = self.get_bucket()
        for object in bucket.objects.all():
            yield object
