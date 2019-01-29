import boto3
import botocore
from datetime import datetime, timedelta
from smart_open import smart_open

default_start_datetime = datetime(2011, 2, 12, 0)
default_end_datetime = datetime.utcnow()
dt = default_start_datetime

s3 = boto3.resource('s3')
b_name1 = 'ghalargefiletest'
b_name2 = 'gharchive'
large_json = 'ghalarge'

one_day = timedelta(days=1)
one_hour = timedelta(hours=1)


def s3_contains(s3, bucket_name, file_name):
    try:
        obj = s3.Object(bucket_name, file_name).load()
        return True
    except Exception as e:
        return False


while dt < default_end_datetime:
    year = str(dt.year)
    month = '%02d' % dt.month
    day = '%02d' % dt.day
    ymd = year + '-' + month + '-' + day
    counter = 0
    large_file_name = large_json + ymd + '.json'
    print(large_file_name)
    if s3_contains(s3, b_name1, large_file_name):
        dt += one_day
        print(dt)
    else:
        with smart_open('s3://%s/%s' % (b_name1, large_file_name), 'wb') as fout:
            temp_dt = dt
            dt += one_day
            while temp_dt < dt:
                hour = str(temp_dt.hour)
                print(temp_dt)
                file_name = ymd + '-' + hour + '.json'
                for li in smart_open('s3://%s/%s' % (b_name2, file_name), 'rb'):
                    fout.write(li)
                temp_dt += one_hour
