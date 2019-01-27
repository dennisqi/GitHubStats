import os
import datetime
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import col


class Calculator:

    def __init__(self, aws_id, aws_key,
                 calculated_file='../../data/calcluated.txt'):
        self.sc = SparkContext()
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
        self.hadoop_conf.set("fs.s3n.awsSecretAccessKey", aws_key)
        self.sqlContext = SQLContext(self.sc)
        self.calculated_file = calculated_file

    def get_json_path_from_s3(self, object):
        return 's3n://' + object.bucket_name + '/' + object.key

    def get_dataframe_from_json(self, json_path):
        return self.sqlContext.read.json(json_path)

    def parsed(self, string):
        date_time = string.split('-')
        return datetime.datetime(
            int(date_time[0]), int(date_time[1]),
            int(date_time[2]), int(date_time[3]))

    def get_date_time(self, object, last_calculated):
        file_name = object.key
        if object.key <= last_calculated.strip():
            return
        date_time, string = None, None
        file_name_splited = file_name.split('.')
        if file_name_splited:
            string = file_name_splited[0]
        if string:
            date_time = self.parsed(string)
        return date_time

    def get_last_calculated(self):
        if os.path.isfile(self.calculated_file):
            with open(self.calculated_file) as calculated:
                for line in calculated:
                    return line

    def calculate(self, object):
        if not object:
            return
        file_name = object.key
        last_calculated = self.get_last_calculated()
        date_time = self.get_date_time(object, last_calculated)
        if not date_time:
            return
        json_path = self.get_json_path_from_s3(object)
        df = self.get_dataframe_from_json(json_path)
        num_repos = 0
        try:
            df_columns = df.columns
            df_first = df.first()
            if 'type' in df_columns and df_first['type'] == 'Event':
                if 'payload' in df_columns \
                        and 'object' in df_first['payload']:
                    num_repos = df.filter(
                        col("payload")['object'] == 'repository'
                    ).count()
                else:
                    num_repos = df.filter(
                        col("payload")['ref_type'] == 'repository'
                    ).count()
            else:
                if 'payload' in df_columns and \
                        'object' in df_first['payload']:
                    num_repos = df.filter(
                        (col("payload")['object'] == 'repository') &
                        (col("type") == 'CreateEvent')
                    ).count()
                else:
                    num_repos = df.filter(
                        (col("payload")['ref_type'] == 'repository') &
                        (col("type") == 'CreateEvent')
                    ).count()
        except Exception as e:
            print('#'*100)
            print(e)
            print('#'*100)

        if num_repos:
            return {'date_time': date_time, 'num_repos': num_repos}
