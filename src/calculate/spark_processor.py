import os
import datetime
import subprocess
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql.functions import col


class SparkProcessor:

    def __init__(self, aws_id, aws_key):
        sc_conf = SparkConf()
        sc_conf.setAppName('gha')
        sc_conf.setMaster(
            'spark://ec2-52-45-53-97.compute-1.amazonaws.com:7077')
        # sc_conf.set('spark.executor.memory', '6g')
        # sc_conf.set('spark.executor.cores', '6')
        # sc_conf.set('spark.driver.cores', '6')
        # sc_conf.set('spark.driver.memory', '6g')
        # sc_conf.set('spark.logConf', True)
        sc_conf.set('spark.submit.deployMode', 'client')
        # sc_conf.set('spark.python.worker.memory', '6g')
        # sc_conf.set('spark.files.maxPartitionBytes', '6g')
        sc = SparkContext(conf=sc_conf)
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
        hadoop_conf.set("fs.s3n.awsSecretAccessKey", aws_key)
        self.sqlContext = SQLContext(sc)

    def get_lines(self, file):
        """Given a path to a file, generates all lines in the file.
        """
        if not os.path.isfile(file):
            open(file).close()
        with open(file, r) as f:
            for line in f:
                yield line.strip()

    def get_head_tail_line(self, head_or_tail, file):
        """Given a path to a file, return the first or the last line.
        """
        if os.path.isfile(file):
            output = subprocess.Popen(
                [head_or_tail, "-n", "1", file],
                stdout=subprocess.PIPE
            ).communicate()[0]
            return output.strip()

    def get_dataframe(self, bucket_name, file_name):
        """
        Given a bucket name and file name,
        return a dataframe contains all records of a day
        """
        json_path = 's3n://' + bucket_name + '/' + file_name
        return self.sqlContext.read.json(json_path)

    def print_error(self, e):
        """Given a error message, highlight and print the messange
        """
        print('#'*100)
        print(e)
        print('#'*100)

    def get_date_time(self, file_name):
        """Given a file name, return a date_time the file_name stands for.
        """
        date_time = None
        date_time_raw = file_name[-15:-5]
        try:
            date_time = datetime.datetime(
                date_time_raw[:4],
                date_time_raw[5:7],
                date_time_raw[8:]
            )
        except Exception as e:
            self.print_error(e)
        finally:
            return date_time

    def process(self, bucket_name, file_name):
        """
        Given a bucket name and a file name,
            count the number of CreateEvents in the file.
        Returns a dict, contains date_time and num_create_events.
        """
        date_time = self.get_date_time(file_name)
        if not date_time:
            return

        df = self.get_dataframe(bucket_name, file_name)
        num_create_events = 0
        try:
            # There are two versions of API for CreateEvent:
            # - One is        col("payload")['object'] == 'repository'
            # - Another is    col("payload")['ref_type'] == 'repository'
            df_columns = df.columns
            df_first = df.first()
            keyword = 'object' if 'object' in df_first['payload'] else 'ref_type'
            num_create_events = \
                df.filter(col('payload')[keyword] == 'repository') \
                .filter(col('type') == 'CreateEvent' | col('type') == 'Event') \
                .count()

        except Exception as e:
            self.print_error(e)

        if num_create_events:
            return {
                'date_time': date_time,
                'num_create_events': num_create_events
            }
