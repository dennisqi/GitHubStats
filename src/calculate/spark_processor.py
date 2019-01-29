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
        self.default_start_datetime = datetime.datetime(2011, 2, 12, 0)

    def process(self, bucket_name, file_name):
        """
        Given a bucket name and a file name,
            count the number of CreateEvents in the file.
        Returns a dict, contains date_time and num_create_events.
        """
        date_time = self.parse_datetime_from_filename(file_name)
        if not date_time:
            return

        df = self.get_dataframe_from_s3(bucket_name, file_name)
        num_create_events = 0

        # There are two versions of API for CreateEvent of repository:
        # - One is        col("payload")['object'] == 'repository'
        # - Another is    col("payload")['ref_type'] == 'repository'
        try:
            df_columns = df.columns
            df_first_record = df.first()
            keyword = 'object' if 'object' in df_first_record['payload'] else 'ref_type'
            num_create_events = \
                df.filter(col('payload')[keyword] == 'repository') \
                .filter((col('type') == 'CreateEvent') | (col('type') == 'Event')) \
                .count()

        except Exception as e:
            self.print_error(e)

        if num_create_events:
            return {
                'date_time': date_time,
                'num_create_events': num_create_events
            }

    def get_dataframe_from_s3(self, bucket_name, file_name):
        """
        Given a bucket name and file name,
        return a dataframe contains all records of a day
        """
        json_path = 's3n://' + bucket_name + '/' + file_name
        return self.sqlContext.read.json(json_path)

    def datetime_to_filename(self, head, date_time, tail):
        return head \
            + str(date_time.year) \
            + '-' + '%02d' % date_time.month \
            + '-' + '%02d' % date_time.day \
            + tail

    def parse_datetime_from_filename(self, file_name):
        """
        Given a file name, return a date_time the file_name stands for.
        For example gharchive2011-02-05.json -> datetime(2011, 02, 05)
        """
        date_time = None
        file_name_info_d = self.parse_file_name_info_d(file_name)
        print(file_name_info_d, file_name)
        try:
            date_time = datetime.datetime(
                int(file_name_info_d['year']),
                int(file_name_info_d['month']),
                int(file_name_info_d['day'])
            )

        except Exception as e:
            self.print_error(e)
        finally:
            return date_time

    def generate_processor_read_file(self, processor_read, processor_write, head, tail):
        """
        Given the processor_read file dir and processor_write file dir,
            generate new processor_read file that contains all file name
            between last wrote file in processor_write and today's dateself.
        """
        tail_line = self.get_head_tail_line('tail', processor_write)
        if tail_line:
            date_time = self.parse_datetime_from_filename(tail_line)
        else:
            date_time = self.default_start_datetime
        end_date_time = datetime.datetime.utcnow()
        oneday = datetime.timedelta(days=1)
        with open(processor_read, 'w') as to_write:
            while date_time < end_date_time:
                date_time += oneday
                to_write.write(self.datetime_to_filename(head, date_time, tail) + '\n')

    def get_lines_of_file(self, file):
        """Given a path to a file, generates all lines in the file.
        """
        if not os.path.isfile(file):
            open(file).close()
        with open(file, 'r') as f:
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

    def print_error(self, e):
        """Given a error message, highlight and print the messange
        """
        print('#'*100)
        print(e)
        print('#'*100)

    def parse_file_name_info_d(self, file_name):
        """
        Given a file name, return a dict contains
        {
            'string_before_date': 'gharchive',
            'year': '2011',
            'month': '02',
            'day': '06'
        }
        """
        return {
            'string_before_date': file_name[:-15],
            'year': file_name[-15:-11],
            'month': file_name[-10:-8],
            'day': file_name[-7:-5]
        }

    def append_to_file(self, file_append_to, data_to_append):
        with open(file_append_to, 'a') as write_file:
            write_file.write(data_to_append + '\n')
