import os
import datetime
import subprocess
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql.functions import col


class SparkProcessor:

    def __init__(self, aws_id, aws_key, host, dbname, user, password, processor_write):
        sc_conf = SparkConf()
        sc_conf.setAppName('ghalarge_deployMode_cluster')
        sc_conf.setMaster(
            'spark://ec2-52-45-53-97.compute-1.amazonaws.com:7077')
        # sc_conf.set('spark.executor.memory', '6g')
        # sc_conf.set('spark.executor.cores', '6')
        # sc_conf.set('spark.driver.cores', '6')
        # sc_conf.set('spark.driver.memory', '6g')
        # sc_conf.set('spark.logConf', True)
        # sc_conf.set('spark.driver.extraClassPath', '../../lib/postgresql-42.2.5.jar')
        sc_conf.set('spark.submit.deployMode', 'cluster')
        # sc_conf.set('spark.python.worker.memory', '6g')
        # sc_conf.set('spark.files.maxPartitionBytes', '6g')
        sc = SparkContext(conf=sc_conf)
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
        hadoop_conf.set("fs.s3n.awsSecretAccessKey", aws_key)
        self.sqlContext = SQLContext(sc)
        self.default_start_datetime = datetime.datetime(2011, 2, 11, 0)
        print('Connecting to DB...')
        self.conn = psycopg2.connect(
            host=host, database=dbname, user=user, password=password)
        print('Connected.')
        self.processor_write = processor_write

    def write_to_db(self, wheather_create_table, create_table_sql, insert_sql, insert_param, file_name):
        """
        If wheather_create_table is True, execute create_table_sql.
        Eexecute insert_sql with insert_paramself.
        Append backup_sentence to self.backup_file.
        """
        cur = self.conn.cursor()
        if wheather_create_table:
            cur.execute(create_table_sql)
        self.conn.commit()

        try:
            cur.execute(insert_sql, insert_param)
        except psycopg2.IntegrityError as ie:
            print("Diplicate key.")
        else:
            with open(self.processor_write, 'a') as processor_write:
                processor_write.write(file_name + '\n')
                print('WROTE RECODR: ' + file_name)

        self.conn.commit()
        cur.close()

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

        rate_last_week = self.rate_last_week(date_time, num_create_events)

        if num_create_events:
            return {
                'date_time': date_time,
                'num_create_events': num_create_events,
                'rate_last_week': rate_last_week
            }

    def rate_last_week(self, date_time, num_create_events):
        """
        Given a datetime predict and num_create_events,
            calculate the increasing rate of today compare to last week
        """
        cur = self.conn.cursor()
        cur.execute(
            'SELECT num_create_events from num_repo_creation_v3 where date_time = %s',
            (date_time - timedelta(days=7)))
        self.conn.commit()
        result = cur.fetch_one()
        if not result:
            return 1.0
        return 1.0 + (num_create_events - float(result)) / float(result)

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
