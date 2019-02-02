import os
import psycopg2
import datetime
import subprocess
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql.functions import col, datediff, to_date, date_add, coalesce


class SparkProcessor:

    def __init__(self, aws_id, aws_key, host, dbname, user, password, processor_write):
    #     sc_conf = SparkConf()
    #     sc_conf.setAppName('ghalarge_deployMode_cluster_rate_last_week_w_dict_6G')
    #     sc_conf.setMaster(
    #         'spark://ec2-52-45-53-97.compute-1.amazonaws.com:7077')
    #     sc_conf.set('spark.executor.memory', '6g')
    #     sc_conf.set('spark.submit.deployMode', 'cluster')
    #     # sc_conf.set('spark.driver.extraClassPath', '~/GitHubStats/lib/postgresql-42.2.5.jar')
    #     # sc_conf.set('spark.jars', 'org.postgresql:postgresql:42.2.2')
    #     sc_conf.set('spark.jars', '../../lib/postgresql-42.2.5.jar')
    #
    #     sc = SparkContext(conf=sc_conf)
    #     self.accum = sc.accumulator(0)
    #     hadoop_conf = sc._jsc.hadoopConfiguration()
    #     hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
    #     hadoop_conf.set("fs.s3n.awsSecretAccessKey", aws_key)
    #     self.sqlContext = SQLContext(sc)
    #
    #     self.default_start_datetime = datetime.datetime(2011, 2, 11, 0)
    #     self.processor_write = processor_write
    #     self.seven_days = {}
    #     self.mode = 'overwrite'
    #
    #     # Connect to DB
        print('Connecting to DB...')
        self.conn = psycopg2.connect(host=host, database=dbname, user=user, password=password)
    #     self.postgres_url = 'jdbc:postgresql://%s/%s' % (host, dbname)
    #     self.properties = {'user': user, 'password': password} #, "driver": "org.postgresql.Driver"}
        print('Connected.')
    #
    # def df_write_to_db(self, df, table_name, mode='ignore'):
    #     df.write \
    #         .format("jdbc") \
    #         .option("driver", "org.postgresql.Driver") \
    #         .option("url", self.postgres_url) \
    #         .option("dbtable", table_name) \
    #         .option("user", self.properties['user']) \
    #         .option("password", self.properties['password']) \
    #         .save()
    #
    # def write_to_db(self, wheather_create_table, create_table_sql, insert_sql, insert_param, file_name):
    #     """
    #     If wheather_create_table is True, execute create_table_sql.
    #     Eexecute insert_sql with insert_paramself.
    #     Append backup_sentence to self.backup_file.
    #     """
    #     cur = self.conn.cursor()
    #     if wheather_create_table:
    #         cur.execute(create_table_sql)
    #     self.conn.commit()
    #
    #     try:
    #         cur.execute(insert_sql, insert_param)
    #     except psycopg2.IntegrityError as ie:
    #         print("Diplicate key.")
    #     else:
    #         with open(self.processor_write, 'a') as processor_write:
    #             processor_write.write(file_name + '\n')
    #             print('WROTE RECODR: ' + file_name)
    #
    #     self.conn.commit()
    #     cur.close()
    #
    # def read_all_to_df(self, bucket_name):
    #     """Given a bucket name read all file on that bucket to a df
    #     """
    #     df = self.sqlContext.read.json('s3n://' + bucket_name + '/')
    #     return df

    def read_files_to_df(self, urls):
        df = self.sqlContext.read.json(urls)
        return df
    #
    # def process_df(self, df):
    #     # There are two versions of API for CreateEvent of repository:
    #     # - One is        col("payload")['object'] == 'repository'
    #     # - Another is    col("payload")['ref_type'] == 'repository'
    #     # try:
    #     df_columns = df.columns
    #     df_first_record = df.first()
    #     keyword = 'object' if 'object' in df_first_record['payload'] else 'ref_type'
    #
    #     num_create_events_df = \
    #         df \
    #         .filter(col('payload')[keyword] == 'repository') \
    #         .filter((col('type') == 'CreateEvent') | (col('type') == 'Event'))
    #
    #     num_create_events_by_date_df = num_create_events_df.groupby(to_date(df.created_at).alias('date_created_at')).count()
    #
    #     num_create_events_by_date_df_1 = num_create_events_by_date_df.alias('num_create_events_by_date_df_1')
    #
    #     num_create_events_by_date_df_1 = \
    #         num_create_events_by_date_df_1 \
    #         .select(
    #             col('date_created_at').alias('date_created_at_1'),
    #             col('count').alias('count_1'))
    #
    #     num_create_events_by_date_df_2 = num_create_events_by_date_df.alias('num_create_events_by_date_df_2')
    #
    #     num_create_events_by_date_df_2 = \
    #         num_create_events_by_date_df_2 \
    #         .select(
    #             col('date_created_at').alias('date_created_at_2'),
    #             col('count').alias('count_2'))
    #
    #     joined_num_create_events_df = \
    #         num_create_events_by_date_df_1 \
    #         .withColumn(
    #             'last_week_date_created_at',
    #             date_add(num_create_events_by_date_df_1.date_created_at_1, -7)) \
    #         .join(
    #             num_create_events_by_date_df_2,
    #             col('last_week_date_created_at')
    #             == col('date_created_at_2'),
    #             how='left_outer')
    #
    #     joined_num_create_events_df = joined_num_create_events_df.withColumn(
    #         'count_2', coalesce('count_2', 'count_1'))
    #
    #     num_create_events_with_growth_rate_df = \
    #         joined_num_create_events_df \
    #         .withColumn(
    #             'weekly_increase_rate',
    #             ((joined_num_create_events_df.count_1 - joined_num_create_events_df.count_2) / joined_num_create_events_df.count_2)
    #         ) \
    #         .select(
    #             'date_created_at_1',
    #             'count_1',
    #             'weekly_increase_rate')
    #
    #     num_create_events_with_growth_rate_df.show()
    #
    #     return num_create_events_with_growth_rate_df
    #     # except Exception as e:
    #     #     self.print_error(e)
    #
    # def get_num_created_repo(self, table_name, date_time):
    #     cur = self.conn.cursor()
    #     cur.execute(
    #         'select count_1 from %s where date_created_at_1 = %s',
    #         (table_name, date_time))
    #     conn.commit()
    #     result_count = cur.fetchone()[0]
    #     cur.close()
    #     return result_count

    def process_present_df(self, present_df, table_name):
        df_columns = present_df.columns
        df_first_record = present_df.first()
        keyword = 'object' if 'object' in df_first_record['payload'] else 'ref_type'

        num_create_events_df = \
            present_df \
            .filter(col('payload')[keyword] == 'repository') \
            .filter((col('type') == 'CreateEvent') | (col('type') == 'Event'))

        num_create_events_by_date_df = \
            num_create_events_df \
            .groupby(to_date(present_df.created_at).alias('date_created_at')) \
            .count()

        return num_create_events_by_date_df.withColumn(
            'weekly_increase_rate',
            self.get_num_created_repo(table_name, date_add(num_create_events_by_date_df.date_created_at, -7))
            # self.sqlContext
            #     .read
            #     .format("jdbc")
            #     .option("driver", "org.postgresql.Driver")
            #     .option("url", self.postgres_url)
            #     .option("dbtable", "select count_1 from %s where date_created_at_1 = %s" % (table_name, date_add(num_create_events_by_date_df.date_created_at, -7)))
            #     .option("user", self.properties['user'])
            #     .option("password", self.properties['password'])
            #     .load()
            )
    #
    # def process(self, bucket_name, file_name, wheather_create_table, create_table_sql):
    #     """
    #     Given a bucket name and a file name,
    #         count the number of CreateEvents in the file.
    #     Returns a dict, contains date_time and num_create_events.
    #     """
    #     self.accum.add(1)
    #     print(self.accum)
    #     date_time = self.parse_datetime_from_filename(file_name)
    #     if not date_time:
    #         return
    #
    #     df = self.get_dataframe_from_s3(bucket_name, file_name)
    #     num_create_events = 0
    #
    #     # There are two versions of API for CreateEvent of repository:
    #     # - One is        col("payload")['object'] == 'repository'
    #     # - Another is    col("payload")['ref_type'] == 'repository'
    #     try:
    #         df_columns = df.columns
    #         df_first_record = df.first()
    #         keyword = 'object' if 'object' in df_first_record['payload'] else 'ref_type'
    #         num_create_events = \
    #             df.filter(col('payload')[keyword] == 'repository') \
    #             .filter((col('type') == 'CreateEvent') | (col('type') == 'Event')) \
    #             .count()
    #
    #     except Exception as e:
    #         self.print_error(e)
    #
    #     rate_last_week = self.rate_last_week(date_time, num_create_events, wheather_create_table, create_table_sql)
    #
    #     if num_create_events:
    #         return {
    #             'date_time': date_time,
    #             'num_create_events': num_create_events,
    #             'rate_last_week': rate_last_week
    #         }

    def read_all_sep_comma(self, spark_recent_todo_files):
        if not os.path.isfile(spark_recent_todo_files):
            open(spark_recent_todo_files, 'w').close()
        result = ''
        with open(spark_recent_todo_files) as fin:
            for url in fin:
                result += url.strip() + ','
        return result
    #
    # def rate_last_week(self, date_time, num_create_events, wheather_create_table, create_table_sql):
    #     """
    #     Given a datetime predict and num_create_events,
    #         calculate the increasing rate of today compare to last week
    #     """
    #     date_time_7 = date_time - datetime.timedelta(days=7)
    #     self.seven_days[date_time] = num_create_events
    #     if date_time_7 not in self.seven_days:
    #         cur = self.conn.cursor()
    #         if wheather_create_table:
    #             cur.execute(create_table_sql)
    #         self.conn.commit()
    #         cur.execute(
    #             'SELECT num_creation from num_repo_creation_v3 where date_time = %s',
    #             (date_time_7,))
    #         self.conn.commit()
    #         result = cur.fetchone()
    #         if not result:
    #             return 1.0
    #         result = result[0]
    #         return 1.0 + (num_create_events - float(result)) / float(result)
    #     result = self.seven_days.pop(date_time_7)
    #     return 1.0 + (num_create_events - float(result)) / float(result)
    #
    # def get_dataframe_from_s3(self, bucket_name, file_name):
    #     """
    #     Given a bucket name and file name,
    #     return a dataframe contains all records of a day
    #     """
    #     json_path = 's3n://' + bucket_name + '/' + file_name
    #     return self.sqlContext.read.json(json_path)
    #
    # def datetime_to_filename(self, head, date_time, tail):
    #     return head \
    #         + str(date_time.year) \
    #         + '-' + '%02d' % date_time.month \
    #         + '-' + '%02d' % date_time.day \
    #         + tail
    #
    # def parse_datetime_from_filename(self, file_name):
    #     """
    #     Given a file name, return a date_time the file_name stands for.
    #     For example gharchive2011-02-05.json -> datetime(2011, 02, 05)
    #     """
    #     date_time = None
    #     file_name_info_d = self.parse_file_name_info_d(file_name)
    #     print(file_name_info_d, file_name)
    #     try:
    #         date_time = datetime.datetime(
    #             int(file_name_info_d['year']),
    #             int(file_name_info_d['month']),
    #             int(file_name_info_d['day']), 0
    #         )
    #
    #     except Exception as e:
    #         self.print_error(e)
    #     finally:
    #         return date_time
    #
    # def generate_processor_read_file(self, processor_read, processor_write, head, tail):
    #     """
    #     Given the processor_read file dir and processor_write file dir,
    #         generate new processor_read file that contains all file name
    #         between last wrote file in processor_write and today's date.
    #     """
    #     tail_line = self.get_head_tail_line('tail', processor_write)
    #     if tail_line:
    #         date_time = self.parse_datetime_from_filename(tail_line)
    #     else:
    #         date_time = self.default_start_datetime
    #     end_date_time = datetime.datetime.utcnow()
    #     oneday = datetime.timedelta(days=1)
    #     with open(processor_read, 'w') as to_write:
    #         while date_time < end_date_time:
    #             date_time += oneday
    #             to_write.write(self.datetime_to_filename(head, date_time, tail) + '\n')

    def generate_todo_urls(self, spark_recent_todo_files, table_name, bucket_name, start_date=datetime.datetime.utcnow()):
        one_day = datetime.timedelta(days=1)
        utc_now = start_date
        with open(spark_recent_todo_files, 'w') as fout:
            last_date = None
            while not last_date:
                # get last processed date
                cur = self.conn.cursor()
                cur.execute(
                    'select exists(select 1 from ' + table_name + ' where date_created_at_1 = %s)',
                    (start_date,))
                last_date = cur.fetchone()[0]
                start_date -= one_day
            while start_date < utc_now:
                start_date += one_day
                fout.write('s3n://' + bucket_name + '/' + start_date.strftime("%Y-%m-%d*") + '\n')

    # def get_lines_of_file(self, file):
    #     """Given a path to a file, generates all lines in the file.
    #     """
    #     if not os.path.isfile(file):
    #         open(file).close()
    #     with open(file, 'r') as f:
    #         for line in f:
    #             yield line.strip()
    #
    # def get_head_tail_line(self, head_or_tail, file):
    #     """Given a path to a file, return the first or the last line.
    #     """
    #     if os.path.isfile(file):
    #         output = subprocess.Popen(
    #             [head_or_tail, "-n", "1", file],
    #             stdout=subprocess.PIPE
    #         ).communicate()[0]
    #         return output.strip()
    #
    # def print_error(self, e):
    #     """Given a error message, highlight and print the messange
    #     """
    #     print('#'*100)
    #     print(e)
    #     print('#'*100)
    #
    # def parse_file_name_info_d(self, file_name):
    #     """
    #     Given a file name, return a dict contains
    #         'string_before_date', 'year', month', day'
    #     """
    #     return {
    #         'string_before_date': file_name[:-15],
    #         'year': file_name[-15:-11],
    #         'month': file_name[-10:-8],
    #         'day': file_name[-7:-5]
    #     }
