import os
import time
import json
import psycopg2
import datetime
import subprocess
from smart_open import smart_open
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col, datediff, to_date, date_add, coalesce, date_trunc, add_months


class SparkProcessor:

    def __init__(self, aws_id, aws_key, host, dbname, user, password, app_name):
        sc_conf = SparkConf()
        sc_conf.setAppName(app_name)
        sc_conf.setMaster(
            'spark://ec2-52-45-53-97.compute-1.amazonaws.com:7077')
        sc_conf.set('spark.executor.memory', '1g')
        sc_conf.set('spark.submit.deployMode', 'cluster')
        sc_conf.set('spark.executor.cores', 1)
        sc_conf.set('spark.jars', '../../lib/postgresql-42.2.5.jar')

        sc = SparkContext(conf=sc_conf)
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
        hadoop_conf.set("fs.s3n.awsSecretAccessKey", aws_key)
        self.sqlContext = SQLContext(sc)

        self.default_start_datetime = datetime.datetime(2011, 2, 11, 0)
        self.seven_days = {}
        self.mode = 'overwrite'

        # Connect to DB
        print('Connecting to DB...')
        self.conn = psycopg2.connect(host=host, database=dbname, user=user, password=password)
        self.postgres_url = 'jdbc:postgresql://%s/%s' % (host, dbname)
        self.properties = {'user': user, 'password': password, 'driver': "org.postgresql.Driver"}
        print('Connected.')

        self.schema = StructType([
            StructField('created_at', StringType(), True),
            StructField(
                'payload',
                StructType([
                    StructField('object', StringType(), True),
                    StructField('ref_type', StringType(), True)
                ])
                , True),
            StructField('type', StringType(), True)
        ])

    def df_jdbc_write_to_db(self, df, table_name, mode='append'):
        """
        Write dataframe directlly to postgres using jdbc
        """
        df.write \
            .format("jdbc") \
            .mode(mode) \
            .option("driver", self.properties['driver']) \
            .option("url", self.postgres_url) \
            .option("dbtable", table_name) \
            .option("user", self.properties['user']) \
            .option("password", self.properties['password']) \
            .save()

    def df_psycopg2_write_to_db(self, insert_sql, insert_param):
        """
        If wheather_create_table is True, execute create_table_sql.
        Eexecute insert_sql with insert_paramself.
        Append backup_sentence to self.backup_file.
        """
        cur = self.conn.cursor()
        try:
            cur.execute(insert_sql, insert_param)
        except psycopg2.IntegrityError as ie:
            print("Diplicate key.")

        self.conn.commit()
        cur.close()

    def read_all_to_df(self, bucket_name, path):
        """
        Given a bucket name read all file on that bucket to a df
        """
        print(path)
        df = self.sqlContext.read.json(path, self.schema)
        return df

    def read_files_to_df(self, urls):
        """
        Given a list of s3 urls, return a dataframe generated by sqlContext
        """
        df = self.sqlContext.read.json(urls, self.schema)
        return df

    def process_history_df(self, df):
        """
        Process function for history data, generate result dataframe
        that contains date, number of create events and
        growth rate of a day compare to last week
        """
        # There are two versions of API for CreateEvent of repository:
        # - One is        col("payload")['object'] == 'repository'
        # - Another is    col("payload")['ref_type'] == 'repository'
        # try:
        df_columns = df.columns
        df_first_record = df.first()
        keyword = 'object' if 'object' in df_first_record['payload'] else 'ref_type'

        num_create_events_df = \
            df \
            .filter(col('payload')[keyword] == 'repository') \
            .filter((col('type') == 'CreateEvent') | (col('type') == 'Event'))

        # count the number of create events happened in one day (group by date)
        num_create_events_by_date_df = num_create_events_df.groupby(to_date(df.created_at).alias('date_created_at')).count()

        # calculate the grawth rate of that day compare to last week
        # dulicated two dataframes, for each day in the first dataframe
        # find the number fo create events in the second dataframe
        # of a day that is 7 days before the day in the first dataframe
        # [df1] 2015-01-07 -> [df2] 2015-01-01 (7 days)
        num_create_events_by_date_df_1 = num_create_events_by_date_df.alias('num_create_events_by_date_df_1')

        num_create_events_by_date_df_1 = \
            num_create_events_by_date_df_1 \
            .select(
                col('date_created_at').alias('date_created_at_1'),
                col('count').alias('count_1'))

        num_create_events_by_date_df_2 = num_create_events_by_date_df.alias('num_create_events_by_date_df_2')

        num_create_events_by_date_df_2 = \
            num_create_events_by_date_df_2 \
            .select(
                col('date_created_at').alias('date_created_at_2'),
                col('count').alias('count_2'))

        joined_num_create_events_df = \
            num_create_events_by_date_df_1 \
            .withColumn(
                'last_week_date_created_at',
                date_add(num_create_events_by_date_df_1.date_created_at_1, -7)) \
            .join(
                num_create_events_by_date_df_2,
                col('last_week_date_created_at')
                == col('date_created_at_2'),
                how='left_outer')

        joined_num_create_events_df = joined_num_create_events_df.withColumn(
            'count_2', coalesce('count_2', 'count_1'))

        num_create_events_with_growth_rate_df = \
            joined_num_create_events_df \
            .withColumn(
                'weekly_increase_rate',
                ((joined_num_create_events_df.count_1 - joined_num_create_events_df.count_2) / joined_num_create_events_df.count_2)
            ) \
            .select(
                'date_created_at_1',
                'count_1',
                'weekly_increase_rate')

        num_create_events_with_growth_rate_df.show()

        return num_create_events_with_growth_rate_df

    def process_history_df_wow(self, df):
        """
        Process function for history data, generate result dataframe
        that contains week, number of create events
        """
        # There are two versions of API for CreateEvent of repository:
        # - One is        col("payload")['object'] == 'repository'
        # - Another is    col("payload")['ref_type'] == 'repository'
        # try:
        df_columns = df.columns
        df_first_record = df.first()
        keyword = 'object' if 'object' in df_first_record['payload'] else 'ref_type'

        num_create_events_df = \
            df \
            .filter(col('payload')[keyword] == 'repository') \
            .filter((col('type') == 'CreateEvent') | (col('type') == 'Event'))

        # count the number of create events happened in one week (group by week)
        num_create_events_by_week_df = num_create_events_df.groupby(date_trunc('week', df.created_at).alias('week_created_at')).count()

        # calculate the grawth rate of that day compare to last week
        # dulicated two dataframes, for each day in the first dataframe
        # find the number fo create events in the second dataframe
        # of a day that is 7 days before the day in the first dataframe
        # [df1] 2015-01-07 -> [df2] 2015-01-01 (7 days)
        num_create_events_by_week_df_1 = num_create_events_by_week_df.alias('num_create_events_by_week_df_1')

        num_create_events_by_week_df_1 = \
            num_create_events_by_week_df_1 \
            .select(
                col('week_created_at').alias('week_created_at_1'),
                col('count').alias('count_1'))

        num_create_events_by_week_df_2 = num_create_events_by_week_df.alias('num_create_events_by_week_df_2')

        num_create_events_by_week_df_2 = \
            num_create_events_by_week_df_2 \
            .select(
                col('week_created_at').alias('week_created_at_2'),
                col('count').alias('count_2'))

        joined_num_create_events_df = \
            num_create_events_by_week_df_1 \
            .withColumn(
                'last_week_week_created_at',
                date_trunc(
                    'week',
                    date_add(num_create_events_by_week_df_1.week_created_at_1, -7))) \
            .join(
                num_create_events_by_week_df_2,
                col('last_week_week_created_at')
                == col('week_created_at_2'),
                how='left_outer')

        joined_num_create_events_df.show()

        joined_num_create_events_df = joined_num_create_events_df.withColumn(
            'count_2', coalesce('count_2', 'count_1'))

        num_create_events_with_growth_rate_df = \
            joined_num_create_events_df \
            .withColumn(
                'weekly_increase_rate',
                ((joined_num_create_events_df.count_1 - joined_num_create_events_df.count_2) / joined_num_create_events_df.count_2)
            ) \
            .select(
                'week_created_at_1',
                'count_1',
                'weekly_increase_rate')

        num_create_events_with_growth_rate_df.show()

        return num_create_events_with_growth_rate_df

    def process_history_df_mom(self, df):
        """
        Process function for history data, generate result dataframe
        that contains week, number of create events
        """
        # There are two versions of API for CreateEvent of repository:
        # - One is        col("payload")['object'] == 'repository'
        # - Another is    col("payload")['ref_type'] == 'repository'
        # try:
        df_columns = df.columns
        df_first_record = df.first()
        keyword = 'object' if 'object' in df_first_record['payload'] else 'ref_type'

        num_create_events_df = \
            df \
            .filter(col('payload')[keyword] == 'repository') \
            .filter((col('type') == 'CreateEvent') | (col('type') == 'Event'))

        # count the number of create events happened in one week (group by week)
        num_create_events_by_month_df = num_create_events_df.groupby(date_trunc('month', df.created_at).alias('month_created_at')).count()

        # calculate the grawth rate of that day compare to last week
        # dulicated two dataframes, for each day in the first dataframe
        # find the number fo create events in the second dataframe
        # of a day that is 7 days before the day in the first dataframe
        # [df1] 2015-01-07 -> [df2] 2015-01-01 (7 days)
        num_create_events_by_month_df_1 = num_create_events_by_month_df.alias('num_create_events_by_month_df_1')

        num_create_events_by_month_df_1 = \
            num_create_events_by_month_df_1 \
            .select(
                col('month_created_at').alias('month_created_at_1'),
                col('count').alias('count_1'))

        num_create_events_by_month_df_2 = num_create_events_by_month_df.alias('num_create_events_by_month_df_2')

        num_create_events_by_month_df_2 = \
            num_create_events_by_month_df_2 \
            .select(
                col('month_created_at').alias('month_created_at_2'),
                col('count').alias('count_2'))

        joined_num_create_events_df = \
            num_create_events_by_month_df_1 \
            .withColumn(
                'last_week_month_created_at',
                date_trunc(
                    'month',
                    add_months(num_create_events_by_month_df_1.month_created_at_1, -1))) \
            .join(
                num_create_events_by_month_df_2,
                col('last_week_month_created_at')
                == col('month_created_at_2'),
                how='left_outer')

        joined_num_create_events_df.show()

        joined_num_create_events_df = joined_num_create_events_df.withColumn(
            'count_2', coalesce('count_2', 'count_1'))

        num_create_events_with_growth_rate_df = \
            joined_num_create_events_df \
            .withColumn(
                'monthly_increase_rate',
                ((joined_num_create_events_df.count_1 - joined_num_create_events_df.count_2) / joined_num_create_events_df.count_2)
            ) \
            .select(
                'month_created_at_1',
                'count_1',
                'monthly_increase_rate')

        num_create_events_with_growth_rate_df.show()

        return num_create_events_with_growth_rate_df

    def process_present_df(self, present_df, table_name):
        """
        Process function for generate result dataframe that contains date,
        number of create events and growth rate of a day compare to last week
        """
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
        )

    def read_all_to_list(self, spark_recent_todo_files):
        """
        Read a file, each line is a url to s3n://bucket/file.json
        => ["s3n://bucket/file.json", "s3n://bucket/file1.json", ...]
        """
        if not os.path.isfile(spark_recent_todo_files):
            open(spark_recent_todo_files, 'w').close()
        result = []
        with open(spark_recent_todo_files) as fin:
            for url in fin:
                result.append(url.strip())
        return result

    def generate_todo_urls(self, spark_recent_todo_files, table_name, bucket_name, start_date=datetime.datetime.utcnow()):
        """
        Given start_date and table name and bucket name, generate aws s3 urls
        """
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

    def benchmark_reading(self, bucket_name, middles, nums_of_files_wanted):
        """
        * for benchmarking purposes
        {'2014-10-03': {1:[100,101,120,109], 12:[98, 89, 70, 99], ...}}
        """
        # schema = StructType([
        #     StructField("name", StringType(), True),
        #     StructField("age", IntegerType(), True)])
        schemas = set()
        for middle in middles:
            middle_d = {}
            for num_of_files in nums_of_files_wanted:
                times = []
                num_of_files_str = str(num_of_files)
                url = 's3n://' + bucket_name + '/' + num_of_files_str + '-' + middle + '*'
                print(url)
                for i in range(3):
                    start_time = time.time()
                    df = self.sqlContext.read.json(url, self.schema)
                    total_seconds = time.time() - start_time
                    times.append(total_seconds)
                    schemas.add(str(df.schema))
                    print(total_seconds)
                middle_d[num_of_files] = times
            with open('result.json', 'a') as fout:
                fout.write(json.dumps(middle_d) + '\n')
        with open('result.json', 'a') as fout:
            fout.write(json.dumps(list(schemas)) + '\n')

    def benchmark_reading_prep(self, bucket_name, file_name, nums_of_files_wanted, middle):
        """
        * for benchmarking purposes
        Given a large file, split them into small fils in different sizes
        # nums_of_files_wanted e.g.
        # [6, 12, 18, 24, 30, 36, 42, 48, 54, 60]
        """
        total_num_lines = self.get_total_lines(bucket_name, file_name)
        print file_name
        for num_of_files in nums_of_files_wanted:
            print 'num_of_files', num_of_files
            self.split_large_file(num_of_files, total_num_lines, bucket_name, file_name, bucket_name, middle)

    def split_large_file(self, num_of_files, total_num_lines, bucket_in_name, file_in_name, bucket_out_name, middle):
        """
        * for benchmarking purposes
        """
        line_counter = 0
        lines_each_file = total_num_lines / num_of_files
        file_out_names = ['%s-%s-%s.json' % (num_of_files, middle, i) for i in range(num_of_files)]
        line_begins = 0
        for file_out_name in file_out_names:
            with smart_open('s3://%s/%s' % (bucket_out_name, file_out_name), 'wb') as fout:
                for line in smart_open('s3://' + bucket_in_name + '/' + file_in_name, 'rb'):
                    if line_counter < line_begins:
                        line_counter += 1
                        continue
                    line_counter += 1
                    fout.write(line)
                    if line_counter % lines_each_file == 0:
                        line_begins = line_counter
                        line_counter = 0
                        break

    def get_total_lines(self, bucket_name, file_name):
        """
        * for benchmarking purposes
        Return the number of lines of a file on AWS S3, given the bucket name
        """
        num_lines = 0
        for line in smart_open('s3://' + bucket_name + '/' + file_name, 'rb'):
            num_lines += 1
        return num_lines
