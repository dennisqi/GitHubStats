import os
import sys
import time
import datetime
from spark_processor import SparkProcessor


if __name__ == '__main__':

    history_or_present = None

    # There are two types of processing,
    # history data processing and present data processing
    #   e.g. history: 2011-02-12 ~ 2019-02-02
    #   e.g. present: last_processed_date ~ now
    if len(sys.argv) < 2 or sys.argv[1].lower() not in ['history', 'present']:
        raise ValueError('Need to specify "history" or "present"')
    else:
        history_or_present = sys.argv[1]

    # conf of DB
    host = os.environ.get('PG_HOST')
    dbname = 'dbname'
    user = os.environ.get('PG_USER')
    password = os.environ.get('PG_PSWD')

    app_name = 'jdbc_cluster_1G1_benchmarking_schema'
    # app_name = 'jdbc_cluster_5g5_2017-12-2018-1-12-2019-1*'

    # Create a spark processor
    processor = SparkProcessor(
        os.environ.get('S3_ACCESS_KEY_ID'),
        os.environ.get('S3_SECRET_ACCESS_KEY'),
        host, dbname, user, password, app_name
    )

    bucket_name = 'gharchive'
    # bucket_name = 'ghstats-small-test'
    # bucket_name = 'ghalargefiletest'
    # bucket_name = 'jdbc-vs-psycopg'
    # bucket_name = 'gharchivewow'
    # bucket_name = 'gharchivemom'

    table_name = 'gharchive'
    # table_name = 'ghstatssmalltest'
    # table_name = 'ghalargefiletest'
    # table_name = 'jdbcvspsycopg'
    table_name_wow = 'gharchivewowtest'
    table_name_mom = 'gharchivemomtest'

    file_names = [
        '1-2014-07-04-0.json',
        '1-2014-10-11-0.json',
        '1-2014-11-05-0.json',
        '1-2017-06-07-0.json',
    ]
    middles = [
        '2014-07-04',
        '2014-10-11',
        '2014-11-05',
        '2017-06-07',
    ]

    # * for benchmarking purposes
    nums_of_files_wanted = [3, 6, 9, 12, 15, 18, 21, 24, 27, 30]

    for i in range(len(file_names)):
        processor.benchmark_reading_prep(
            'benchmark-reading', file_names[i],
            nums_of_files_wanted, middles[i]
            )

    # * for benchmarking purposes
    nums_of_files_wanted = [1, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30]
    processor.benchmark_reading('benchmark-reading', middles, nums_of_files_wanted)

    # The dataframe that will be stored into postgresql
    result_df = None

    if history_or_present == 'history':
        path = [
            's3n://' + bucket_name + '/' + '2017-12*',
            's3n://' + bucket_name + '/' + '2018-01*',
            's3n://' + bucket_name + '/' + '2018-12*',
            's3n://' + bucket_name + '/' + '2019-01*',
        ]

        start_time = time.time()
        df = processor.read_all_to_df(bucket_name, path)
        end_time = time.time()
        print("Read: --- %s seconds ---" % (end_time - start_time))

        start_time = time.time()
        result_df = processor.process_history_df(df)
        result_df.show()
        # result_df_wow = processor.process_history_df_wow(df)
        # result_df_wow.show()
        result_df_mom = processor.process_history_df_mom(df)
        result_df_mom.show()
        end_time = time.time()
        print("Calc: --- %s seconds ---" % (end_time - start_time))

        start_time = time.time()
        processor.df_jdbc_write_to_db(result_df, table_name, mode='append')
        # processor.df_jdbc_write_to_db(result_df_wow, table_name_wow, mode='append')
        processor.df_jdbc_write_to_db(result_df_mom, table_name_mom, mode='append')
        end_time = time.time()
        print("JDBC: --- %s seconds ---" % (end_time - start_time))

        # insert_sql = "INSERT INTO " + table_name + " VALUES (%s, %s, %s);"
        # start_time = time.time()
        # for row in result_df.rdd.toLocalIterator():
        #     insert_param = (row.date_created_at_1, row.count_1, row.weekly_increase_rate)
        #     processor.df_psycopg2_write_to_db(insert_sql, insert_param)
        # end_time = time.time()
        # print("psy2: --- %s seconds ---" % (end_time - start_time))

    else:
        # If it is present processing,
        # spark_recent_todo_files contains all urls to each archive json file
        spark_recent_todo_files = '../../data/spark_recent_todo_s3_urls.txt'

        start_date = datetime.datetime(2011, 2, 25, 0)
        start_date -= datetime.timedelta(days=1)

        start_time = time.time()
        processor.generate_todo_urls(
            spark_recent_todo_files, table_name, bucket_name, start_date)
        end_time = time.time()
        print("Gene: --- %s seconds ---" % (end_time - start_time))

        start_time = time.time()
        s3_urls = processor.read_all_to_list(spark_recent_todo_files)
        end_time = time.time()
        print("Conv: --- %s seconds ---" % (end_time - start_time))

        start_time = time.time()
        present_df = processor.read_files_to_df(s3_urls)
        end_time = time.time()
        print("Read: --- %s seconds ---" % (end_time - start_time))

        start_time = time.time()
        result_df = processor.process_present_df(present_df, table_name)
        end_time = time.time()
        print("Calc: --- %s seconds ---" % (end_time - start_time))

        processor.df_jdbc_write_to_db(result_df, table_name, mode='overwrite')
        end_time = time.time()
        print("JDBC: --- %s seconds ---" % (end_time - start_time))

        # insert_sql = "INSERT INTO jdbcvspsycopg VALUES (%s, %s, %s);"
        # start_time = time.time()
        # for row in result_df.rdd.toLocalIterator():
        #     insert_param = (row.date_created_at_1, row.count_1, row.weekly_increase_rate)
        #     processor.df_psycopg2_write_to_db(insert_sql, insert_param)
        # end_time = time.time()
        # print("psy2: --- %s seconds ---" % (end_time - start_time))
