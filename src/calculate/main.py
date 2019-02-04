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

    # Create a spark processor
    processor = SparkProcessor(
        os.environ.get('S3_ACCESS_KEY_ID'),
        os.environ.get('S3_SECRET_ACCESS_KEY'),
        host, dbname, user, password
    )

    bucket_name = 'gharchive'
    # bucket_name = 'ghstats-small-test'
    # bucket_name = 'ghalargefiletest'

    table_name = 'gharchive'
    # table_name = 'ghstatssmalltest'
    # table_name = 'ghalargefiletest'

    # The dataframe that will be stored into postgresql
    result_df = None

    if history_or_present == 'history':
        # path = 's3n://' + bucket_name + '/'
        path = [
            's3n://' + bucket_name + '/' + '2018*',
            's3n://' + bucket_name + '/' + '2019*'
        ]

        start_time = time.time()
        df = processor.read_all_to_df(bucket_name, path)
        end_time = time.time()
        print("Read: --- %s seconds ---" % (end_time - start_time))

        start_time = time.time()
        result_df = processor.process_history_df(df)
        end_time = time.time()
        print("Calc: --- %s seconds ---" % (end_time - start_time))

        start_time = time.time()
        processor.df_jdbc_write_to_db(result_df, table_name)
        end_time = time.time()
        print("JDBC: --- %s seconds ---" % (end_time - start_time))

        # insert_sql = "INSERT INTO num_repo_creation_v3 VALUES (%s, %s, %s);"
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

        processor.df_jdbc_write_to_db(result_df, table_name)
        end_time = time.time()
        print("JDBC: --- %s seconds ---" % (end_time - start_time))

        # insert_sql = "INSERT INTO num_repo_creation_v3 VALUES (%s, %s, %s);"
        # start_time = time.time()
        # for row in result_df.rdd.toLocalIterator():
        #     insert_param = (row.date_created_at_1, row.count_1, row.weekly_increase_rate)
        #     processor.df_psycopg2_write_to_db(insert_sql, insert_param)
        # end_time = time.time()
        # print("psy2: --- %s seconds ---" % (end_time - start_time))
