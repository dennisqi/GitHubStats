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
    dbname = os.environ.get('DB_NAME')
    user = os.environ.get('PG_USER')
    password = os.environ.get('PG_PSWD')

    app_name = 'jdbc_cluster_1G1_benchmarking_schema'

    # Create a spark processor
    processor = SparkProcessor(
        os.environ.get('S3_ACCESS_KEY_ID'),
        os.environ.get('S3_SECRET_ACCESS_KEY'),
        host, dbname, user, password, app_name
    )

    bucket_name = 'gharchive'

    table_name = 'gharchive'
    table_name_wow = 'gharchivewowtest'  # month over month growth rate
    table_name_mom = 'gharchivemomtest'  # week over week growth rate

    # The dataframe that will be stored into postgresql
    result_df = None

    if history_or_present == 'history':
        path = ['s3n://' + bucket_name + '/*']
        df = processor.read_all_to_df(bucket_name, path)
        result_df = processor.process_history_df(df)
        result_df_wow = processor.process_history_df_wow(df)
        result_df_mom = processor.process_history_df_mom(df)
        processor.df_jdbc_write_to_db(result_df, table_name, mode='append')
        processor.df_jdbc_write_to_db(result_df_wow, table_name_wow, mode='append')
        processor.df_jdbc_write_to_db(result_df_mom, table_name_mom, mode='append')

    else:
        # If it is present processing,
        # spark_recent_todo_files contains all urls to each archive json file
        spark_recent_todo_files = '../../data/spark_recent_todo_s3_urls.txt'

        start_date = datetime.datetime(2011, 2, 25, 0)
        start_date -= datetime.timedelta(days=1)

        processor.generate_todo_urls(spark_recent_todo_files, table_name, bucket_name, start_date)
        s3_urls = processor.read_all_to_list(spark_recent_todo_files)
        present_df = processor.read_files_to_df(s3_urls)
        result_df = processor.process_present_df(present_df, table_name)
        processor.df_jdbc_write_to_db(result_df, table_name, mode='overwrite')
