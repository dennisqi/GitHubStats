import os
import time
from datetime import timedelta
from spark_processor import SparkProcessor


if __name__ == '__main__':

    # Connect to DB to write into
    host = os.environ.get('PG_HOST')
    dbname = 'dbname'
    user = os.environ.get('PG_USER')
    password = os.environ.get('PG_PSWD')

    processor_write = ''

    # Read all files from s3 bucket
    processor = SparkProcessor(
        os.environ.get('S3_ACCESS_KEY_ID'),
        os.environ.get('S3_SECRET_ACCESS_KEY'),
        host, dbname, user, password, processor_write
    )

    bucket_name = 'ghstats-small-test'

    df = processor.read_all_to_df(bucket_name)
    result_df = processor.process_df(df)

    table_name = 'ghstats-small-test'
    processor.df_write_to_db(result_df, table_name)

    # # two fiels that contains file names what we have processed
    # #   and are going to process
    # processor_read = '../../data/processor_read.txt'
    # processor_write = '../../data/processor_write.txt'
    #
    #
    # # SQL query for creating table
    # create_table_sql = """
    #     CREATE TABLE IF NOT EXISTS num_repo_creation_v3 (
    #         date_time timestamp PRIMARY KEY,
    #         num_creation integer NOT NULL,
    #         increase_rate_last_week double precision NOT NULL
    #     );
    # """
    # insert_sql = "INSERT INTO num_repo_creation_v3 VALUES (%s, %s, %s);"
    #
    # # bucket_name is the s3 bucket that we read from
    # bucket_name = 'ghalargefiletest'
    #
    # # indicates wheather or not to create table
    # wheather_create_table = True
    #
    # processor = SparkProcessor(
    #     os.environ.get('S3_ACCESS_KEY_ID'),
    #     os.environ.get('S3_SECRET_ACCESS_KEY'),
    #     host, dbname, user, password, processor_write
    # )
    #
    # # head + datetime + tail = filename
    # #   ghalarge + 2011-05-03 + .json = ghalarge2011-05-03.json
    # head = 'ghalarge'
    # tail = '.json'
    #
    # processor.generate_processor_read_file(processor_read, processor_write, head, tail)
    #
    # counter = 0
    # # Read from processor_read.txt which contains lines of file names on S3
    # for file_name in processor.get_lines_of_file(file=processor_read):
    #
    #     # result_d is a dict contains date_time and num_create_events
    #     result_d = processor.process(bucket_name, file_name, wheather_create_table, create_table_sql)
    #     wheather_create_table = False
    #
    #     if not result_d:
    #         print('RECORD IS EMPTY ' + file_name)
    #         continue
    #
    #     # Generate parameter and sql query to insert in to DB table
    #     insert_param = (
    #         result_d['date_time'],
    #         result_d['num_create_events'],
    #         result_d['rate_last_week'])
    #
    #     # Write into DB, create table only one time, then set wheather_create_table to False
    #     processor.write_to_db(wheather_create_table, create_table_sql, insert_sql, insert_param, file_name)
    #     print('INSERTED RECODR: ' + file_name)
