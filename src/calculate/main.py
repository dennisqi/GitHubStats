import os
from spark_processor import SparkProcessor
from dbwriter import DBRwiter


if __name__ == '__main__':

    # two fiels that contains file names what we have processed and are going to process
    processor_read = '../../data/processor_read.txt'
    processor_write = '../../data/processor_write.txt'

    # Connect to DB to write into
    host = os.environ.get('PG_HOST')
    dbname = 'dbname'
    user = os.environ.get('PG_USER')
    password = os.environ.get('PG_PSWD')
    dbwriter = DBRwiter(host, dbname, user, password, processor_write)

    # SQL query for creating table
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS num_repo_creation_v2 (
            date_time timestamp PRIMARY KEY,
            num_creation INT NOT NULL
        );
    """

    # bucket_name is the s3 bucket that we read from
    bucket_name = 'ghalargefiletest'

    # indicates wheather or not to create table
    wheather_create_table = True

    processor = SparkProcessor(
        os.environ.get('S3_ACCESS_KEY_ID'),
        os.environ.get('S3_SECRET_ACCESS_KEY')
    )

    # head + datetime + tail = filename
    # ghalarge + 2011-05-03 + .json = ghalarge2011-05-03.json
    head = 'ghalarge'
    tail = '.json'

    processor.generate_processor_read_file(processor_read, processor_write, head, tail)

    counter = 0
    # Read from processor_read.txt which contains lines of file names on S3
    for file_name in processor.get_lines_of_file(file=processor_read):

        # result_d is a dict contains date_time and num_create_events
        result_d = processor.process(bucket_name, file_name)
        # print(result_d)
        # if counter >= 10:
        #     break
        # counter += 1

        if not result_d:
            print('RECORD IS EMPTY ' + file_name)
            continue

        # Generate parameter and sql query to insert in to DB table
        insert_sql = "INSERT INTO num_repo_creation_v2 VALUES (%s, %s);"
        insert_param = (result_d['date_time'], result_d['num_create_events'])

        # Write into DB, create table only one time, then set wheather_create_table to False
        dbwriter.write_to_db(wheather_create_table, create_table_sql, insert_sql, insert_param, file_name)
        wheather_create_table = False
        print('INSERTED RECODR: ' + file_name)

        # Write the file_name into processor_write file, to keep the record
        # processor.append_to_file(processor_write, file_name)
