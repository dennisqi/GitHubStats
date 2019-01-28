import os
from calculator import Calculator
from s3connector import S3Connector
from dbwriter import DBRwiter


if __name__ == '__main__':
    bucket_name = 'gharchive'
    s3connector1 = S3Connector(bucket_name)

    calculator1 = Calculator(
        os.environ.get('S3_ACCESS_KEY_ID'),
        os.environ.get('S3_SECRET_ACCESS_KEY')
    )

    host = os.environ.get('PG_HOST')
    dbname = 'dbname'
    user = os.environ.get('PG_USER')
    password = os.environ.get('PG_PSWD')
    dbwriter1 = DBRwiter(host, dbname, user, password)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS num_repo_creation (
            date_time timestamp PRIMARY KEY,
            num_creation INT NOT NULL
        );
    """

    for object in s3connector1.get_objects():
        record = calculator1.calculate(object)
        if not record:
            print('RECORD IS EMPTY ' + object.key)
            continue

        insert_sql = "INSERT INTO num_repo_creation VALUES (%s, %s);"
        insert_para = (record['date_time'], record['num_repos'])
        backup_log = record['date_time'].strftime("%Y-%m-%d-%H.json")

        dbwriter1.write_to_db(
            create_table_sql, insert_sql, insert_para, backup_log)
        print('INSERTED RECODR: ' + backup_log)
