import os
import sys
import psycopg2


if __name__ == '__main__':
    # num_repo_creation_v2
    # num_repo_creation
    sql = 'select * from '
    if len(sys.argv) >= 2:
        sql += sys.argv[1]
    else:
        raise ValueError('Need a table name to process.')
    print('Connecting to DB...')
    conn = psycopg2.connect(
        host=os.environ.get('PG_HOST'),
        database='dbname',
        user=os.environ.get('PG_USER'),
        password=os.environ.get('PG_PSWD')
    )
    print('Connected.')
    cur = conn.cursor()
    cur.execute(sql)
    for record in cur.fetchall():
        print(record)
    cur.close()
    conn.commit()
