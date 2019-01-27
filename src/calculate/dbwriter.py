import os
import psycopg2


class DBRwiter(object):

    def __init__(self, host, dbname, user, password,
                 calculated_file='../../data/calcluated.txt'):
        print('Connecting to DB...')
        self.conn = psycopg2.connect(
            host=host, database=dbname, user=user, password=password)
        print('Connected.')
        self.calculated_file = calculated_file

    def write_to_db(
            self, create_table_sql, insert_sql, insert_para, backup_log):
        cur = self.conn.cursor()
        cur.execute(create_table_sql)
        self.conn.commit()

        try:
            cur.execute(insert_sql, insert_para)
        except psycopg2.IntegrityError as ie:
            print("Diplicate key.")
        else:
            with open(self.calculated_file, 'w') as calculated:
                calculated.write(backup_log)

        self.conn.commit()
        cur.close()
