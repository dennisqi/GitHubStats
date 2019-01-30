import os
import psycopg2


class DBRwiter(object):

    def __init__(self, host, dbname, user, password, processor_write):
        print('Connecting to DB...')
        self.conn = psycopg2.connect(
            host=host, database=dbname, user=user, password=password)
        print('Connected.')
        self.processor_write = processor_write

    def write_to_db(self, wheather_create_table, create_table_sql, insert_sql, insert_param, file_name):
        """
        If wheather_create_table is True, execute create_table_sql.
        Eexecute insert_sql with insert_paramself.
        Append backup_sentence to self.backup_file.
        """
        cur = self.conn.cursor()
        if wheather_create_table:
            cur.execute(create_table_sql)
        self.conn.commit()

        try:
            cur.execute(insert_sql, insert_param)
        except psycopg2.IntegrityError as ie:
            print("Diplicate key.")
        else:
            with open(self.processor_write, 'a') as processor_write:
                processor_write.write(file_name + '\n')
                print('WROTE RECODR: ' + file_name)

        self.conn.commit()
        cur.close()
