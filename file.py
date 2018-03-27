import psycopg2
from configparser import RawConfigParser

config = RawConfigParser()
config.read('config.properties')


def get_conn():
    if not hasattr(get_conn, 'db_details'):
        get_conn.db_details = dict(config.items('DB'))
    conn = psycopg2.connect(**get_conn.db_details)
    return conn


def batch(iterable, size=1):
    for i in range(0, len(iterable), size):
        yield iterable[i: i + size]


def insert_rows_batch(table, rows, batch_size=500, target_fields=None):
    """
    A utility method to insert batch of tuples(rows) into a table
    NOTE: Handle data type for fields in rows yourself as per your table columns' type.

    :param table: Name of the target table
    :type table: str
    :param rows: The rows to insert into the table
    :type rows: iterable of tuples
    :param batch_size: The size of batch of rows to insert at a time
    :type batch_size: int
    :param target_fields: The names of the columns to fill in the table
    :type target_fields: iterable of strings
    """
    if target_fields:
        target_fields = ", ".join(target_fields)
        target_fields = "({})".format(target_fields)
    else:
        target_fields = ''

    conn = get_conn()
    cur = conn.cursor()
    count = 0

    for mini_batch in batch(rows, batch_size):
        mini_batch_size = len(mini_batch)
        count += mini_batch_size
        record_template = ','.join(["%s"] * mini_batch_size)
        sql = "INSERT INTO {0} {1} VALUES {2};".format(
            table,
            target_fields,
            record_template)
        cur.execute(sql, mini_batch)
        conn.commit()
        print("Loaded {} rows into {} so far".format(count, table))
    print("Done loading. Loaded a total of {} rows".format(count))
    cur.close()
    conn.close()


def upsert(table, pk_fields, all_fields, rows, pk_name=None, schema=None, target_fields=None,
           batch_size=100):
    """
    Implements Insert + Update (UPSERT) for Postgres database.
    
    Make sure to pass pk_fields and all_fields params.
    
    NOTE: Maintain the order of the all_fields parameter as per the order of column names in database.

    :param table: The name of the table
    :param pk_fields: A list of primary key field(s)
    :param all_fields: A list of all fields or column names of the database in correct order
    :param rows: A list of tuples of rows to be insetred or updated
    :param pk_name: The name of the table primary key. Don't pass it if primary key name has not been set manually,
                    in that case will use the default primary key name as TABLE-NAME_pkey
    :param schema: The schema used for the table.
    :param target_fields: A list of all column names (Optional).
    :param batch_size: The size of the batch to perform upsert upon, default 100
    :return: None
    """

    assert len(pk_fields) > 0 and len(all_fields) > 1

    other_fields = [field for field in all_fields if field not in pk_fields]

    if target_fields:
        target_fields = ", ".join(target_fields)
        target_fields = "({})".format(target_fields)
    else:
        target_fields = ''

    if not pk_name:
        pk_name = table + "_pkey"

    if schema and '.' not in table:
        table = '%s.%s' % (schema, table)

    field_bracket = "{}" if len(other_fields) == 1 else "({})"

    insert_sql = "INSERT INTO {} {}".format(table, target_fields) + " VALUES {}" + \
                 " ON CONFLICT ON CONSTRAINT {}".format(pk_name) + \
                 " DO UPDATE SET " + field_bracket.format(', '.join(other_fields)) + \
                 " = ({}) ;".format(', '.join(['EXCLUDED.' + col for col in other_fields]))

    conn = get_conn()
    cur = conn.cursor()
    count = 0
    last_batch_size = len(rows) % batch_size

    sql = insert_sql.format(','.join(["%s"] * batch_size))
    last_sql = insert_sql.format(','.join(["%s"] * last_batch_size))

    for mini_batch in batch(iterable=rows, size=batch_size):
        mini_batch_size = len(mini_batch)
        if mini_batch_size == last_batch_size:
            sql = last_sql
        cur.execute(sql, mini_batch)
        conn.commit()
        count += mini_batch_size
        print("Commit done on {} row(s) for UPSERT so far.".format(count))
    print("Commit done on all {} rows for UPSERT.".format(len(rows)))
    print("UPSERT Done.")
    cur.close()
    conn.close()
