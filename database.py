import sqlite3
from schema import tables_schema

# Connect and initialize the sqlite database
def create_db(log=False):
    con = sqlite3.connect('twitter.db')
    # print logging if log is true
    if log: con.set_trace_callback(print)
    cur = con.cursor()
    
    for table in tables_schema:
        # Create table
        cur.execute(table)

    cur.close()
    con.commit()
    return con