import pymysql
import re
def exec_sql_file(cursor, sql_file):
    print("n[INFO] Executing SQL script file: '%s'" % (sql_file))
    statement = ""

    for line in open(sql_file):
        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if not re.search(r';$', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:  # when you get a line ending in ';' then exec statement and reset for next statement
            statement = statement + line
            #print "nn[DEBUG] Executing SQL statement:n%s" % (statement)
            try:
                cursor.execute(statement)
            except (OperationalError, ProgrammingError) as e:
                print("n[WARN] MySQLError during execute statement ntArgs: '%s'" % (str(e.args)))

            statement = ""

def create_table():
    #query for create table
    #fd = open('/opt/airflow/dags/create/northwind.sql', 'r')
    conn = pymysql.connect(host='host.docker.internal',
                           port=3307,
                           user='admin', 
                           password='admin',  
                           db='northwind')
    #run mysql query
    cur = conn.cursor()
    exec_sql_file(cur,'/opt/airflow/dags/create/northwind.sql')
    conn.commit()
    cur.close()
    conn.close()

    print('table "northwind" successfully created')

if __name__=='__main__':
    create_table()
