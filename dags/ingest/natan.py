import pymysql
import csv
import sys

print('satu '+sys.argv[1])
print('nol '+sys.argv[0])
conn = pymysql.connect(host='host.docker.internal',
                           port=3307,
                           user='admin', 
                           password='admin',  
                           db='northwind')
    #run mysql query
cur = conn.cursor()


sql = """select * from northwind.orders o where cast(order_date as date) = '"""+sys.argv[1]+"""'"""
csv_file_path = '/opt/airflow/dags/output/natan/orders/orders_'+sys.argv[1]+'.csv'

try:
    cur.execute(sql)
    rows = cur.fetchall()
finally:
    conn.close()

# Continue only if there are rows returned.
if rows:
    # New empty list called 'result'. This will be written to a file.
    result = list()

    # The row name is the first entry for each entity in the description tuple.
    column_names = list()
    for i in cur.description:
        column_names.append(i[0])

    result.append(column_names)
    for row in rows:
        result.append(row)

    # Write result to file.
    with open(csv_file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in result:
            csvwriter.writerow(row)
else:
    print("No rows found for query: {}".format(sql))