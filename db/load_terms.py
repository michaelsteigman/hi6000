import sys, psycopg2, psycopg2.extras, csv
from secrets import dbname, dbpasswd, dbuser

# try:
#     conn_string = 'dbname=' + dbname + ' user=' + dbuser + ' dbpasswd=' + dbpasswd
#     conn = psycopg2.connect(conn_string)
#     print('Connected to DB')
#     conn.close()
# except:
#     print('Could not connect to DB')
#     exit()

# cur = cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
with open(sys.argv[1], 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    fieldnames = list(reader.fieldnames)
    query = 'INSERT INTO ensembleclassdictionary (' + ','.join(fieldnames) + ') VALUES (' + ','.join(['%(' + col + ')s' for col in fieldnames]) + ')'
    data = [row for row in reader]
    print(query, data[:10])
    #cur.executemany(query, data)


#conn.commit()
#conn.close()
