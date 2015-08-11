import psycopg2, re, sys, csv

try:
    conn = psycopg2.connect("dbname='practicum' user='ms'")
except:
    print('I am unable to connect to the database')
    exit()


if len(sys.argv) != 2:
    print('Please supply the path to file to be loaded')
    exit()

path = sys.argv[1]
filename = re.search(r'([a-z]*.[a-z]*)$',path).group(0).strip()
print(filename)
exit()
cur = conn.cursor()
with open(path, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        cur.execute('INSERT INTO ensemble_pairs (med_string, problem_string,source_file) VALUES (%s, %s, %s)', (row[0].strip(),row[1].strip(),filename))
conn.commit()
conn.close()


