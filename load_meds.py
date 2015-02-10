import psycopg2, os, mimetypes, re

try:
    conn = psycopg2.connect("dbname='ms' user='ms'")
except:
    print "I am unable to connect to the database"
    exit()

cur = conn.cursor()
for (dirpath, dirnames, filenames) in os.walk('/home/ms/practicum/medex_output'):
    for file in filenames:
        filepath = os.path.join(dirpath, file)
        if mimetypes.guess_type(filepath)[0] == 'text/plain':
            input = open(filepath, 'r')
            for line in input.read().splitlines():
                medstring, drug, brand, form, strength, dose, route, freq, dur, nec, umlscui, rxcui, rxcui_gen, gen = re.sub(r'^[0-9]*\w','',line).split('|')
                cur.execute('SELECT 1 FROM ensemble_meds WHERE medstring = %s AND rxcui = %s', (medstring.strip(),rxcui.strip()))
                exists = cur.fetchall()
                if not exists:
                    cur.execute('INSERT INTO ensemble_meds (sourcefile,medstring,umlscui,rxcui,rxcui_gen,gen) VALUES (%s, %s, %s, %s, %s, %s)', (file.strip(),medstring.strip(),umlscui.strip(),rxcui.strip(),rxcui_gen.strip(),gen.strip()))

conn.commit()
conn.close()
