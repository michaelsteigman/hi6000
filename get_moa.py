import psycopg2, os, requests
from xml.etree import ElementTree as ET

try:
    conn = psycopg2.connect("dbname='ms' user='ms'")
except:
    print "I am unable to connect to the database"
    exit()

cur = conn.cursor()
cur.execute('SELECT DISTINCT rxcui, rxcui_gen FROM ensemble_meds LIMIT 20')
rows = cur.fetchall()
for row in rows:
    url = 'http://rxnav.nlm.nih.gov/REST/rxcui/{0}/hierarchy?src=NDFRT&type=MOA&oneLevel=1'.format(row[0])
    r = requests.get(url)
    tree = ET.fromstring(r.text)

    # retrieve VA product and MoA
    va_product_nui = None
    moa = None
    moa_nui = None
    n_moa = 0
    n_va = 0
    for node in tree.iter('node'):
        for elm in node.findall('./nodeAttr/attrValue'):
            if elm.text == 'VA Product':
                va_product_nui = node.find('nodeId').text
                print 'va prod ' + va_product_nui
            elif elm.text == 'MECHANISM_OF_ACTION_KIND':
                moa = node.find('nodeName').text
                moa_nui = node.find('nodeId').text
                print 'moa ...' + str(moa) + ' ' + str(moa_nui)

    print 'URL ' + str(url)
    if moa:
#    cur.execute('UPDATE ensemble_meds SET ndfrt_moa = %s, ndfrt_moa_nui = %s WHERE rxcui = %s', moa.strip(), moa_nui.strip(), row[0])
        n_moa += 1

    if va_product_nui:
        url = 'http://rxnav.nlm.nih.gov/REST/Ndfrt/VA?nui={0}'.format(va_product_nui)
        r = requests.get(url)
        tree = ET.fromstring(r.text)

        if tree.find('concept/conceptNui'):
            cname = tree.find('concept/conceptName').text
            cnui = tree.find('concept/conceptNui').text
            cur.execute('UPDATE ensemble_meds SET ndfrt_va_class = %s, ndfrt_va_class_nui = %s WHERE rxcui = %s', cname.strip(), cnui.strip(), row[0])
    print '-------'


    # update all rxcuis with this moa


print 'moa = ' + str(n_moa) + ' & va = ' + str(n_va)
#conn.commit()
conn.close()
