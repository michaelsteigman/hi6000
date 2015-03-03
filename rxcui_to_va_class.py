import psycopg2, os, requests, re
from xml.etree import ElementTree as ET
from secrets import ncbo_apikey, dbname, dbuser, dbpasswd

try:
    conn = psycopg2.connect( dbname = dbname, user = dbuser, password = dbpasswd )
except:
    print("Unable to connect to the database")
    exit()

cur = conn.cursor()
cur.execute('SELECT DISTINCT rxcui, rxcui_gen, medstring FROM ensemble_meds LIMIT 20')
rows = cur.fetchall()
for row in rows:

    # extract VA product and ingredient level IDs plus MoA
    product_nui = None
    ingredient_nui = None
    class_name = None
    class_nui = None
    
    r = requests.get('http://rxnav.nlm.nih.gov/REST/rxcui/{}/hierarchy?src=NDFRT&type=MOA&oneLevel=1'.format(row[0]))
    tree = ET.fromstring(r.text)

    # use generic rxcui if no NDFRT data for non-gen cui
    if re.search('ERROR', tree.find('.//title').text):
        print('nothing for ' + row[0] + ', so falling back to generic: ' + row[1] + ' and product_nui is ' + str(product_nui))
        r = requests.get('http://rxnav.nlm.nih.gov/REST/rxcui/{}/hierarchy?src=NDFRT&type=MOA&oneLevel=1'.format(row[1]))
        tree = ET.fromstring(r.text)

        if re.search('ERROR', tree.find('.//title').text):
            #print('nothing for generic, falling back to medstring: ' + row[2] + ' and product_nui is ' + str(product_nui))
            params = {'text':row[2], 'apikey': ncbo_apikey, 'format':'xml', 'ontologies':'NDFRT', 'longest_only':'true', 'exclude_synonyms':'true'}
            r = requests.get('http://data.bioontology.org/annotator', params=params)
            tree = ET.fromstring(r.text)

            if tree.find('.//id') is not None:
                print('got something from annotator ' + re.search(r'N[0-9]*$', tree.find('.//id').text).group(0))
                product_nui = re.search(r'N[0-9]*$', tree.find('.//id').text).group(0)
            else:
                print('    **** could not determine any NUI for medstring ' + row[2])
                continue


    if not product_nui:
        for node in tree.iter('node'):
        # if we have all elements, move on
            if product_nui and ingredient_nui and moa_nui:
                break
            for elm in node.findall('./nodeAttr/attrValue'):
                if elm.text == 'VA Product':
                    product_nui = node.find('nodeId').text
                elif elm.text == 'Ingredient':
                    ingredient_nui = node.find('nodeId').text
                elif elm.text == 'MECHANISM_OF_ACTION_KIND':
                    moa = node.find('nodeName').text
                    moa_nui = node.find('nodeId').text


    # should probably not be done inside cursor loop
    if ingredient_nui and not product_nui:
    # must have apikeys module available with ncbo_apikey inside of it

        query_string = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ndfrt: <http://purl.bioontology.org/ontology/NDFRT/>
SELECT DISTINCT ?vaprod_nui
FROM <http://bioportal.bioontology.org/ontologies/NDFRT>
WHERE
{{
?vaprod rdfs:subClassOf ndfrt:{}.
?vaprod ndfrt:NUI ?vaprod_nui.
?vaprod ndfrt:LEVEL ?level.
FILTER regex(?level, 'VA Product', 'i')
}}
Limit 1
""".format(ingredient_nui)
        sparql_service = "http://sparql.bioontology.org/sparql/"
        params = {'query': query_string, 'apikey': ncbo_apikey}
        r = requests.get(sparql_service, params=params)
        tree = ET.fromstring(r.text)
        namespaces = {'sparql': 'http://www.w3.org/2005/sparql-results#'}
        if tree.find('.//sparql:literal', namespaces) is not None:
            product_nui = tree.find('.//sparql:literal', namespaces).text
        else:
            print('   --------- couldn not find va product for ' + row[2])
            continue

    # finally!
    if product_nui:
        print('product_nui is ' + product_nui)
        r = requests.get('http://rxnav.nlm.nih.gov/REST/Ndfrt/VA?nui={}'.format(product_nui))
        tree = ET.fromstring(r.text)
        if tree.find('concept/conceptNui') is not None:
            class_name = tree.find('concept/conceptName').text
            class_nui = tree.find('concept/conceptNui').text
        else:
            print('   +++++++ couldn not find class name for ' + row[2])
            continue

    print(row[2] + ' ---> ' + str(class_name))
# update all meds with retrieved classes
#cur.execute('UPDATE ensemble_meds SET ndfrt_va_class = %s, ndfrt_va_class_nui = %s WHERE rxcui = %s', cname.strip(), cnui.strip(), row[0])

#conn.commit()
conn.close()
