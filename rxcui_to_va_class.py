import psycopg2, os, requests, re
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import ParseError
from secrets import ncbo_apikey, dbname, dbuser, dbpasswd

try:
    conn = psycopg2.connect( dbname = dbname, user = dbuser, password = dbpasswd )
except:
    print("Unable to connect to the database")
    exit()

cur = conn.cursor()
cur.execute('SELECT DISTINCT rxcui, rxcui_gen, medstring FROM ensemble_meds WHERE ndfrt_va_class_nui is null')
rows = cur.fetchall()
for row in rows:

    product_nui = None
    ingredient_nui = None
    class_name = None
    class_nui = None

    #
    # First, try to retrieve MOA for the RxCUI MedEx gave us (row[0])
    #
    r = requests.get('http://rxnav.nlm.nih.gov/REST/rxcui/{}/hierarchy?src=NDFRT&type=MOA&oneLevel=1'.format(row[0]))
    try:
        tree = ET.fromstring(r.text)
    except ParseError:
        print('could not parse: ' + r.text)
        continue

    #
    # If the title of the XML classification doc returned by RxNav contains the word 'ERROR', try again with the generic RxCUI from MedEx (row[1])
    #
    if re.search('ERROR', tree.find('.//title').text):
        #print('nothing for ' + row[0] + ', so falling back to generic: ' + row[1] + ' and product_nui is ' + str(product_nui))
        r = requests.get('http://rxnav.nlm.nih.gov/REST/rxcui/{}/hierarchy?src=NDFRT&type=MOA&oneLevel=1'.format(row[1]))
        try:
            tree = ET.fromstring(r.text)
        except ParseError:
            print('could not parse: ' + r.text)
            continue

        #
        # If we are still getting an error (no results), try to search NDFRT ontology on Bioportal for medstring (row[2]) using their annotator service
        #
        if re.search('ERROR', tree.find('.//title').text):
            #print('nothing for generic, falling back to medstring: ' + row[2] + ' and product_nui is ' + str(product_nui))
            params = {'text':row[2], 'apikey': ncbo_apikey, 'format':'xml', 'ontologies':'NDFRT', 'longest_only':'true', 'exclude_synonyms':'true'}
            r = requests.get('http://data.bioontology.org/annotator', params=params)
            try:
                tree = ET.fromstring(r.text)
            except ParseError:
                print('could not parse: ' + r.text)
                continue

            if tree.find('.//id') is not None and re.search(r'N[0-9]*$', tree.find('.//id').text) is not None:
                #print('got something from annotator ' + re.search(r'N[0-9]*$', tree.find('.//id').text).group(0))
                product_nui = re.search(r'N[0-9]*$', tree.find('.//id').text).group(0)
            else:
                #print('    **** could not determine any NUI for medstring ' + row[2] + '; r.text was ' + r.text)
                continue

    #
    # If Bioportal returned a hit for medstring, we'll have a product_nui; if not, and we haven't hit an ERROR above, extract the product_nui here
    #
    if not product_nui:
        for node in tree.iter('node'):
            if product_nui and ingredient_nui:
                break
            for elm in node.findall('./nodeAttr/attrValue'):
                if elm.text == 'VA Product':
                    product_nui = node.find('nodeId').text
                elif elm.text == 'Ingredient':
                    ingredient_nui = node.find('nodeId').text


    #
    # If we have an ingredient level NUI but still no product_nui yet, try to query Bioportal's SPARQL endpoint for subclass of ingredent that is a VA Product
    #
    if ingredient_nui and not product_nui:
    # NOTE: must have apikeys module available with ncbo_apikey inside of it (see import at top of script)

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

    #
    # Finally! If we have a VA Product NUI, we can try to query RxNav for the VA class
    #
    if product_nui:
        r = requests.get('http://rxnav.nlm.nih.gov/REST/Ndfrt/VA?nui={}'.format(product_nui))
        try:
            tree = ET.fromstring(r.text)
        except ParseError:
            print('could not parse ' + r.test)
            continue

        if tree.find('concept/conceptNui') is not None:
            class_name = tree.find('concept/conceptName').text
            class_nui = tree.find('concept/conceptNui').text
            # update all meds with retrieved classes
            cur.execute('UPDATE ensemble_meds SET ndfrt_va_class = %s, ndfrt_va_class_nui = %s WHERE rxcui = %s', (class_name.strip(), class_nui.strip(), row[0],))
        else:
            #print('   +++++++ couldn not find class name for ' + row[2])
            continue
    else:
        #print(row[0], row[2])


conn.commit()
conn.close()
